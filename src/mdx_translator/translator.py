"""Core MDX translator: sends MDX cube to Claude API via FMAPI, returns Metric View definition.

Reuses the parsing/building logic from dax_translator, with MDX-specific prompt.
"""

from __future__ import annotations

import json
import os
import re
import textwrap

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from dotenv import load_dotenv

from dax_translator.models import (
    MeasureWarning,
    TranslatedDimension,
    TranslatedJoin,
    TranslatedMeasure,
    TranslationResult,
    TranslationStatus,
    WindowSpec,
)

from .models import MdxCube
from .prompt import SYSTEM_PROMPT, build_user_prompt

load_dotenv()

VALID_WINDOW_UNITS = {"day", "month", "year", "quarter", "week", "hour"}


def _fix_window_range(raw_range: str) -> str:
    """Normalize window range values that Claude may format incorrectly.

    Valid: current, cumulative, all, trailing <N> <unit>, leading <N> <unit>
    Fixes: trailing_3_months → trailing 3 month, trailing 6 months → trailing 6 month
    """
    raw_range = raw_range.strip()
    if raw_range in ("current", "cumulative", "all"):
        return raw_range
    # Normalize underscores to spaces
    normalized = raw_range.replace("_", " ")
    # Fix plural units: "months" → "month", "years" → "year"
    for unit in list(VALID_WINDOW_UNITS):
        normalized = re.sub(rf"\b{unit}s\b", unit, normalized)
    return normalized


DEFAULT_FMAPI_ENDPOINT = "databricks-claude-sonnet-4-6"
DEFAULT_PROFILE = os.getenv("DATABRICKS_PROFILE", "DEFAULT")
MAX_RETRIES = 2


def _extract_json(text: str) -> dict:
    """Extract JSON from Claude's response, handling markdown code fences."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()
    return json.loads(text)


def _build_yaml_body(result: TranslationResult) -> str:
    """Build the YAML body for the metric view."""
    doc: dict = {}
    v01 = result.version == "0.1"

    doc["version"] = result.version
    if result.comment and not v01:
        doc["comment"] = result.comment
    doc["source"] = result.source

    if result.joins:
        doc["joins"] = []
        for j in result.joins:
            jd: dict = {"name": j.name, "source": j.source}
            if j.on:
                jd["on"] = j.on
            if j.using:
                jd["using"] = j.using
            doc["joins"].append(jd)

    doc["dimensions"] = []
    for d in result.dimensions:
        dd: dict = {"name": d.name, "expr": d.expr}
        if d.comment and not v01:
            dd["comment"] = d.comment
        doc["dimensions"].append(dd)

    doc["measures"] = []
    for m in result.measures:
        md: dict = {"name": m.name, "expr": m.expr}
        if m.comment and not v01:
            md["comment"] = m.comment
        if m.window:
            md["window"] = [
                {"order": w.order, "range": w.range, "semiadditive": w.semiadditive}
                for w in m.window
            ]
        doc["measures"].append(md)

    return yaml.dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)


def _build_sql(result: TranslationResult, view_name: str) -> str:
    """Build the full CREATE OR REPLACE VIEW ... WITH METRICS statement."""
    yaml_body = _build_yaml_body(result)
    indented = textwrap.indent(yaml_body.rstrip(), "  ")
    return (
        f"CREATE OR REPLACE VIEW {view_name}\n"
        f"WITH METRICS\n"
        f"LANGUAGE YAML\n"
        f"AS $$\n"
        f"{indented}\n"
        f"$$"
    )


def _parse_response(raw: dict, cube: MdxCube) -> TranslationResult:
    """Parse Claude's JSON response into a TranslationResult."""
    status = TranslationStatus(raw.get("status", "success"))
    version = raw.get("version", "1.1")

    source = raw.get("source", "") or cube.measure_group.databricks_table

    joins = [
        TranslatedJoin(name=j["name"], source=j["source"], on=j.get("on"), using=j.get("using"))
        for j in raw.get("joins", [])
    ]
    dimensions = [
        TranslatedDimension(name=d["name"], expr=d["expr"], comment=d.get("comment"))
        for d in raw.get("dimensions", [])
    ]
    measures = []
    for m in raw.get("measures", []):
        window = None
        if m.get("window"):
            window = [
                WindowSpec(order=w["order"], range=_fix_window_range(w["range"]), semiadditive=w.get("semiadditive", "last"))
                for w in m["window"]
            ]
        measures.append(
            TranslatedMeasure(name=m["name"], expr=m["expr"], comment=m.get("comment"), window=window)
        )
    warnings = [
        MeasureWarning(
            measure_name=w["measure_name"],
            dax_expression=w.get("dax_expression", w.get("mdx_expression", "")),
            warning_type=w["warning_type"],
            message=w["message"],
            approximation=w.get("approximation"),
        )
        for w in raw.get("warnings", [])
    ]

    if any(m.window for m in measures):
        version = "0.1"

    # Fix window order fields — must reference dimension names, not expressions
    dim_names = {d.name for d in dimensions}
    dim_expr_to_name = {d.expr: d.name for d in dimensions}
    for m in measures:
        if m.window:
            for w in m.window:
                if w.order not in dim_names:
                    # Try mapping expr → name
                    if w.order in dim_expr_to_name:
                        w.order = dim_expr_to_name[w.order]
                    else:
                        # Fuzzy match: find a dimension whose name is contained in the order
                        for dn in dim_names:
                            if dn.lower() in w.order.lower() or w.order.lower() in dn.lower():
                                w.order = dn
                                break

    # Post-process: reject measures that contain SQL window functions (OVER clause)
    clean_measures = []
    for m in measures:
        if re.search(r'\bOVER\s*\(', m.expr, re.IGNORECASE):
            warnings.append(MeasureWarning(
                measure_name=m.name,
                dax_expression=m.expr,
                warning_type="unsupported_dax",
                message="Measure uses SQL window function (OVER), which is not supported in Metric Views. Removed from output.",
                approximation=None,
            ))
            if status == TranslationStatus.SUCCESS:
                status = TranslationStatus.PARTIAL
        else:
            clean_measures.append(m)
    measures = clean_measures

    result = TranslationResult(
        status=status, version=version, source=source, comment=raw.get("comment"),
        joins=joins, dimensions=dimensions, measures=measures, warnings=warnings,
    )

    view_name = f"{cube.catalog}.{cube.schema_name}.mv_{cube.name.lower().replace(' ', '_')}"
    result.yaml_body = _build_yaml_body(result)
    result.sql = _build_sql(result, view_name)
    return result


def _call_fmapi(
    system_prompt: str,
    user_prompt: str,
    endpoint: str = DEFAULT_FMAPI_ENDPOINT,
    profile: str = DEFAULT_PROFILE,
) -> str:
    """Call Claude via Databricks FMAPI."""
    w = WorkspaceClient(profile=profile)
    response = w.serving_endpoints.query(
        name=endpoint,
        messages=[
            ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
            ChatMessage(role=ChatMessageRole.USER, content=user_prompt),
        ],
        max_tokens=4096,
    )
    return response.choices[0].message.content


def translate(
    cube: MdxCube,
    fmapi_endpoint: str | None = None,
    databricks_profile: str | None = None,
) -> TranslationResult:
    """Translate an MDX OLAP cube into a Databricks Metric View definition.

    Args:
        cube: The MDX cube definition to translate.
        fmapi_endpoint: FMAPI serving endpoint name.
        databricks_profile: Databricks CLI profile for auth.

    Returns:
        TranslationResult with the generated Metric View definition.
    """
    endpoint = fmapi_endpoint or os.getenv("FMAPI_ENDPOINT", DEFAULT_FMAPI_ENDPOINT)
    profile = databricks_profile or os.getenv("DATABRICKS_PROFILE", DEFAULT_PROFILE)

    # Serialize cube for the prompt
    if hasattr(cube, "model_dump_json"):
        cube_json = cube.model_dump_json(indent=2)
    else:
        cube_json = cube.json(indent=2)

    user_prompt = build_user_prompt(cube_json)

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response_text = _call_fmapi(SYSTEM_PROMPT, user_prompt, endpoint, profile)
            raw = _extract_json(response_text)
            return _parse_response(raw, cube)
        except json.JSONDecodeError as e:
            last_error = e
            if attempt < MAX_RETRIES:
                continue
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                continue

    raise RuntimeError(f"Failed to translate MDX after {MAX_RETRIES + 1} attempts: {last_error}")
