"""Core translator: sends DAX model to Claude API (via Databricks FMAPI or direct), returns Metric View definition."""

from __future__ import annotations

import json
import os
import re
import textwrap

import yaml
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from .models import (
    DaxModel,
    MeasureWarning,
    TranslatedDimension,
    TranslatedJoin,
    TranslatedMeasure,
    TranslationResult,
    TranslationStatus,
    WindowSpec,
)
from .prompt import SYSTEM_PROMPT, build_user_prompt

load_dotenv()

VALID_WINDOW_UNITS = {"day", "month", "year", "quarter", "week", "hour"}


def _fix_window_range(raw_range: str) -> str:
    """Normalize window range values that Claude may format incorrectly."""
    raw_range = raw_range.strip()
    if raw_range in ("current", "cumulative", "all"):
        return raw_range
    normalized = raw_range.replace("_", " ")
    for unit in list(VALID_WINDOW_UNITS):
        normalized = re.sub(rf"\b{unit}s\b", unit, normalized)
    return normalized


DEFAULT_FMAPI_ENDPOINT = "databricks-claude-sonnet-4-6"
DEFAULT_PROFILE = os.getenv("DATABRICKS_PROFILE", "DEFAULT")
MAX_RETRIES = 2


def _extract_json(text: str) -> dict:
    """Extract JSON from Claude's response, handling markdown code fences."""
    text = text.strip()
    # Strip markdown code fences if present
    if text.startswith("```"):
        # Remove opening fence (```json or ```)
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        # Remove closing fence
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()
    return json.loads(text)


def _build_yaml_body(result: TranslationResult) -> str:
    """Build the YAML body for the metric view from the TranslationResult."""
    doc: dict = {}

    # version 0.1 does not support `comment` at any level
    v01 = result.version == "0.1"

    doc["version"] = result.version
    if result.comment and not v01:
        doc["comment"] = result.comment

    doc["source"] = result.source

    if result.joins:
        doc["joins"] = []
        for j in result.joins:
            join_dict: dict = {"name": j.name, "source": j.source}
            if j.on:
                join_dict["on"] = j.on
            if j.using:
                join_dict["using"] = j.using
            doc["joins"].append(join_dict)

    doc["dimensions"] = []
    for d in result.dimensions:
        dim_dict: dict = {"name": d.name, "expr": d.expr}
        if d.comment and not v01:
            dim_dict["comment"] = d.comment
        doc["dimensions"].append(dim_dict)

    doc["measures"] = []
    for m in result.measures:
        meas_dict: dict = {"name": m.name, "expr": m.expr}
        if m.comment and not v01:
            meas_dict["comment"] = m.comment
        if m.window:
            meas_dict["window"] = [
                {"order": w.order, "range": w.range, "semiadditive": w.semiadditive}
                for w in m.window
            ]
        doc["measures"].append(meas_dict)

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


def _parse_response(raw: dict, dax_model: DaxModel) -> TranslationResult:
    """Parse Claude's JSON response into a TranslationResult."""
    status = TranslationStatus(raw.get("status", "success"))
    version = raw.get("version", "1.1")

    source = raw.get("source", "")
    if not source:
        fact = dax_model.get_table(dax_model.fact_table)
        source = fact.databricks_table if fact and fact.databricks_table else ""

    joins = [
        TranslatedJoin(
            name=j["name"],
            source=j["source"],
            on=j.get("on"),
            using=j.get("using"),
        )
        for j in raw.get("joins", [])
    ]

    dimensions = [
        TranslatedDimension(
            name=d["name"],
            expr=d["expr"],
            comment=d.get("comment"),
        )
        for d in raw.get("dimensions", [])
    ]

    measures = []
    for m in raw.get("measures", []):
        window = None
        if m.get("window"):
            window = [
                WindowSpec(
                    order=w["order"],
                    range=_fix_window_range(w["range"]),
                    semiadditive=w.get("semiadditive", "last"),
                )
                for w in m["window"]
            ]
        measures.append(
            TranslatedMeasure(
                name=m["name"],
                expr=m["expr"],
                comment=m.get("comment"),
                window=window,
            )
        )

    warnings = [
        MeasureWarning(
            measure_name=w["measure_name"],
            dax_expression=w["dax_expression"],
            warning_type=w["warning_type"],
            message=w["message"],
            approximation=w.get("approximation"),
        )
        for w in raw.get("warnings", [])
    ]

    # Auto-detect version based on window measures
    has_windows = any(m.window for m in measures)
    if has_windows:
        version = "0.1"

    # Fix window order fields — must reference dimension names, not expressions
    dim_names = {d.name for d in dimensions}
    dim_expr_to_name = {d.expr: d.name for d in dimensions}
    for m in measures:
        if m.window:
            for w in m.window:
                if w.order not in dim_names:
                    if w.order in dim_expr_to_name:
                        w.order = dim_expr_to_name[w.order]
                    else:
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
        status=status,
        version=version,
        source=source,
        comment=raw.get("comment"),
        joins=joins,
        dimensions=dimensions,
        measures=measures,
        warnings=warnings,
    )

    # Build YAML and SQL
    view_name = f"{dax_model.catalog}.{dax_model.schema_name}.mv_{dax_model.name}"
    result.yaml_body = _build_yaml_body(result)
    result.sql = _build_sql(result, view_name)

    return result


def _call_fmapi(
    system_prompt: str,
    user_prompt: str,
    endpoint: str = DEFAULT_FMAPI_ENDPOINT,
    profile: str = DEFAULT_PROFILE,
) -> str:
    """Call Claude via Databricks Foundation Model API (OpenAI-compatible)."""
    from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

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


def _call_anthropic(
    system_prompt: str,
    user_prompt: str,
    model: str,
    api_key: str | None = None,
) -> str:
    """Call Claude via direct Anthropic API."""
    import anthropic

    client = anthropic.Anthropic(api_key=api_key) if api_key else anthropic.Anthropic()
    message = client.messages.create(
        model=model,
        max_tokens=4096,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return message.content[0].text


def translate(
    dax_model: DaxModel,
    model: str | None = None,
    api_key: str | None = None,
    use_fmapi: bool | None = None,
    fmapi_endpoint: str | None = None,
    databricks_profile: str | None = None,
) -> TranslationResult:
    """Translate a DAX model into a Databricks Metric View definition.

    Args:
        dax_model: The DAX model to translate.
        model: Claude model ID (for direct Anthropic API). Ignored when using FMAPI.
        api_key: Anthropic API key (for direct API). Ignored when using FMAPI.
        use_fmapi: If True, use Databricks FMAPI. If None, auto-detect (FMAPI if no ANTHROPIC_API_KEY).
        fmapi_endpoint: FMAPI serving endpoint name. Defaults to databricks-claude-sonnet-4-6.
        databricks_profile: Databricks CLI profile for FMAPI auth.

    Returns:
        TranslationResult with the generated Metric View definition.
    """
    # Auto-detect: use FMAPI unless ANTHROPIC_API_KEY is set
    if use_fmapi is None:
        use_fmapi = not (api_key or os.getenv("ANTHROPIC_API_KEY"))

    endpoint = fmapi_endpoint or os.getenv("FMAPI_ENDPOINT", DEFAULT_FMAPI_ENDPOINT)
    profile = databricks_profile or os.getenv("DATABRICKS_PROFILE", DEFAULT_PROFILE)

    # Serialize DAX model for the prompt
    dax_json = dax_model.model_dump_json(indent=2)
    user_prompt = build_user_prompt(dax_json)

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            if use_fmapi:
                response_text = _call_fmapi(SYSTEM_PROMPT, user_prompt, endpoint, profile)
            else:
                model_id = model or os.getenv("CLAUDE_MODEL", "claude-sonnet-4-20250514")
                response_text = _call_anthropic(SYSTEM_PROMPT, user_prompt, model_id, api_key)

            raw = _extract_json(response_text)
            return _parse_response(raw, dax_model)

        except json.JSONDecodeError as e:
            last_error = e
            if attempt < MAX_RETRIES:
                continue
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                continue

    raise RuntimeError(
        f"Failed to translate after {MAX_RETRIES + 1} attempts: {last_error}"
    )
