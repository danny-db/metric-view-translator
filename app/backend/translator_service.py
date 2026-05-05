"""Self-contained DAX + MDX → Metric View translator.

Uses Claude via Databricks FMAPI (app service principal auth).
No imports from the parent src/ directory — all prompts and logic are embedded.
"""

from __future__ import annotations

import json
import re
import textwrap

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

from .config import MAX_RETRIES, SERVING_ENDPOINT_NAME
from .models import (
    DaxModel,
    MdxCube,
    MeasureWarning,
    TranslatedDimension,
    TranslatedJoin,
    TranslatedMeasure,
    TranslationResult,
    TranslationStatus,
    WindowSpec,
)

VALID_WINDOW_UNITS = {"day", "month", "year", "quarter", "week", "hour"}


# ── Prompts ──────────────────────────────────────────────────────────────────

DAX_SYSTEM_PROMPT = r"""You are an expert at translating Power BI DAX measures into Databricks Metric View definitions.

# Your Task
Given a DAX model (tables, relationships, measures), produce a complete Databricks Metric View definition as a JSON object that follows the exact schema specified below.

# Databricks Metric View YAML Reference

## Top-Level Fields
| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `version` | No | string | `"1.1"` for standard measures, `"0.1"` if window measures are used |
| `source` | Yes | string | Fully qualified source table (catalog.schema.table) |
| `comment` | No | string | Description of the metric view |
| `filter` | No | string | SQL boolean expression applied as global WHERE |
| `dimensions` | Yes | list | Dimension definitions (at least one) |
| `measures` | Yes | list | Measure definitions (at least one) |
| `joins` | No | list | Star/snowflake join definitions |

## Dimensions
- `name`: Display name (backtick-quoted in queries if it has spaces)
- `expr`: SQL expression — can be a column reference, SQL function, CASE expression, or joined table column (`join_name.column`)
- `comment`: Optional description
- Cannot use aggregate functions

## Measures
- `name`: Display name, queried via `MEASURE("name")`
- `expr`: Must contain an aggregate function (SUM, COUNT, AVG, MIN, MAX)
- Supports `FILTER (WHERE ...)` for conditional aggregation
- Supports ratios of aggregates: `SUM(a) / COUNT(DISTINCT b)`
- Use `NULLIF(denominator, 0)` for safe division (maps from DAX DIVIDE)

## Filtered Measures
```yaml
- name: Active Revenue
  expr: SUM(amount) FILTER (WHERE status = 'Active')
```

## Ratio Measures
```yaml
- name: Revenue per Customer
  expr: SUM(amount) / COUNT(DISTINCT customer_id)
```

## Safe Division (DAX DIVIDE equivalent)
```yaml
- name: Profit Margin
  expr: SUM(profit) / NULLIF(SUM(revenue), 0)
```

## Joins (Star Schema)
```yaml
joins:
  - name: customer
    source: catalog.schema.dim_customer
    on: source.customer_id = customer.customer_id
  - name: product
    source: catalog.schema.dim_product
    on: source.product_id = product.product_id
```
- Reference joined columns as `join_name.column` in dimensions
- Reference fact table columns as just `column_name` (no `source.` prefix needed in dimensions/measures)

## Window Measures (require version: "0.1")
```yaml
measures:
  - name: ytd_sales
    expr: SUM(amount)
    window:
      - order: date_dim_name
        range: cumulative
        semiadditive: last
      - order: year_dim_name
        range: current
        semiadditive: last
```
Window range values: `current`, `cumulative`, `trailing <N> <unit>`, `leading <N> <unit>`, `all`

# DAX-to-SQL Mapping Rules

## Simple Aggregations
| DAX | Metric View SQL |
|-----|----------------|
| `SUM(Table[Column])` | `SUM(column)` |
| `COUNT(Table[Column])` | `COUNT(column)` |
| `DISTINCTCOUNT(Table[Column])` | `COUNT(DISTINCT column)` |
| `AVERAGE(Table[Column])` | `AVG(column)` |
| `MIN(Table[Column])` | `MIN(column)` |
| `MAX(Table[Column])` | `MAX(column)` |
| `COUNTROWS(Table)` | `COUNT(1)` |

## CALCULATE with Filters
| DAX | Metric View SQL |
|-----|----------------|
| `CALCULATE(SUM(T[Col]), T[Status]="Active")` | `SUM(col) FILTER (WHERE status = 'Active')` |
| `CALCULATE(SUM(T[Col]), T[A]="X", T[B]="Y")` | `SUM(col) FILTER (WHERE a = 'X' AND b = 'Y')` |

## Row-Level Operations
| DAX | Metric View SQL |
|-----|----------------|
| `SUMX(Table, Table[Qty]*Table[Price])` | `SUM(quantity * unit_price)` |
| `IF(Table[Col]>X, "A", "B")` | `CASE WHEN col > X THEN 'A' ELSE 'B' END` (as dimension) |

## Relationships / RELATED
| DAX | Metric View SQL |
|-----|----------------|
| `RELATED(DimTable[Column])` | `join_name.column` (with join defined) |

## Division / DIVIDE
| DAX | Metric View SQL |
|-----|----------------|
| `DIVIDE(SUM(A), SUM(B), 0)` | `SUM(a) / NULLIF(SUM(b), 0)` |
| `[Measure1] / [Measure2]` | Inline the aggregate expressions |

## Composite Measures
When a DAX measure references another measure (e.g., `[Total Sales]`), inline the referenced measure's aggregate expression. Do NOT use MEASURE() syntax for non-window measures.

## Time Intelligence (Approximations)
| DAX | Metric View Approach |
|-----|---------------------|
| `TOTALYTD(SUM(T[Col]), Calendar[Date])` | Window measure: cumulative over date + current year. Requires version "0.1". |
| `SAMEPERIODLASTYEAR(...)` | Emit warning with `warning_type: "time_intelligence"`. Best approximation: trailing 1 year window. |
| `DATEADD(Calendar[Date], -1, MONTH)` | Emit warning. Approximation: trailing 1 month window or filter approach. |

## Unsupported DAX
| DAX | Action |
|-----|--------|
| `RANKX(...)` | Emit warning with `warning_type: "unsupported_dax"` |
| `USERELATIONSHIP(...)` | Emit warning with `warning_type: "unsupported_dax"` |
| `ALLSELECTED(...)` | Emit warning with `warning_type: "unsupported_dax"` |
| Any DAX with no SQL equivalent | Emit warning, provide best approximation if possible |

# Output JSON Schema

Return a JSON object with exactly these fields:

```json
{
  "status": "success" | "partial" | "failed",
  "version": "1.1" or "0.1",
  "source": "catalog.schema.fact_table",
  "comment": "Description of the metric view",
  "joins": [
    {
      "name": "join_alias",
      "source": "catalog.schema.dim_table",
      "on": "source.fk = join_alias.pk"
    }
  ],
  "dimensions": [
    {
      "name": "Dimension Name",
      "expr": "sql_expression",
      "comment": "optional description"
    }
  ],
  "measures": [
    {
      "name": "Measure Name",
      "expr": "aggregate_expression",
      "comment": "optional description",
      "window": [
        {
          "order": "dimension_name",
          "range": "cumulative",
          "semiadditive": "last"
        }
      ]
    }
  ],
  "warnings": [
    {
      "measure_name": "name",
      "dax_expression": "original DAX",
      "warning_type": "unsupported_dax" | "approximation" | "time_intelligence",
      "message": "explanation",
      "approximation": "best-effort SQL or null"
    }
  ]
}
```

# Critical Rules

1. **Column naming**: Convert DAX `Table[ColumnName]` to snake_case SQL column names. E.g., `Sales[OrderID]` -> `order_id`, `Sales[Amount]` -> `amount`.
2. **No `source.` prefix** in dimension/measure expressions for fact table columns. Only use `join_name.column` for joined dimension table columns.
3. **Inline composite measures**: If a measure references `[Other Measure]`, substitute the full aggregate expression.
4. **Use NULLIF for DIVIDE**: DAX `DIVIDE(num, den, 0)` -> `num / NULLIF(den, 0)`.
5. **FILTER clause syntax**: Always `FILTER (WHERE ...)` with a space before the parenthesis.
6. **Version auto-detect**: Use `"0.1"` only if any measure has a `window` block. Otherwise `"1.1"`.
7. **Graceful degradation**: Never fail completely. If some measures can't be translated, mark them with warnings and translate the rest. Set status to `"partial"`.
8. **Return pure JSON only**: No markdown, no code fences, no explanation. Just the JSON object.
9. **IF expressions as dimensions**: DAX `IF(condition, "A", "B")` becomes a dimension with `CASE WHEN ... THEN ... ELSE ... END`.
10. **SUMX becomes SUM of product**: `SUMX(T, T[A]*T[B])` -> `SUM(a * b)`.
11. **NEVER use SQL window functions**: Measure `expr` must NEVER contain `OVER(`, `PARTITION BY`, `ROWS BETWEEN`, or any SQL analytic/window function syntax. The only windowing allowed is through the YAML `window` block (order/range/semiadditive). If a DAX pattern requires SQL window functions, emit a warning with `warning_type: "unsupported_dax"` instead.
"""

MDX_SYSTEM_PROMPT = r"""You are an expert at translating SSAS Multidimensional (OLAP) MDX calculated members into Databricks Metric View definitions.

# Your Task
Given an MDX OLAP cube definition (measure groups, dimensions, hierarchies, calculated members), produce a complete Databricks Metric View definition as a JSON object.

# MDX Concepts -> Metric View Mapping

## Structural Mapping
| MDX Concept | Metric View Equivalent |
|-------------|----------------------|
| Cube / Measure Group | `source` (fact table) |
| Dimension | Join to dimension table |
| Dimension Attribute | Dimension `expr` |
| Dimension Hierarchy | Multiple dimensions (one per level) |
| Base Measure (SUM, COUNT, etc.) | Measure `expr` with aggregate |
| Calculated Member | Measure `expr` (inline the calculation) |

## MDX Measure -> SQL Mapping
| MDX Expression | Metric View SQL |
|---------------|----------------|
| `[Measures].[Sales Amount]` (base SUM) | `SUM(sales_amount)` |
| `[Measures].[Order Count]` (base COUNT) | `COUNT(order_id)` |
| `[Measures].[Distinct Customers]` (DistinctCount) | `COUNT(DISTINCT customer_id)` |
| `[Measures].[Avg Price]` (base AVG) | `AVG(price)` |
| `[Measures].[Min/Max Amount]` | `MIN(amount)` / `MAX(amount)` |

## MDX Calculated Members -> SQL Mapping
| MDX Pattern | Metric View SQL |
|------------|----------------|
| `[Measures].[A] / [Measures].[B]` | `SUM(a) / NULLIF(SUM(b), 0)` |
| `[Measures].[A] - [Measures].[B]` | `SUM(a) - SUM(b)` |
| `([Measures].[A] - [Measures].[B]) / [Measures].[A]` | `(SUM(a) - SUM(b)) / NULLIF(SUM(a), 0)` |
| `IIF(condition, value1, value2)` | `CASE WHEN condition THEN value1 ELSE value2 END` |

## MDX Filter Patterns -> SQL
| MDX Pattern | Metric View SQL |
|------------|----------------|
| `FILTER(set, condition)` on a measure | `agg FILTER (WHERE condition)` |
| Tuple filter: `([Status].&[Active], [Measures].[Sales])` | `SUM(amount) FILTER (WHERE status = 'Active')` |

## MDX Dimension References -> Joins
| MDX | Metric View |
|-----|-------------|
| `[Customer].[Name]` | `customer.name` (with join) |
| `[Product].[Category]` | `product.category` (with join) |
| `[Date].[Calendar].[Year]` | `calendar.year` (with join) |

## MDX Time Intelligence -> Window Measures
| MDX Pattern | Metric View Approach |
|------------|---------------------|
| `SUM(YTD(), [Measures].[Sales])` | Window measure: cumulative + current year. Version 0.1. |
| `ParallelPeriod([Date].[Calendar].[Year], 1, currentmember)` | Warning: time_intelligence. Approximation: trailing 1 year window. |
| `LastPeriods(6, currentmember)` | Warning: time_intelligence. Approximation: trailing 6 month window. |

## Unsupported MDX
| MDX Pattern | Action |
|------------|--------|
| `RANK(...)` / `TopCount(...)` / `BottomCount(...)` | Warning: unsupported_dax |
| `Generate(...)` / `CrossJoin(...)` | Warning: unsupported_dax |
| `SCOPE` / `FREEZE` / cell-level calculations | Warning: unsupported_dax |
| `.Parent` / `.PrevMember` / `.NextMember` / `.Lag()` / `.Lead()` | Warning: unsupported_dax. Require SQL window functions which are forbidden. |
| `([Measures].[X], [Dim].Parent)` parent-relative references | Warning: unsupported_dax. Cannot compute parent-level aggregation. |

# Databricks Metric View YAML Reference

## Structure
```yaml
version: "1.1"  # or "0.1" for window measures
source: catalog.schema.fact_table
joins:
  - name: customer
    source: catalog.schema.dim_customer
    on: source.customer_id = customer.customer_id
dimensions:
  - name: Dimension Name
    expr: sql_expression
measures:
  - name: Measure Name
    expr: aggregate_expression
  - name: Window Measure
    expr: SUM(amount)
    window:
      - order: date_dim
        range: cumulative
        semiadditive: last
```

## Measure Rules
- `expr` must contain an aggregate function (SUM, COUNT, AVG, MIN, MAX)
- Supports `FILTER (WHERE ...)` for conditional aggregation
- Supports ratios: `SUM(a) / COUNT(DISTINCT b)`
- Use `NULLIF(den, 0)` for safe division
- Window measures require version "0.1" — no `comment` fields allowed in 0.1

# Output JSON Schema

```json
{
  "status": "success" | "partial" | "failed",
  "version": "1.1" or "0.1",
  "source": "catalog.schema.fact_table",
  "comment": "optional (v1.1 only)",
  "joins": [{"name": "alias", "source": "catalog.schema.dim", "on": "source.fk = alias.pk"}],
  "dimensions": [{"name": "Name", "expr": "sql_expr", "comment": "optional (v1.1 only)"}],
  "measures": [{"name": "Name", "expr": "agg_expr", "window": [{"order": "dim", "range": "range_val", "semiadditive": "last"}]}],
  "warnings": [{"measure_name": "n", "dax_expression": "original MDX", "warning_type": "type", "message": "msg", "approximation": null}]
}
```

# Critical Rules

1. **Column naming**: ALWAYS use lowercase snake_case for ALL column references.
2. **Inline calculated members**: If a calculated member references `[Measures].[X]`, inline the base aggregate.
3. **Use NULLIF for division**: Any division -> `NULLIF(denominator, 0)`.
4. **FILTER clause syntax**: `FILTER (WHERE ...)` with a space before `(`.
5. **Version**: Use `"0.1"` only if any measure has `window`. Otherwise `"1.1"`.
6. **Version 0.1**: No `comment` fields anywhere.
7. **Graceful degradation**: Unsupported MDX -> warnings, not failures.
8. **Return pure JSON only**: No markdown, no code fences.
9. **IIF -> CASE**: `IIF(cond, a, b)` -> `CASE WHEN cond THEN a ELSE b END`.
10. **Dimension hierarchies**: Create separate dimension entries for each useful hierarchy level.
11. **NEVER use SQL window functions**: Measure `expr` must NEVER contain `OVER(`, `PARTITION BY`, `ROWS BETWEEN`, or any SQL analytic/window function syntax. The only windowing allowed is through the YAML `window` block (order/range/semiadditive). If an MDX pattern requires SQL window functions (e.g., `.Parent`, percentage of parent), emit a warning with `warning_type: "unsupported_dax"` instead.
"""


# ── Helpers ──────────────────────────────────────────────────────────────────


def _fix_window_range(raw_range: str) -> str:
    raw_range = raw_range.strip()
    if raw_range in ("current", "cumulative", "all"):
        return raw_range
    normalized = raw_range.replace("_", " ")
    for unit in list(VALID_WINDOW_UNITS):
        normalized = re.sub(rf"\b{unit}s\b", unit, normalized)
    return normalized


def _extract_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()
    return json.loads(text)


def _build_yaml_body(result: TranslationResult) -> str:
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


def _fix_window_orders(dimensions, measures):
    """Fix window order fields — must reference dimension names, not expressions."""
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


def _parse_common(raw: dict, source_fallback: str, view_name: str) -> TranslationResult:
    """Parse Claude's JSON response into a TranslationResult (shared by DAX and MDX)."""
    status = TranslationStatus(raw.get("status", "success"))
    version = raw.get("version", "1.1")
    source = raw.get("source", "") or source_fallback

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
                WindowSpec(
                    order=w["order"],
                    range=_fix_window_range(w["range"]),
                    semiadditive=w.get("semiadditive", "last"),
                )
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

    _fix_window_orders(dimensions, measures)

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
    result.yaml_body = _build_yaml_body(result)
    result.sql = _build_sql(result, view_name)
    return result


# ── Schema Introspection ─────────────────────────────────────────────────────


def _get_table_columns(table_fqn: str) -> list[str]:
    """Fetch column names for a table using the app's service principal."""
    try:
        from databricks.sdk.service.sql import StatementState
        w = WorkspaceClient()
        from .config import DATABRICKS_WAREHOUSE_ID
        resp = w.statement_execution.execute_statement(
            warehouse_id=DATABRICKS_WAREHOUSE_ID,
            statement=f"DESCRIBE TABLE {table_fqn}",
            wait_timeout="15s",
        )
        if resp.status and resp.status.state == StatementState.SUCCEEDED and resp.result:
            return [row[0] for row in resp.result.data_array if row[0] and not row[0].startswith("#")]
    except Exception:
        pass
    return []


# ── FMAPI Call ───────────────────────────────────────────────────────────────


def _call_fmapi(system_prompt: str, user_prompt: str, endpoint: str) -> str:
    w = WorkspaceClient()
    response = w.serving_endpoints.query(
        name=endpoint,
        messages=[
            ChatMessage(role=ChatMessageRole.SYSTEM, content=system_prompt),
            ChatMessage(role=ChatMessageRole.USER, content=user_prompt),
        ],
        max_tokens=4096,
    )
    return response.choices[0].message.content


# ── Public API ───────────────────────────────────────────────────────────────


def translate_dax(dax_model: DaxModel) -> TranslationResult:
    """Translate a DAX model into a Databricks Metric View definition."""
    endpoint = SERVING_ENDPOINT_NAME
    dax_json = dax_model.model_dump_json(indent=2)
    user_prompt = (
        "Translate the following DAX model into a Databricks Metric View definition.\n\n"
        "Return ONLY a JSON object matching the output schema. No markdown, no explanation.\n\n"
        f"DAX Model:\n{dax_json}"
    )

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response_text = _call_fmapi(DAX_SYSTEM_PROMPT, user_prompt, endpoint)
            raw = _extract_json(response_text)

            fact = dax_model.get_table(dax_model.fact_table)
            source_fallback = fact.databricks_table if fact and fact.databricks_table else ""
            view_name = f"{dax_model.catalog}.{dax_model.schema_name}.mv_{dax_model.name}"
            return _parse_common(raw, source_fallback, view_name)
        except (json.JSONDecodeError, Exception) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                continue

    raise RuntimeError(f"Failed to translate DAX after {MAX_RETRIES + 1} attempts: {last_error}")


def translate_mdx(cube: MdxCube) -> TranslationResult:
    """Translate an MDX OLAP cube into a Databricks Metric View definition."""
    endpoint = SERVING_ENDPOINT_NAME
    cube_json = cube.model_dump_json(indent=2)
    user_prompt = (
        "Translate the following MDX OLAP cube definition into a Databricks Metric View definition.\n\n"
        "Return ONLY a JSON object matching the output schema. No markdown, no explanation.\n\n"
        f"MDX Cube Definition:\n{cube_json}"
    )

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response_text = _call_fmapi(MDX_SYSTEM_PROMPT, user_prompt, endpoint)
            raw = _extract_json(response_text)

            source_fallback = cube.measure_group.databricks_table
            view_name = f"{cube.catalog}.{cube.schema_name}.mv_{cube.name.lower().replace(' ', '_')}"
            return _parse_common(raw, source_fallback, view_name)
        except (json.JSONDecodeError, Exception) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                continue

    raise RuntimeError(f"Failed to translate MDX after {MAX_RETRIES + 1} attempts: {last_error}")


def translate_text(
    measures_text: str,
    source_table: str,
    mode: str = "dax",
    catalog: str = "main",
    schema_name: str = "default",
    view_name: str = "",
    dimension_tables: list[dict] | None = None,
    model: str | None = None,
) -> TranslationResult:
    """Translate raw DAX/MDX measure text into a Metric View.

    This is the simple mode — user pastes measure definitions as text
    and provides a source table. Claude infers everything else.
    """
    from .config import get_config
    cfg = get_config()
    endpoint = model or cfg.get("serving_endpoint", SERVING_ENDPOINT_NAME)
    system_prompt = DAX_SYSTEM_PROMPT if mode == "dax" else MDX_SYSTEM_PROMPT

    # Append configurable prompt suffix if set
    suffix_key = "dax_prompt_suffix" if mode == "dax" else "mdx_prompt_suffix"
    prompt_suffix = cfg.get(suffix_key, "")

    dim_context = ""
    if dimension_tables:
        dim_context = "\n\nDimension tables available for star-schema joins:\n"
        for dt in dimension_tables:
            table_name = dt.get('table', '')
            join_key = dt.get('join_key', '')
            dim_key = dt.get('dim_key', '')
            dim_context += f"\n### {table_name}\n"
            dim_context += f"  Join: source.{join_key} = {table_name.split('.')[-1]}.{dim_key}\n"
            # Fetch actual columns from the table
            cols = _get_table_columns(table_name)
            if cols:
                dim_context += f"  Columns: {', '.join(cols)}\n"
            else:
                dim_context += f"  (column names unknown — use snake_case)\n"

    # Fetch fact table columns
    fact_cols = _get_table_columns(source_table)
    fact_context = ""
    if fact_cols:
        fact_context = f"\nFact table columns: {', '.join(fact_cols)}"

    label = "DAX measures" if mode == "dax" else "MDX calculated members"
    user_prompt = f"""Translate the following {label} into a Databricks Metric View definition.

Source table (fact table): {source_table}{fact_context}
{dim_context}

{label.capitalize()}:
{measures_text}

IMPORTANT:
- The source table is already in Databricks. Use it as the `source` field.
- ONLY use column names that actually exist in the tables listed above. Do NOT guess column names.
- For column names, use the exact snake_case names from the schema (e.g., `product_name` not `name`).
- If dimension tables are listed above with their columns, create joins and use those exact column names.
- Reference fact table columns without prefix. Reference dimension columns as `join_alias.column_name`.
- If measures reference dimension tables (RELATED, joins), include joins in the output.
{prompt_suffix}

Return ONLY a JSON object matching the output schema. No markdown, no explanation."""

    if not view_name:
        safe_name = source_table.split(".")[-1].replace("-", "_")
        view_name = f"mv_{safe_name}"
    full_view_name = f"{catalog}.{schema_name}.{view_name}"

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response_text = _call_fmapi(system_prompt, user_prompt, endpoint)
            raw = _extract_json(response_text)
            return _parse_common(raw, source_table, full_view_name)
        except (json.JSONDecodeError, Exception) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                continue

    raise RuntimeError(f"Failed to translate text after {MAX_RETRIES + 1} attempts: {last_error}")
