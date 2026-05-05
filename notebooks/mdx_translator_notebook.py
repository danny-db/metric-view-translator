# Databricks notebook source
# MAGIC %md
# MAGIC # MDX OLAP Cube-to-Databricks Metric View Translator
# MAGIC
# MAGIC Converts SSAS Multidimensional (OLAP) MDX calculated members into Databricks Metric View definitions using Claude via FMAPI.
# MAGIC
# MAGIC ## Setup
# MAGIC 1. Attach to a cluster with DBR 15.4+ (Python 3.11+)
# MAGIC 2. Run the `%pip install` cell below
# MAGIC 3. Configure the FMAPI endpoint name and target catalog/schema
# MAGIC 4. Define your MDX cube and run translation

# COMMAND ----------

# MAGIC %pip install "pydantic>=2.0" pyyaml --quiet --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# FMAPI endpoint for Claude (must exist as a serving endpoint in this workspace)
FMAPI_ENDPOINT = "databricks-claude-sonnet-4-6"

# Target location for test data and metric views
TARGET_CATALOG = "main"  # Change to your catalog
TARGET_SCHEMA = "mdx_translator_test"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Library (self-contained — no external package needed)

# COMMAND ----------

import json
import re
import textwrap
from enum import Enum
from typing import Optional

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from pydantic import BaseModel, Field


# ── MDX Input Models ─────────────────────────────────────────────────────────


class MdxAttribute(BaseModel):
    """An attribute (column) in an OLAP dimension."""
    name: str
    data_type: str = "STRING"


class MdxHierarchy(BaseModel):
    """A hierarchy within an OLAP dimension (e.g., Date -> Year -> Quarter -> Month -> Day)."""
    name: str
    levels: list[str] = Field(default_factory=list)


class MdxDimension(BaseModel):
    """An OLAP dimension (e.g., [Date], [Product], [Customer])."""
    name: str
    attributes: list[MdxAttribute] = Field(default_factory=list)
    hierarchies: list[MdxHierarchy] = Field(default_factory=list)
    databricks_table: Optional[str] = None
    join_key: Optional[str] = None
    dim_key: Optional[str] = None


class MdxMeasureGroup(BaseModel):
    """A measure group (fact table) in the OLAP cube."""
    name: str
    databricks_table: str
    columns: list[MdxAttribute] = Field(default_factory=list)


class MdxCalculatedMember(BaseModel):
    """A calculated member / measure in the OLAP cube."""
    name: str
    expression: str
    format_string: Optional[str] = None
    description: Optional[str] = None


class MdxCube(BaseModel):
    """Complete OLAP cube definition for translation."""
    name: str
    measure_group: MdxMeasureGroup
    dimensions: list[MdxDimension] = Field(default_factory=list)
    calculated_members: list[MdxCalculatedMember] = Field(default_factory=list)
    catalog: str = "main"
    schema_name: str = "mdx_translator_test"

    def get_dimension(self, name: str) -> Optional[MdxDimension]:
        for d in self.dimensions:
            if d.name == name:
                return d
        return None


# ── Translation Output Models ────────────────────────────────────────────────


class TranslationStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class TranslatedDimension(BaseModel):
    name: str
    expr: str
    comment: Optional[str] = None


class WindowSpec(BaseModel):
    order: str
    range: str
    semiadditive: str = "last"


class TranslatedMeasure(BaseModel):
    name: str
    expr: str
    comment: Optional[str] = None
    window: Optional[list[WindowSpec]] = None


class TranslatedJoin(BaseModel):
    name: str
    source: str
    on: Optional[str] = None
    using: Optional[list[str]] = None


class MeasureWarning(BaseModel):
    measure_name: str
    dax_expression: str
    warning_type: str
    message: str
    approximation: Optional[str] = None


class TranslationResult(BaseModel):
    status: TranslationStatus
    version: str = "1.1"
    source: str
    comment: Optional[str] = None
    joins: list[TranslatedJoin] = Field(default_factory=list)
    dimensions: list[TranslatedDimension] = Field(default_factory=list)
    measures: list[TranslatedMeasure] = Field(default_factory=list)
    warnings: list[MeasureWarning] = Field(default_factory=list)
    sql: str = ""
    yaml_body: str = ""

    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    @property
    def has_window_measures(self) -> bool:
        return any(m.window for m in self.measures)


# COMMAND ----------

# MAGIC %md
# MAGIC ## System Prompt

# COMMAND ----------

SYSTEM_PROMPT = r"""You are an expert at translating SSAS Multidimensional (OLAP) MDX calculated members into Databricks Metric View definitions.

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
| `IIF([Measures].[Revenue] > 0, [Measures].[Profit] / [Measures].[Revenue], 0)` | `CASE WHEN SUM(revenue) > 0 THEN SUM(profit) / SUM(revenue) ELSE 0 END` or `SUM(profit) / NULLIF(SUM(revenue), 0)` |

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
| `[Date].[Calendar].[Month]` | `DATE_TRUNC('MONTH', calendar.date)` or `calendar.month` |

## MDX Time Intelligence -> Window Measures
| MDX Pattern | Metric View Approach |
|------------|---------------------|
| `SUM(YTD(), [Measures].[Sales])` | Window measure: cumulative + current year. Version 0.1. |
| `AGGREGATE(PeriodsToDate([Date].[Calendar].[Year]), [Measures].[Sales])` | Same as YTD -- cumulative window + current year. |
| `ParallelPeriod([Date].[Calendar].[Year], 1, currentmember)` | Warning: time_intelligence. Approximation: trailing 1 year window. |
| `LastPeriods(6, currentmember)` | Warning: time_intelligence. Approximation: trailing 6 month window. |
| `AVG(LastPeriods(N), [Measures].[Sales])` | Trailing N window with AVG. |
| `ClosingPeriod([Date].[Calendar].[Month])` | Warning: time_intelligence. Semi-additive last value. |
| `CLOSINGBALANCEMONTH(...)` | Semi-additive: window current + semiadditive last. |

## Unsupported MDX
| MDX Pattern | Action |
|------------|--------|
| `RANK(...)` / `TopCount(...)` / `BottomCount(...)` | Warning: unsupported_dax (use same type) |
| `Generate(...)` / `CrossJoin(...)` | Warning: unsupported_dax |
| `SCOPE` / `FREEZE` / cell-level calculations | Warning: unsupported_dax |
| `LinkMember(...)` | Warning: unsupported_dax |
| String functions on measures | Warning: unsupported_dax |
| `.Parent` / `.PrevMember` / `.NextMember` / `.Lag()` / `.Lead()` member navigation | Warning: unsupported_dax. These require SQL window functions which are forbidden. |
| `([Measures].[X], [Dim].Parent)` or any parent-relative reference | Warning: unsupported_dax. Cannot compute parent-level aggregation in Metric Views. |

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
- Window measures require version "0.1" -- no `comment` fields allowed in 0.1

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

1. **Column naming**: ALWAYS use lowercase snake_case for ALL column references in SQL expressions. MDX `[Measures].[Sales Amount]` -> `amount`. `OrderID` -> `order_id`. `CustomerID` -> `customer_id`. `UnitPrice` -> `unit_price`. `ProductID` -> `product_id`. `DateKey` -> `date_key`. `ProductName` -> `product_name`. The Databricks tables use snake_case columns -- PascalCase will cause errors.
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


def build_user_prompt(mdx_cube_json: str) -> str:
    """Build the user prompt containing the MDX cube to translate."""
    return f"""Translate the following MDX OLAP cube definition into a Databricks Metric View definition.

Return ONLY a JSON object matching the output schema. No markdown, no explanation.

MDX Cube Definition:
{mdx_cube_json}"""


# COMMAND ----------

# MAGIC %md
# MAGIC ## Translator Engine

# COMMAND ----------

MAX_RETRIES = 2
VALID_WINDOW_UNITS = {"day", "month", "year", "quarter", "week", "hour"}


def _fix_window_range(raw_range: str) -> str:
    """Normalize window range values that Claude may format incorrectly.

    Valid: current, cumulative, all, trailing <N> <unit>, leading <N> <unit>
    Fixes: trailing_3_months -> trailing 3 month, trailing 6 months -> trailing 6 month
    """
    raw_range = raw_range.strip()
    if raw_range in ("current", "cumulative", "all"):
        return raw_range
    # Normalize underscores to spaces
    normalized = raw_range.replace("_", " ")
    # Fix plural units: "months" -> "month", "years" -> "year"
    for unit in list(VALID_WINDOW_UNITS):
        normalized = re.sub(rf"\b{unit}s\b", unit, normalized)
    return normalized


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
    doc = {}
    v01 = result.version == "0.1"

    doc["version"] = result.version
    if result.comment and not v01:
        doc["comment"] = result.comment
    doc["source"] = result.source

    if result.joins:
        doc["joins"] = []
        for j in result.joins:
            jd = {"name": j.name, "source": j.source}
            if j.on:
                jd["on"] = j.on
            if j.using:
                jd["using"] = j.using
            doc["joins"].append(jd)

    doc["dimensions"] = []
    for d in result.dimensions:
        dd = {"name": d.name, "expr": d.expr}
        if d.comment and not v01:
            dd["comment"] = d.comment
        doc["dimensions"].append(dd)

    doc["measures"] = []
    for m in result.measures:
        md = {"name": m.name, "expr": m.expr}
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
    return f"CREATE OR REPLACE VIEW {view_name}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n{indented}\n$$"


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
                    # Try mapping expr -> name
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
                message=f"Measure uses SQL window function (OVER), which is not supported in Metric Views. Removed from output.",
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


def translate(cube: MdxCube, endpoint: str = None) -> TranslationResult:
    """Translate an MDX OLAP cube into a Databricks Metric View definition using FMAPI."""
    endpoint = endpoint or FMAPI_ENDPOINT
    w = WorkspaceClient()

    # Compatible with both Pydantic v1 (.json()) and v2 (.model_dump_json())
    if hasattr(cube, "model_dump_json"):
        cube_json = cube.model_dump_json(indent=2)
    else:
        cube_json = cube.json(indent=2)

    user_prompt = build_user_prompt(cube_json)

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            response = w.serving_endpoints.query(
                name=endpoint,
                messages=[
                    ChatMessage(role=ChatMessageRole.SYSTEM, content=SYSTEM_PROMPT),
                    ChatMessage(role=ChatMessageRole.USER, content=user_prompt),
                ],
                max_tokens=4096,
            )
            response_text = response.choices[0].message.content
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


# COMMAND ----------

# MAGIC %md
# MAGIC ## Deployer

# COMMAND ----------

from databricks.sdk.service.sql import StatementState
import time


def execute_sql(sql: str, catalog: str = TARGET_CATALOG, schema: str = TARGET_SCHEMA) -> list[dict]:
    """Execute SQL on the workspace SQL warehouse."""
    w = WorkspaceClient()
    warehouses = list(w.warehouses.list())
    wh_id = None
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            wh_id = wh.id
            break
    if not wh_id and warehouses:
        wh_id = warehouses[0].id
    if not wh_id:
        raise RuntimeError("No SQL warehouse found")

    response = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=wh_id, catalog=catalog, schema=schema, wait_timeout="50s",
    )
    while response.status and response.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(2)
        response = w.statement_execution.get_statement(response.statement_id)
    if response.status and response.status.state == StatementState.FAILED:
        raise RuntimeError(f"SQL failed: {response.status.error}")
    if not response.result or not response.result.data_array:
        return []
    columns = [col.name for col in response.manifest.schema.columns]
    return [dict(zip(columns, row)) for row in response.result.data_array]


def deploy_metric_view(sql: str) -> None:
    execute_sql(sql)


def query_measure(view_name: str, measure_name: str) -> str:
    rows = execute_sql(f"SELECT MEASURE(`{measure_name}`) AS result FROM {view_name} GROUP BY ALL")
    return rows[0]["result"] if rows else None


# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Data

# COMMAND ----------

fq = f"{TARGET_CATALOG}.{TARGET_SCHEMA}"

execute_sql(f"CREATE SCHEMA IF NOT EXISTS {fq}")

execute_sql(f"""
CREATE OR REPLACE TABLE {fq}.dim_date AS
SELECT date, YEAR(date) AS year, MONTH(date) AS month, CONCAT('Q', QUARTER(date)) AS quarter, DATE_TRUNC('MONTH', date) AS month_start
FROM (SELECT EXPLODE(SEQUENCE(DATE'2023-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS date)
""")

execute_sql(f"""
CREATE OR REPLACE TABLE {fq}.dim_customer AS
SELECT * FROM VALUES
    (1,'Acme Corp','Enterprise','Australia'), (2,'Beta Inc','Mid-Market','Australia'),
    (3,'Gamma Ltd','SMB','New Zealand'), (4,'Delta Co','Enterprise','Australia'),
    (5,'Epsilon Pty','Mid-Market','Australia'), (6,'Zeta Group','Enterprise','Singapore'),
    (7,'Eta Systems','SMB','Australia'), (8,'Theta Data','Enterprise','Australia'),
    (9,'Iota Labs','Mid-Market','New Zealand'), (10,'Kappa Tech','SMB','Australia'),
    (11,'Lambda AI','Enterprise','Singapore'), (12,'Mu Analytics','Mid-Market','Australia'),
    (13,'Nu Cloud','SMB','Australia'), (14,'Xi Solutions','Enterprise','New Zealand'),
    (15,'Omicron Digital','Mid-Market','Australia')
AS t(customer_id, name, segment, country)
""")

execute_sql(f"""
CREATE OR REPLACE TABLE {fq}.dim_product AS
SELECT * FROM VALUES
    (1,'Widget A','Hardware'), (2,'Widget B','Hardware'), (3,'Service X','Services'),
    (4,'Service Y','Services'), (5,'License Pro','Software'), (6,'License Std','Software'),
    (7,'Addon Z','Addons'), (8,'Platform 1','Platform'), (9,'Platform 2','Platform'), (10,'Custom Sol','Services')
AS t(product_id, product_name, category)
""")

execute_sql(f"""
CREATE OR REPLACE TABLE {fq}.fact_sales AS
WITH base AS (SELECT EXPLODE(SEQUENCE(1, 100)) AS order_id)
SELECT order_id, (order_id % 15) + 1 AS customer_id, (order_id % 10) + 1 AS product_id,
    DATE_ADD(DATE'2023-01-15', (order_id * 7) % 1050) AS date_key,
    ROUND(100 + (order_id * 37 % 4900), 2) AS amount,
    (order_id % 20) + 1 AS quantity, ROUND(10 + (order_id * 13 % 490), 2) AS unit_price,
    ROUND((100 + (order_id * 37 % 4900)) * 0.15, 2) AS profit,
    ROUND((100 + (order_id * 37 % 4900)) * 1.0, 2) AS revenue,
    CASE WHEN order_id % 3 = 0 THEN 'Inactive' ELSE 'Active' END AS status,
    CASE WHEN order_id % 4 = 0 THEN 'West' WHEN order_id % 4 = 1 THEN 'East' WHEN order_id % 4 = 2 THEN 'North' ELSE 'South' END AS region,
    CASE WHEN order_id % 5 = 0 THEN 'Premium' WHEN order_id % 5 = 1 THEN 'Gold' ELSE 'Standard' END AS tier
FROM base
""")

print(f"Test data created in {fq}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 1: Simple MDX Base Measures
# MAGIC `SUM`, `COUNT`, `DistinctCount`, `AVG` — basic aggregation measures

# COMMAND ----------

level1 = MdxCube(
    name="level1_simple",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    measure_group=MdxMeasureGroup(
        name="Sales",
        databricks_table=f"{fq}.fact_sales",
        columns=[
            MdxAttribute(name="OrderID", data_type="INT"),
            MdxAttribute(name="CustomerID", data_type="INT"),
            MdxAttribute(name="Amount", data_type="DECIMAL"),
            MdxAttribute(name="Quantity", data_type="INT"),
            MdxAttribute(name="UnitPrice", data_type="DECIMAL"),
            MdxAttribute(name="Status", data_type="STRING"),
            MdxAttribute(name="Region", data_type="STRING"),
        ],
    ),
    dimensions=[
        MdxDimension(name="Region", attributes=[MdxAttribute(name="Region")]),
        MdxDimension(name="Status", attributes=[MdxAttribute(name="Status")]),
    ],
    calculated_members=[
        MdxCalculatedMember(
            name="Total Sales",
            expression="[Measures].[Sales Amount]",
            description="Base SUM measure on Amount column (aggregation type: Sum)",
        ),
        MdxCalculatedMember(
            name="Order Count",
            expression="[Measures].[Order Count]",
            description="Base COUNT measure on OrderID column (aggregation type: Count)",
        ),
        MdxCalculatedMember(
            name="Unique Customers",
            expression="[Measures].[Distinct Customer Count]",
            description="DistinctCount measure on CustomerID (aggregation type: DistinctCount)",
        ),
        MdxCalculatedMember(
            name="Avg Order Value",
            expression="[Measures].[Sales Amount] / [Measures].[Order Count]",
            description="Average order value = Total Sales / Order Count",
        ),
    ],
)

result1 = translate(level1)
print(f"Status: {result1.status.value} | Dims: {len(result1.dimensions)} | Measures: {len(result1.measures)}")
print(result1.sql)

# COMMAND ----------

deploy_metric_view(result1.sql)

# Validate
mv_total = query_measure(f"{fq}.mv_level1_simple", "Total Sales")
direct_total = execute_sql(f"SELECT SUM(amount) AS v FROM {fq}.fact_sales")[0]["v"]
print(f"Total Sales: MV={mv_total} vs SQL={direct_total} -> {'PASS' if float(mv_total) == float(direct_total) else 'FAIL'}")

mv_count = query_measure(f"{fq}.mv_level1_simple", "Order Count")
print(f"Order Count: {mv_count}")

mv_unique = query_measure(f"{fq}.mv_level1_simple", "Unique Customers")
print(f"Unique Customers: {mv_unique}")

mv_avg = query_measure(f"{fq}.mv_level1_simple", "Avg Order Value")
print(f"Avg Order Value: {mv_avg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 2: Medium MDX — IIF Conditions, Dimension Joins, Tuple Filters
# MAGIC Tuple filter notation, `IIF` conditionals, dimension join references

# COMMAND ----------

level2 = MdxCube(
    name="level2_medium",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    measure_group=MdxMeasureGroup(
        name="Sales",
        databricks_table=f"{fq}.fact_sales",
        columns=[
            MdxAttribute(name="OrderID", data_type="INT"),
            MdxAttribute(name="CustomerID", data_type="INT"),
            MdxAttribute(name="ProductID", data_type="INT"),
            MdxAttribute(name="Amount", data_type="DECIMAL"),
            MdxAttribute(name="Quantity", data_type="INT"),
            MdxAttribute(name="UnitPrice", data_type="DECIMAL"),
            MdxAttribute(name="Profit", data_type="DECIMAL"),
            MdxAttribute(name="Revenue", data_type="DECIMAL"),
            MdxAttribute(name="Status", data_type="STRING"),
            MdxAttribute(name="Region", data_type="STRING"),
            MdxAttribute(name="Tier", data_type="STRING"),
        ],
    ),
    dimensions=[
        MdxDimension(
            name="Customer",
            databricks_table=f"{fq}.dim_customer",
            join_key="customer_id", dim_key="customer_id",
            attributes=[
                MdxAttribute(name="CustomerID", data_type="INT"),
                MdxAttribute(name="Name"), MdxAttribute(name="Segment"), MdxAttribute(name="Country"),
            ],
        ),
        MdxDimension(
            name="Product",
            databricks_table=f"{fq}.dim_product",
            join_key="product_id", dim_key="product_id",
            attributes=[
                MdxAttribute(name="ProductID", data_type="INT"),
                MdxAttribute(name="ProductName"), MdxAttribute(name="Category"),
            ],
        ),
    ],
    calculated_members=[
        MdxCalculatedMember(
            name="Active Sales",
            expression="([Status].&[Active], [Measures].[Sales Amount])",
            description="Sales filtered to Status=Active using tuple notation",
        ),
        MdxCalculatedMember(
            name="Profit Margin",
            expression="IIF([Measures].[Revenue] > 0, [Measures].[Profit] / [Measures].[Revenue], 0)",
            format_string="Percent",
            description="Profit divided by Revenue, 0 if no revenue",
        ),
        MdxCalculatedMember(
            name="Line Total",
            expression="[Measures].[Unit Price] * [Measures].[Quantity]",
            description="Revenue calculated as UnitPrice * Quantity (SUM of product)",
        ),
        MdxCalculatedMember(
            name="Sales Contribution",
            expression="[Measures].[Sales Amount] / ([Measures].[Sales Amount], [Product].[Category].Parent)",
            description="Sales as percentage of parent category",
        ),
    ],
)

result2 = translate(level2)
print(f"Status: {result2.status.value} | Joins: {len(result2.joins)} | Measures: {len(result2.measures)}")
deploy_metric_view(result2.sql)

# Validate
mv_active = query_measure(f"{fq}.mv_level2_medium", "Active Sales")
sql_active = execute_sql(f"SELECT SUM(amount) AS v FROM {fq}.fact_sales WHERE status = 'Active'")[0]["v"]
print(f"Active Sales: MV={mv_active} vs SQL={sql_active} -> {'PASS' if float(mv_active) == float(sql_active) else 'FAIL'}")

mv_line = query_measure(f"{fq}.mv_level2_medium", "Line Total")
sql_line = execute_sql(f"SELECT SUM(unit_price * quantity) AS v FROM {fq}.fact_sales")[0]["v"]
print(f"Line Total: MV={mv_line} vs SQL={sql_line} -> {'PASS' if float(mv_line) == float(sql_line) else 'FAIL'}")

mv_margin = query_measure(f"{fq}.mv_level2_medium", "Profit Margin")
sql_margin = execute_sql(f"SELECT SUM(profit)/NULLIF(SUM(revenue),0) AS v FROM {fq}.fact_sales")[0]["v"]
print(f"Profit Margin: MV={mv_margin} vs SQL={sql_margin} -> {'PASS' if abs(float(mv_margin) - float(sql_margin)) < 0.01 else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 3: Time Intelligence (Window Measures)
# MAGIC `PeriodsToDate` (YTD), `ParallelPeriod`, `LastPeriods`, `ClosingPeriod`

# COMMAND ----------

level3 = MdxCube(
    name="level3_time_intel",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    measure_group=MdxMeasureGroup(
        name="Sales",
        databricks_table=f"{fq}.fact_sales",
        columns=[
            MdxAttribute(name="OrderID", data_type="INT"),
            MdxAttribute(name="CustomerID", data_type="INT"),
            MdxAttribute(name="DateKey", data_type="DATE"),
            MdxAttribute(name="Amount", data_type="DECIMAL"),
            MdxAttribute(name="Status", data_type="STRING"),
        ],
    ),
    dimensions=[
        MdxDimension(
            name="Date",
            databricks_table=f"{fq}.dim_date",
            join_key="date_key", dim_key="date",
            attributes=[
                MdxAttribute(name="Date", data_type="DATE"),
                MdxAttribute(name="Year", data_type="INT"),
                MdxAttribute(name="Month", data_type="INT"),
                MdxAttribute(name="Quarter"),
            ],
            hierarchies=[
                MdxHierarchy(name="Calendar", levels=["Year", "Quarter", "Month", "Date"]),
            ],
        ),
    ],
    calculated_members=[
        MdxCalculatedMember(
            name="YTD Sales",
            expression="AGGREGATE(PeriodsToDate([Date].[Calendar].[Calendar Year], [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])",
            description="Year-to-date sales using PeriodsToDate",
        ),
        MdxCalculatedMember(
            name="Prior Year Sales",
            expression="(ParallelPeriod([Date].[Calendar].[Calendar Year], 1, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])",
            format_string="$#,#.00",
            description="Sales for the same period one year ago",
        ),
        MdxCalculatedMember(
            name="Rolling 6M Avg",
            expression="AVG(LastPeriods(6, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])",
            description="Average sales over last 6 periods",
        ),
        MdxCalculatedMember(
            name="YoY Growth",
            expression="IIF((ParallelPeriod([Date].[Calendar].[Calendar Year], 1, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount]) > 0, ([Measures].[Sales Amount] - (ParallelPeriod([Date].[Calendar].[Calendar Year], 1, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])) / (ParallelPeriod([Date].[Calendar].[Calendar Year], 1, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount]), NULL)",
            format_string="Percent",
            description="Year-over-year growth rate",
        ),
        MdxCalculatedMember(
            name="Closing Balance",
            expression="(ClosingPeriod([Date].[Calendar].[Month], [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])",
            description="Value at the closing period of the month",
        ),
    ],
)

result3 = translate(level3)
print(f"Status: {result3.status.value} | Version: {result3.version} | Warnings: {len(result3.warnings)}")
for w in result3.warnings:
    print(f"  [{w.warning_type}] {w.measure_name}: {w.message[:80]}")

try:
    deploy_metric_view(result3.sql)
    print("Deployed!")
except Exception as e:
    print(f"Deploy issue: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 4: Complex — Multi-Dimension Star Schema, Composite Calculated Members, FILTER
# MAGIC `DIVIDE` patterns, composite member references, multi-filter tuples, 3-way joins

# COMMAND ----------

level4 = MdxCube(
    name="level4_complex",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    measure_group=MdxMeasureGroup(
        name="Sales",
        databricks_table=f"{fq}.fact_sales",
        columns=[
            MdxAttribute(name="OrderID", data_type="INT"),
            MdxAttribute(name="CustomerID", data_type="INT"),
            MdxAttribute(name="ProductID", data_type="INT"),
            MdxAttribute(name="DateKey", data_type="DATE"),
            MdxAttribute(name="Amount", data_type="DECIMAL"),
            MdxAttribute(name="Quantity", data_type="INT"),
            MdxAttribute(name="UnitPrice", data_type="DECIMAL"),
            MdxAttribute(name="Profit", data_type="DECIMAL"),
            MdxAttribute(name="Revenue", data_type="DECIMAL"),
            MdxAttribute(name="Status", data_type="STRING"),
            MdxAttribute(name="Region", data_type="STRING"),
            MdxAttribute(name="Tier", data_type="STRING"),
        ],
    ),
    dimensions=[
        MdxDimension(
            name="Customer",
            databricks_table=f"{fq}.dim_customer",
            join_key="customer_id", dim_key="customer_id",
            attributes=[
                MdxAttribute(name="CustomerID", data_type="INT"),
                MdxAttribute(name="Name"), MdxAttribute(name="Segment"), MdxAttribute(name="Country"),
            ],
        ),
        MdxDimension(
            name="Product",
            databricks_table=f"{fq}.dim_product",
            join_key="product_id", dim_key="product_id",
            attributes=[
                MdxAttribute(name="ProductID", data_type="INT"),
                MdxAttribute(name="ProductName"), MdxAttribute(name="Category"),
            ],
        ),
        MdxDimension(
            name="Date",
            databricks_table=f"{fq}.dim_date",
            join_key="date_key", dim_key="date",
            attributes=[
                MdxAttribute(name="Date", data_type="DATE"),
                MdxAttribute(name="Year", data_type="INT"),
                MdxAttribute(name="Month", data_type="INT"),
            ],
            hierarchies=[MdxHierarchy(name="Calendar", levels=["Year", "Month", "Date"])],
        ),
    ],
    calculated_members=[
        MdxCalculatedMember(
            name="Revenue per Customer",
            expression="[Measures].[Sales Amount] / [Measures].[Distinct Customer Count]",
            format_string="$#,#.00",
            description="Total sales divided by distinct customer count",
        ),
        MdxCalculatedMember(
            name="Premium Active Sales",
            expression="([Status].&[Active], [Tier].&[Premium], [Measures].[Sales Amount])",
            description="Sales for Active status AND Premium tier",
        ),
        MdxCalculatedMember(
            name="Premium Ratio",
            expression="IIF([Measures].[Sales Amount] > 0, ([Status].&[Active], [Tier].&[Premium], [Measures].[Sales Amount]) / [Measures].[Sales Amount], 0)",
            format_string="Percent",
            description="Premium active sales as % of total",
        ),
        MdxCalculatedMember(
            name="Gross Margin",
            expression="IIF([Measures].[Revenue] > 0, ([Measures].[Revenue] - [Measures].[Sales Amount]) / [Measures].[Revenue], 0)",
            format_string="Percent",
            description="Gross margin percentage",
        ),
        MdxCalculatedMember(
            name="Weighted Avg Price",
            expression="[Measures].[Line Total] / [Measures].[Total Quantity]",
            description="Weighted average price = SUM(UnitPrice*Qty) / SUM(Qty)",
        ),
    ],
)

result4 = translate(level4)
print(f"Status: {result4.status.value} | Joins: {len(result4.joins)} | Dims: {len(result4.dimensions)} | Measures: {len(result4.measures)}")
deploy_metric_view(result4.sql)

# Validate
for name, sql_expr in [
    ("Revenue per Customer", f"SELECT SUM(amount)/NULLIF(COUNT(DISTINCT customer_id),0) FROM {fq}.fact_sales"),
    ("Premium Active Sales", f"SELECT SUM(amount) FROM {fq}.fact_sales WHERE status='Active' AND tier='Premium'"),
    ("Gross Margin", f"SELECT (SUM(revenue) - SUM(amount))/NULLIF(SUM(revenue),0) FROM {fq}.fact_sales"),
    ("Weighted Avg Price", f"SELECT SUM(unit_price * quantity)/NULLIF(SUM(quantity),0) FROM {fq}.fact_sales"),
]:
    mv_val = query_measure(f"{fq}.mv_level4_complex", name)
    sql_val = list(execute_sql(sql_expr)[0].values())[0]
    match = abs(float(mv_val) - float(sql_val)) < 1
    print(f"  {name}: MV={mv_val} vs SQL={sql_val} -> {'PASS' if match else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 5: Advanced — Nested IIF (KPI), FILTER Count, Cumulative, Pct of Total
# MAGIC Traffic-light KPI, `FILTER` member count, `SUM(NULL:CurrentMember)`, `[All]` total reference

# COMMAND ----------

level5 = MdxCube(
    name="level5_advanced",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    measure_group=MdxMeasureGroup(
        name="Sales",
        databricks_table=f"{fq}.fact_sales",
        columns=[
            MdxAttribute(name="OrderID", data_type="INT"),
            MdxAttribute(name="CustomerID", data_type="INT"),
            MdxAttribute(name="ProductID", data_type="INT"),
            MdxAttribute(name="DateKey", data_type="DATE"),
            MdxAttribute(name="Amount", data_type="DECIMAL"),
            MdxAttribute(name="Quantity", data_type="INT"),
            MdxAttribute(name="UnitPrice", data_type="DECIMAL"),
            MdxAttribute(name="Profit", data_type="DECIMAL"),
            MdxAttribute(name="Revenue", data_type="DECIMAL"),
            MdxAttribute(name="Status", data_type="STRING"),
            MdxAttribute(name="Region", data_type="STRING"),
            MdxAttribute(name="Tier", data_type="STRING"),
        ],
    ),
    dimensions=[
        MdxDimension(name="Customer", databricks_table=f"{fq}.dim_customer",
                    join_key="customer_id", dim_key="customer_id",
                    attributes=[MdxAttribute(name="CustomerID", data_type="INT"),
                                MdxAttribute(name="Name"), MdxAttribute(name="Segment")]),
        MdxDimension(name="Product", databricks_table=f"{fq}.dim_product",
                    join_key="product_id", dim_key="product_id",
                    attributes=[MdxAttribute(name="ProductID", data_type="INT"),
                                MdxAttribute(name="ProductName"), MdxAttribute(name="Category")]),
        MdxDimension(name="Date", databricks_table=f"{fq}.dim_date",
                    join_key="date_key", dim_key="date",
                    attributes=[MdxAttribute(name="Date", data_type="DATE"),
                                MdxAttribute(name="Year", data_type="INT"),
                                MdxAttribute(name="Month", data_type="INT")],
                    hierarchies=[MdxHierarchy(name="Calendar", levels=["Year", "Month", "Date"])]),
    ],
    calculated_members=[
        MdxCalculatedMember(
            name="Sales KPI Status",
            expression='IIF([Measures].[Sales Amount] > 3000, "Green", IIF([Measures].[Sales Amount] > 1000, "Yellow", "Red"))',
            description="Traffic light: Green > 3000, Yellow > 1000, Red otherwise",
        ),
        MdxCalculatedMember(
            name="High Value Order Count",
            expression="COUNT(FILTER([Status].Members, [Measures].[Sales Amount] > 1000))",
            description="Count of status members where sales exceed 1000",
        ),
        MdxCalculatedMember(
            name="Cumulative Sales",
            expression="SUM(NULL:[Date].[Calendar].CurrentMember, [Measures].[Sales Amount])",
            description="Running total from beginning to current date member",
        ),
        MdxCalculatedMember(
            name="Pct of Total",
            expression="[Measures].[Sales Amount] / ([Measures].[Sales Amount], [Product].[Category].[All])",
            description="Sales as percentage of grand total across all products",
        ),
        MdxCalculatedMember(
            name="3M Moving Avg",
            expression="AVG(LastPeriods(3, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])",
            description="3-month moving average of sales",
        ),
        MdxCalculatedMember(
            name="Max Order",
            expression="MAX([Customer].[Name].Members, [Measures].[Sales Amount])",
            description="Maximum sales amount across all customers",
        ),
    ],
)

result5 = translate(level5)
print(f"Status: {result5.status.value} | v{result5.version} | Measures: {len(result5.measures)} | Warnings: {len(result5.warnings)}")
for w in result5.warnings:
    print(f"  [{w.warning_type}] {w.measure_name}: {w.message[:100]}")
for m in result5.measures:
    tag = " [WINDOW]" if m.window else ""
    print(f"  -> {m.name}: {m.expr[:80]}{tag}")

# COMMAND ----------

deploy_metric_view(result5.sql)
warned5 = {w.measure_name for w in result5.warnings}
for m in result5.measures:
    if m.name in warned5:
        continue
    try:
        val = query_measure(f"{fq}.mv_level5_advanced", m.name)
        print(f"  MEASURE({m.name}) = {val}")
    except Exception as e:
        print(f"  MEASURE({m.name}) ERROR: {e}")

# Validate key measures
for name, sql in [
    ("Pct of Total", f"SELECT SUM(amount) / NULLIF((SELECT SUM(amount) FROM {fq}.fact_sales), 0) FROM {fq}.fact_sales"),
]:
    if name in warned5:
        continue
    try:
        mv_val = float(query_measure(f"{fq}.mv_level5_advanced", name))
        sql_val = float(list(execute_sql(sql)[0].values())[0])
        print(f"  {name}: MV={mv_val} vs SQL={sql_val} -> {'PASS' if abs(mv_val - sql_val) < 0.01 else 'FAIL'}")
    except Exception as e:
        print(f"  {name}: validation error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 6: Expert MDX — Pushing the Limits
# MAGIC `RANK`, `TopCount`, `Generate`, `FILTER` with tuples, `PeriodsToDate + ParallelPeriod` combined,
# MAGIC `UNARY_OPERATOR`, string expressions on members, nested calculated member references.
# MAGIC
# MAGIC Many of these should produce warnings — tests graceful degradation.

# COMMAND ----------

level6 = MdxCube(
    name="level6_expert",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    measure_group=MdxMeasureGroup(
        name="Sales",
        databricks_table=f"{fq}.fact_sales",
        columns=[
            MdxAttribute(name="OrderID", data_type="INT"),
            MdxAttribute(name="CustomerID", data_type="INT"),
            MdxAttribute(name="ProductID", data_type="INT"),
            MdxAttribute(name="DateKey", data_type="DATE"),
            MdxAttribute(name="Amount", data_type="DECIMAL"),
            MdxAttribute(name="Quantity", data_type="INT"),
            MdxAttribute(name="UnitPrice", data_type="DECIMAL"),
            MdxAttribute(name="Profit", data_type="DECIMAL"),
            MdxAttribute(name="Revenue", data_type="DECIMAL"),
            MdxAttribute(name="Status", data_type="STRING"),
            MdxAttribute(name="Region", data_type="STRING"),
            MdxAttribute(name="Tier", data_type="STRING"),
        ],
    ),
    dimensions=[
        MdxDimension(name="Customer", databricks_table=f"{fq}.dim_customer",
                    join_key="customer_id", dim_key="customer_id",
                    attributes=[MdxAttribute(name="CustomerID", data_type="INT"),
                                MdxAttribute(name="Name"), MdxAttribute(name="Segment")]),
        MdxDimension(name="Product", databricks_table=f"{fq}.dim_product",
                    join_key="product_id", dim_key="product_id",
                    attributes=[MdxAttribute(name="ProductID", data_type="INT"),
                                MdxAttribute(name="ProductName"), MdxAttribute(name="Category")]),
        MdxDimension(name="Date", databricks_table=f"{fq}.dim_date",
                    join_key="date_key", dim_key="date",
                    attributes=[MdxAttribute(name="Date", data_type="DATE"),
                                MdxAttribute(name="Year", data_type="INT")],
                    hierarchies=[MdxHierarchy(name="Calendar", levels=["Year", "Date"])]),
    ],
    calculated_members=[
        MdxCalculatedMember(
            name="Product Rank",
            expression="RANK([Product].[ProductName].CurrentMember, ORDER([Product].[ProductName].Members, [Measures].[Sales Amount], BDESC))",
            description="Rank of current product by sales descending",
        ),
        MdxCalculatedMember(
            name="Top 5 Customer Revenue",
            expression="SUM(TopCount([Customer].[Name].Members, 5, [Measures].[Sales Amount]), [Measures].[Sales Amount])",
            description="Total revenue from top 5 customers",
        ),
        MdxCalculatedMember(
            name="Multi Region Sales",
            expression="SUM(Generate([Region].Members, {[Region].CurrentMember}), [Measures].[Sales Amount])",
            description="Sales across generated region set",
        ),
        MdxCalculatedMember(
            name="Active Customer Count",
            expression="COUNT(FILTER([Customer].[Name].Members, ([Status].&[Active], [Measures].[Sales Amount]) > 0))",
            description="Count of customers with active sales > 0",
        ),
        MdxCalculatedMember(
            name="YTD vs PY YTD",
            expression="SUM(PeriodsToDate([Date].[Calendar].[Calendar Year]), [Measures].[Sales Amount]) - SUM(PeriodsToDate([Date].[Calendar].[Calendar Year]), (ParallelPeriod([Date].[Calendar].[Calendar Year], 1), [Measures].[Sales Amount]))",
            description="YTD sales minus prior year YTD sales",
        ),
        MdxCalculatedMember(
            name="Custom Rollup",
            expression="[Measures].[Sales Amount] * [Product].[Category].CurrentMember.Properties('UNARY_OPERATOR')",
            description="Custom rollup using unary operator property",
        ),
        MdxCalculatedMember(
            name="Region Label",
            expression="[Region].CurrentMember.Name + ' - ' + FORMAT([Measures].[Sales Amount], '$#,#')",
            description="String concatenation of region name and formatted sales",
        ),
        MdxCalculatedMember(
            name="Profit per Customer",
            expression="IIF([Measures].[Distinct Customer Count] > 0, [Measures].[Profit] / [Measures].[Distinct Customer Count], 0)",
            description="Profit divided by distinct customer count",
        ),
    ],
)

result6 = translate(level6)
print(f"Status: {result6.status.value} | v{result6.version} | Measures: {len(result6.measures)} | Warnings: {len(result6.warnings)}")
print()
print("WARNINGS (expected for expert-level MDX):")
for w in result6.warnings:
    print(f"  [{w.warning_type}] {w.measure_name}: {w.message[:120]}")
print()
print("TRANSLATED MEASURES:")
for m in result6.measures:
    tag = " [WINDOW]" if m.window else ""
    print(f"  -> {m.name}: {m.expr[:90]}{tag}")

# COMMAND ----------

try:
    deploy_metric_view(result6.sql)
    print("Level 6 deployed!")
    warned6 = {w.measure_name for w in result6.warnings}
    for m in result6.measures:
        if m.name in warned6:
            print(f"  {m.name}: SKIPPED (warned)")
            continue
        try:
            val = query_measure(f"{fq}.mv_level6_expert", m.name)
            print(f"  MEASURE({m.name}) = {val}")
        except Exception as e:
            print(f"  MEASURE({m.name}) ERROR: {e}")
except Exception as e:
    print(f"Deploy failed (expected for some window measure combos): {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom MDX Cube — Define Your Own
# MAGIC
# MAGIC Edit the cube below with your own SSAS MDX calculated members and run translation.

# COMMAND ----------

# my_cube = MdxCube(
#     name="my_custom_cube",
#     catalog="my_catalog",
#     schema_name="my_schema",
#     measure_group=MdxMeasureGroup(
#         name="MyFact",
#         databricks_table="my_catalog.my_schema.my_fact_table",
#         columns=[
#             MdxAttribute(name="Amount", data_type="DECIMAL"),
#             MdxAttribute(name="CustomerID", data_type="INT"),
#         ],
#     ),
#     dimensions=[
#         MdxDimension(
#             name="Customer",
#             databricks_table="my_catalog.my_schema.dim_customer",
#             join_key="customer_id", dim_key="customer_id",
#             attributes=[
#                 MdxAttribute(name="CustomerID", data_type="INT"),
#                 MdxAttribute(name="Name"),
#                 MdxAttribute(name="Segment"),
#             ],
#         ),
#     ],
#     calculated_members=[
#         MdxCalculatedMember(
#             name="Total Revenue",
#             expression="[Measures].[Sales Amount]",
#             description="Base SUM measure on Amount column",
#         ),
#         MdxCalculatedMember(
#             name="Customer Count",
#             expression="[Measures].[Distinct Customer Count]",
#             description="DistinctCount on CustomerID",
#         ),
#         MdxCalculatedMember(
#             name="Revenue per Customer",
#             expression="[Measures].[Sales Amount] / [Measures].[Distinct Customer Count]",
#             description="Average revenue per customer",
#         ),
#     ],
# )
#
# my_result = translate(my_cube)
# print(my_result.sql)
# deploy_metric_view(my_result.sql)
