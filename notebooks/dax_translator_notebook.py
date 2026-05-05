# Databricks notebook source
# MAGIC %md
# MAGIC # DAX-to-Databricks Metric View Translator
# MAGIC
# MAGIC Converts Power BI DAX measures into Databricks Metric View definitions using Claude via FMAPI.
# MAGIC
# MAGIC ## Setup
# MAGIC 1. Attach to a cluster with DBR 15.4+ (Python 3.11+)
# MAGIC 2. Run the `%pip install` cell below
# MAGIC 3. Configure the FMAPI endpoint name and target catalog/schema
# MAGIC 4. Define your DAX model and run translation

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
TARGET_SCHEMA = "dax_translator_test"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Library (self-contained — no external package needed)

# COMMAND ----------

import json
import re
import textwrap
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole
from pydantic import BaseModel, Field


# ── Models ────────────────────────────────────────────────────────────────────


class DaxColumn(BaseModel):
    name: str
    data_type: str = "STRING"


class DaxTable(BaseModel):
    name: str
    columns: list[DaxColumn] = Field(default_factory=list)
    databricks_table: Optional[str] = None


class DaxRelationship(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str


class DaxMeasure(BaseModel):
    name: str
    expression: str
    description: Optional[str] = None


class DaxModel(BaseModel):
    name: str
    fact_table: str
    tables: list[DaxTable]
    relationships: list[DaxRelationship] = Field(default_factory=list)
    measures: list[DaxMeasure]
    catalog: str = "main"
    schema_name: str = "dax_translator_test"

    def get_table(self, name: str) -> Optional[DaxTable]:
        for t in self.tables:
            if t.name == name:
                return t
        return None


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

SYSTEM_PROMPT = r"""You are an expert at translating Power BI DAX measures into Databricks Metric View definitions.

# Your Task
Given a DAX model (tables, relationships, measures), produce a complete Databricks Metric View definition as a JSON object.

# Databricks Metric View YAML Reference

## Top-Level Fields
| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `version` | Yes | string | `"1.1"` for standard measures, `"0.1"` if window measures are used |
| `source` | Yes | string | Fully qualified source table (catalog.schema.table) |
| `comment` | No | string | Description (v1.1 only) |
| `dimensions` | Yes | list | Dimension definitions (at least one) |
| `measures` | Yes | list | Measure definitions (at least one) |
| `joins` | No | list | Star/snowflake join definitions |

## DAX-to-SQL Mapping Rules

| DAX | Metric View SQL |
|-----|----------------|
| `SUM(Table[Column])` | `SUM(column)` |
| `COUNT(Table[Column])` | `COUNT(column)` |
| `DISTINCTCOUNT(Table[Column])` | `COUNT(DISTINCT column)` |
| `AVERAGE(Table[Column])` | `AVG(column)` |
| `MIN/MAX(Table[Column])` | `MIN/MAX(column)` |
| `COUNTROWS(Table)` | `COUNT(1)` |
| `CALCULATE(agg, T[Col]="X")` | `agg FILTER (WHERE col = 'X')` |
| `CALCULATE(agg, T[A]="X", T[B]="Y")` | `agg FILTER (WHERE a = 'X' AND b = 'Y')` |
| `SUMX(T, T[A]*T[B])` | `SUM(a * b)` |
| `IF(cond, "A", "B")` as dimension | `CASE WHEN cond THEN 'A' ELSE 'B' END` |
| `RELATED(Dim[Col])` | `join_name.column` with join |
| `DIVIDE(num, den, 0)` | `num / NULLIF(den, 0)` |
| `TOTALYTD(SUM(T[Col]), Cal[Date])` | Window: cumulative + current year (version 0.1) |
| `SAMEPERIODLASTYEAR(...)` | Warning: time_intelligence + trailing 1 year approximation |
| `DATEADD(Cal[Date], -1, MONTH)` | Warning: time_intelligence + trailing 1 month approximation |
| `RANKX(...)` | Warning: unsupported_dax |

## Output JSON Schema
```json
{
  "status": "success" | "partial" | "failed",
  "version": "1.1" or "0.1",
  "source": "catalog.schema.fact_table",
  "comment": "optional",
  "joins": [{"name": "alias", "source": "catalog.schema.dim", "on": "source.fk = alias.pk"}],
  "dimensions": [{"name": "Name", "expr": "sql_expr", "comment": "optional"}],
  "measures": [{"name": "Name", "expr": "agg_expr", "comment": "optional", "window": [{"order": "dim", "range": "cumulative", "semiadditive": "last"}]}],
  "warnings": [{"measure_name": "n", "dax_expression": "dax", "warning_type": "type", "message": "msg", "approximation": null}]
}
```

## Critical Rules
1. Column names: snake_case. `Sales[OrderID]` → `order_id`.
2. No `source.` prefix for fact columns. Use `join_name.column` for dim columns.
3. Inline composite measures (don't use MEASURE() for non-window measures).
4. DIVIDE → `NULLIF`. FILTER clause: `FILTER (WHERE ...)` with space before `(`.
5. Use `"0.1"` only if any measure has `window`. Otherwise `"1.1"`.
6. Version 0.1 does NOT support `comment` fields — omit them.
7. Graceful degradation: unsupported DAX → warnings, not failures.
8. Return pure JSON only. No markdown, no code fences, no explanation.
9. **NEVER use SQL window functions**: Measure `expr` must NEVER contain `OVER(`, `PARTITION BY`, `ROWS BETWEEN`, or any SQL analytic/window function syntax. The only windowing allowed is through the YAML `window` block (order/range/semiadditive). If a DAX pattern requires SQL window functions, emit a warning with `warning_type: "unsupported_dax"` instead.
"""


def build_user_prompt(dax_model_json: str) -> str:
    return f"""Translate the following DAX model into a Databricks Metric View definition.
Return ONLY a JSON object matching the output schema. No markdown, no explanation.

DAX Model:
{dax_model_json}"""


# COMMAND ----------

# MAGIC %md
# MAGIC ## Translator Engine

# COMMAND ----------

MAX_RETRIES = 2


def _extract_json(text: str) -> dict:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()
    return json.loads(text)


def _build_yaml_body(result: TranslationResult) -> str:
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
    doc["dimensions"] = [
        {"name": d.name, "expr": d.expr} | ({"comment": d.comment} if d.comment and not v01 else {})
        for d in result.dimensions
    ]
    doc["measures"] = []
    for m in result.measures:
        md = {"name": m.name, "expr": m.expr}
        if m.comment and not v01:
            md["comment"] = m.comment
        if m.window:
            md["window"] = [{"order": w.order, "range": w.range, "semiadditive": w.semiadditive} for w in m.window]
        doc["measures"].append(md)
    return yaml.dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)


def _build_sql(result: TranslationResult, view_name: str) -> str:
    yaml_body = _build_yaml_body(result)
    indented = textwrap.indent(yaml_body.rstrip(), "  ")
    return f"CREATE OR REPLACE VIEW {view_name}\nWITH METRICS\nLANGUAGE YAML\nAS $$\n{indented}\n$$"


def _parse_response(raw: dict, dax_model: DaxModel) -> TranslationResult:
    status = TranslationStatus(raw.get("status", "success"))
    version = raw.get("version", "1.1")
    source = raw.get("source", "")
    if not source:
        fact = dax_model.get_table(dax_model.fact_table)
        source = fact.databricks_table if fact and fact.databricks_table else ""

    joins = [TranslatedJoin(name=j["name"], source=j["source"], on=j.get("on"), using=j.get("using"))
             for j in raw.get("joins", [])]
    dimensions = [TranslatedDimension(name=d["name"], expr=d["expr"], comment=d.get("comment"))
                  for d in raw.get("dimensions", [])]
    measures = []
    for m in raw.get("measures", []):
        window = None
        if m.get("window"):
            window = [WindowSpec(order=w["order"], range=w["range"], semiadditive=w.get("semiadditive", "last"))
                      for w in m["window"]]
        measures.append(TranslatedMeasure(name=m["name"], expr=m["expr"], comment=m.get("comment"), window=window))
    warnings = [MeasureWarning(**w) for w in raw.get("warnings", [])]

    if any(m.window for m in measures):
        version = "0.1"

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
    view_name = f"{dax_model.catalog}.{dax_model.schema_name}.mv_{dax_model.name}"
    result.yaml_body = _build_yaml_body(result)
    result.sql = _build_sql(result, view_name)
    return result


def translate(dax_model: DaxModel, endpoint: str = None) -> TranslationResult:
    """Translate a DAX model using FMAPI."""
    endpoint = endpoint or FMAPI_ENDPOINT
    w = WorkspaceClient()
    # Compatible with both Pydantic v1 (.json()) and v2 (.model_dump_json())
    if hasattr(dax_model, "model_dump_json"):
        dax_json = dax_model.model_dump_json(indent=2)
    else:
        dax_json = dax_model.json(indent=2)
    user_prompt = build_user_prompt(dax_json)

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
            return _parse_response(raw, dax_model)
        except json.JSONDecodeError as e:
            last_error = e
            if attempt < MAX_RETRIES:
                continue
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES:
                continue
    raise RuntimeError(f"Failed to translate after {MAX_RETRIES + 1} attempts: {last_error}")


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
# MAGIC ## Level 1: Simple Aggregations
# MAGIC `SUM`, `COUNT`, `DISTINCTCOUNT`, `AVERAGE`

# COMMAND ----------

level1 = DaxModel(
    name="level1_simple",
    fact_table="Sales",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    tables=[DaxTable(name="Sales", databricks_table=f"{fq}.fact_sales", columns=[
        DaxColumn(name="OrderID"), DaxColumn(name="CustomerID"), DaxColumn(name="Amount"),
        DaxColumn(name="Quantity"), DaxColumn(name="UnitPrice"), DaxColumn(name="Status"), DaxColumn(name="Region"),
    ])],
    measures=[
        DaxMeasure(name="Total Sales", expression="SUM(Sales[Amount])"),
        DaxMeasure(name="Order Count", expression="COUNT(Sales[OrderID])"),
        DaxMeasure(name="Unique Customers", expression="DISTINCTCOUNT(Sales[CustomerID])"),
        DaxMeasure(name="Avg Order", expression="AVERAGE(Sales[Amount])"),
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

mv_avg = query_measure(f"{fq}.mv_level1_simple", "Avg Order")
print(f"Avg Order: {mv_avg}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 2: CALCULATE, SUMX, Joins
# MAGIC Filtered measures, row-level calcs, dimension table joins

# COMMAND ----------

level2 = DaxModel(
    name="level2_medium",
    fact_table="Sales",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    tables=[
        DaxTable(name="Sales", databricks_table=f"{fq}.fact_sales", columns=[
            DaxColumn(name="OrderID"), DaxColumn(name="CustomerID"), DaxColumn(name="Amount"),
            DaxColumn(name="Quantity"), DaxColumn(name="UnitPrice"), DaxColumn(name="Status"), DaxColumn(name="Region"),
        ]),
        DaxTable(name="DimCustomer", databricks_table=f"{fq}.dim_customer", columns=[
            DaxColumn(name="CustomerID"), DaxColumn(name="Name"), DaxColumn(name="Segment"),
        ]),
    ],
    relationships=[DaxRelationship(from_table="Sales", from_column="CustomerID", to_table="DimCustomer", to_column="CustomerID")],
    measures=[
        DaxMeasure(name="Active Sales", expression='CALCULATE(SUM(Sales[Amount]), Sales[Status]="Active")'),
        DaxMeasure(name="Active West Sales", expression='CALCULATE(SUM(Sales[Amount]), Sales[Status]="Active", Sales[Region]="West")'),
        DaxMeasure(name="Line Total", expression="SUMX(Sales, Sales[Quantity]*Sales[UnitPrice])"),
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
sql_line = execute_sql(f"SELECT SUM(quantity * unit_price) AS v FROM {fq}.fact_sales")[0]["v"]
print(f"Line Total: MV={mv_line} vs SQL={sql_line} -> {'PASS' if float(mv_line) == float(sql_line) else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 3: Time Intelligence (Window Measures)
# MAGIC `TOTALYTD`, `SAMEPERIODLASTYEAR`, `DATEADD`, `RANKX`

# COMMAND ----------

level3 = DaxModel(
    name="level3_complex",
    fact_table="Sales",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    tables=[
        DaxTable(name="Sales", databricks_table=f"{fq}.fact_sales", columns=[
            DaxColumn(name="OrderID"), DaxColumn(name="Amount"), DaxColumn(name="DateKey"), DaxColumn(name="Status"),
        ]),
        DaxTable(name="Calendar", databricks_table=f"{fq}.dim_date", columns=[
            DaxColumn(name="Date"), DaxColumn(name="Year"), DaxColumn(name="Month"),
        ]),
    ],
    relationships=[DaxRelationship(from_table="Sales", from_column="DateKey", to_table="Calendar", to_column="Date")],
    measures=[
        DaxMeasure(name="YTD Sales", expression="TOTALYTD(SUM(Sales[Amount]), Calendar[Date])"),
        DaxMeasure(name="Sales Last Year", expression="CALCULATE(SUM(Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date]))"),
        DaxMeasure(name="Sales Rank", expression="RANKX(ALL(Sales), SUM(Sales[Amount]))"),
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
# MAGIC ## Level 4: Composite Measures, Multi-Table Star Schema
# MAGIC `DIVIDE`, composite `[Measure]` references, 3-way joins

# COMMAND ----------

level4 = DaxModel(
    name="level4_very_complex",
    fact_table="Sales",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    tables=[
        DaxTable(name="Sales", databricks_table=f"{fq}.fact_sales", columns=[
            DaxColumn(name="OrderID"), DaxColumn(name="CustomerID"), DaxColumn(name="ProductID"),
            DaxColumn(name="Amount"), DaxColumn(name="Profit"), DaxColumn(name="Revenue"),
            DaxColumn(name="Status"), DaxColumn(name="Tier"), DaxColumn(name="DateKey"),
        ]),
        DaxTable(name="DimCustomer", databricks_table=f"{fq}.dim_customer", columns=[
            DaxColumn(name="CustomerID"), DaxColumn(name="Name"), DaxColumn(name="Segment"),
        ]),
        DaxTable(name="DimProduct", databricks_table=f"{fq}.dim_product", columns=[
            DaxColumn(name="ProductID"), DaxColumn(name="ProductName"), DaxColumn(name="Category"),
        ]),
        DaxTable(name="Calendar", databricks_table=f"{fq}.dim_date", columns=[
            DaxColumn(name="Date"), DaxColumn(name="Year"),
        ]),
    ],
    relationships=[
        DaxRelationship(from_table="Sales", from_column="CustomerID", to_table="DimCustomer", to_column="CustomerID"),
        DaxRelationship(from_table="Sales", from_column="ProductID", to_table="DimProduct", to_column="ProductID"),
        DaxRelationship(from_table="Sales", from_column="DateKey", to_table="Calendar", to_column="Date"),
    ],
    measures=[
        DaxMeasure(name="Total Sales", expression="SUM(Sales[Amount])"),
        DaxMeasure(name="Sales per Customer", expression="[Total Sales] / DISTINCTCOUNT(Sales[CustomerID])"),
        DaxMeasure(name="Premium Sales", expression='CALCULATE([Total Sales], Sales[Status]="Active", Sales[Tier]="Premium")'),
        DaxMeasure(name="Profit Margin", expression="DIVIDE(SUM(Sales[Profit]), SUM(Sales[Revenue]), 0)"),
    ],
)

result4 = translate(level4)
print(f"Status: {result4.status.value} | Joins: {len(result4.joins)} | Dims: {len(result4.dimensions)} | Measures: {len(result4.measures)}")
deploy_metric_view(result4.sql)

# Validate
for name, sql_expr in [
    ("Total Sales", f"SELECT SUM(amount) FROM {fq}.fact_sales"),
    ("Profit Margin", f"SELECT SUM(profit)/NULLIF(SUM(revenue),0) FROM {fq}.fact_sales"),
    ("Premium Sales", f"SELECT SUM(amount) FROM {fq}.fact_sales WHERE status='Active' AND tier='Premium'"),
]:
    mv_val = query_measure(f"{fq}.mv_level4_very_complex", name)
    sql_val = list(execute_sql(sql_expr)[0].values())[0]
    match = abs(float(mv_val) - float(sql_val)) < 1
    print(f"  {name}: MV={mv_val} vs SQL={sql_val} -> {'PASS' if match else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 5: Advanced Business Logic
# MAGIC `ALL`, `ALLEXCEPT`, `VAR/RETURN`, running totals, weighted averages, YoY growth

# COMMAND ----------

level5 = DaxModel(
    name="level5_advanced",
    fact_table="Sales",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    tables=[
        DaxTable(name="Sales", databricks_table=f"{fq}.fact_sales", columns=[
            DaxColumn(name="OrderID"), DaxColumn(name="CustomerID"), DaxColumn(name="ProductID"),
            DaxColumn(name="DateKey"), DaxColumn(name="Amount"), DaxColumn(name="Quantity"),
            DaxColumn(name="UnitPrice"), DaxColumn(name="Profit"), DaxColumn(name="Revenue"),
            DaxColumn(name="Cost"), DaxColumn(name="Status"), DaxColumn(name="Region"), DaxColumn(name="Tier"),
        ]),
        DaxTable(name="DimCustomer", databricks_table=f"{fq}.dim_customer", columns=[
            DaxColumn(name="CustomerID"), DaxColumn(name="Name"), DaxColumn(name="Segment"),
        ]),
        DaxTable(name="DimProduct", databricks_table=f"{fq}.dim_product", columns=[
            DaxColumn(name="ProductID"), DaxColumn(name="ProductName"), DaxColumn(name="Category"),
        ]),
        DaxTable(name="Calendar", databricks_table=f"{fq}.dim_date", columns=[
            DaxColumn(name="Date"), DaxColumn(name="Year"), DaxColumn(name="Month"),
        ]),
    ],
    relationships=[
        DaxRelationship(from_table="Sales", from_column="CustomerID", to_table="DimCustomer", to_column="CustomerID"),
        DaxRelationship(from_table="Sales", from_column="ProductID", to_table="DimProduct", to_column="ProductID"),
        DaxRelationship(from_table="Sales", from_column="DateKey", to_table="Calendar", to_column="Date"),
    ],
    measures=[
        DaxMeasure(name="Pct of Grand Total", expression="DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALL(Sales)), 0)"),
        DaxMeasure(name="Pct of Category", expression="DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALLEXCEPT(Sales, DimProduct[Category])), 0)"),
        DaxMeasure(name="Premium Count", expression='CALCULATE(COUNTROWS(Sales), Sales[Amount] > 1000, Sales[Status]="Active")'),
        DaxMeasure(name="Net Margin", expression="VAR TotalRevenue = SUM(Sales[Revenue]) VAR TotalCost = SUM(Sales[Profit]) RETURN IF(TotalRevenue > 0, TotalCost / TotalRevenue, 0)"),
        DaxMeasure(name="Running Total", expression="CALCULATE(SUM(Sales[Amount]), FILTER(ALL(Calendar), Calendar[Date] <= MAX(Calendar[Date])))"),
        DaxMeasure(name="YoY Growth", expression="VAR CY = SUM(Sales[Amount]) VAR LY = CALCULATE(SUM(Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date])) RETURN DIVIDE(CY - LY, LY, 0)"),
        DaxMeasure(name="Active Customers", expression='CALCULATE(DISTINCTCOUNT(Sales[CustomerID]), Sales[Status]="Active")'),
        DaxMeasure(name="Weighted Avg Price", expression="DIVIDE(SUMX(Sales, Sales[UnitPrice] * Sales[Quantity]), SUM(Sales[Quantity]), 0)"),
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
    ("Premium Count", f"SELECT COUNT(*) FROM {fq}.fact_sales WHERE amount > 1000 AND status = 'Active'"),
    ("Active Customers", f"SELECT COUNT(DISTINCT customer_id) FROM {fq}.fact_sales WHERE status = 'Active'"),
    ("Weighted Avg Price", f"SELECT SUM(unit_price * quantity) / NULLIF(SUM(quantity), 0) FROM {fq}.fact_sales"),
]:
    if name in warned5:
        continue
    mv_val = float(query_measure(f"{fq}.mv_level5_advanced", name))
    sql_val = float(list(execute_sql(sql)[0].values())[0])
    print(f"  {name}: MV={mv_val} vs SQL={sql_val} -> {'PASS' if abs(mv_val - sql_val) < 0.01 else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Level 6: Expert DAX — Pushing the Limits
# MAGIC `LASTDATE`, `DATESINPERIOD`, `TOPN`, `ALLSELECTED`, `USERELATIONSHIP`, `RANKX`, `EARLIER`, `CONCATENATEX`
# MAGIC
# MAGIC Many of these should produce warnings — tests graceful degradation.

# COMMAND ----------

level6 = DaxModel(
    name="level6_expert",
    fact_table="Sales",
    catalog=TARGET_CATALOG,
    schema_name=TARGET_SCHEMA,
    tables=[
        DaxTable(name="Sales", databricks_table=f"{fq}.fact_sales", columns=[
            DaxColumn(name="OrderID"), DaxColumn(name="CustomerID"), DaxColumn(name="ProductID"),
            DaxColumn(name="DateKey"), DaxColumn(name="Amount"), DaxColumn(name="Quantity"),
            DaxColumn(name="UnitPrice"), DaxColumn(name="Profit"), DaxColumn(name="Revenue"),
            DaxColumn(name="Status"), DaxColumn(name="Region"), DaxColumn(name="Tier"),
        ]),
        DaxTable(name="DimCustomer", databricks_table=f"{fq}.dim_customer", columns=[
            DaxColumn(name="CustomerID"), DaxColumn(name="Name"), DaxColumn(name="Segment"),
        ]),
        DaxTable(name="DimProduct", databricks_table=f"{fq}.dim_product", columns=[
            DaxColumn(name="ProductID"), DaxColumn(name="ProductName"), DaxColumn(name="Category"),
        ]),
        DaxTable(name="Calendar", databricks_table=f"{fq}.dim_date", columns=[
            DaxColumn(name="Date"), DaxColumn(name="Year"), DaxColumn(name="Month"),
        ]),
    ],
    relationships=[
        DaxRelationship(from_table="Sales", from_column="CustomerID", to_table="DimCustomer", to_column="CustomerID"),
        DaxRelationship(from_table="Sales", from_column="ProductID", to_table="DimProduct", to_column="ProductID"),
        DaxRelationship(from_table="Sales", from_column="DateKey", to_table="Calendar", to_column="Date"),
    ],
    measures=[
        DaxMeasure(name="Closing Balance", expression="CALCULATE(SUM(Sales[Amount]), LASTDATE(Calendar[Date]))"),
        DaxMeasure(name="Rolling 7M Avg", expression="AVERAGEX(DATESINPERIOD(Calendar[Date], MAX(Calendar[Date]), -7, MONTH), CALCULATE(SUM(Sales[Amount])))"),
        DaxMeasure(name="New Customers", expression="COUNTROWS(FILTER(VALUES(Sales[CustomerID]), CALCULATE(MIN(Sales[DateKey])) >= MIN(Calendar[Date]) && CALCULATE(MIN(Sales[DateKey])) <= MAX(Calendar[Date])))"),
        DaxMeasure(name="Top 10 Revenue", expression="SUMX(TOPN(10, VALUES(Sales[CustomerID]), CALCULATE(SUM(Sales[Amount]))), CALCULATE(SUM(Sales[Amount])))"),
        DaxMeasure(name="Pct of Filtered Total", expression="DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALLSELECTED(Sales)), 0)"),
        DaxMeasure(name="Ship Date Sales", expression="CALCULATE(SUM(Sales[Amount]), USERELATIONSHIP(Sales[DateKey], Calendar[Date]))"),
        DaxMeasure(name="Product Revenue Rank", expression="RANKX(ALL(DimProduct), CALCULATE(SUM(Sales[Amount])))"),
        DaxMeasure(name="Cumulative Pct", expression="COUNTROWS(FILTER(Sales, EARLIER(Sales[Amount]) >= Sales[Amount])) / COUNTROWS(Sales)"),
        DaxMeasure(name="Region List", expression='CONCATENATEX(VALUES(Sales[Region]), Sales[Region], ", ")'),
        DaxMeasure(name="Category Contribution", expression="DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALL(DimProduct[ProductName]), ALL(DimProduct[ProductID])), 0)"),
    ],
)

result6 = translate(level6)
print(f"Status: {result6.status.value} | v{result6.version} | Measures: {len(result6.measures)} | Warnings: {len(result6.warnings)}")
print()
print("WARNINGS (expected for expert-level DAX):")
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
# MAGIC ## Custom DAX Model — Define Your Own
# MAGIC
# MAGIC Edit the model below with your own Power BI DAX measures and run translation.

# COMMAND ----------

# my_model = DaxModel(
#     name="my_custom_model",
#     fact_table="MyFact",
#     catalog="my_catalog",
#     schema_name="my_schema",
#     tables=[
#         DaxTable(name="MyFact", databricks_table="my_catalog.my_schema.my_fact_table", columns=[
#             DaxColumn(name="Amount"),
#             DaxColumn(name="CustomerID"),
#         ]),
#     ],
#     measures=[
#         DaxMeasure(name="Total Revenue", expression="SUM(MyFact[Amount])"),
#         DaxMeasure(name="Customer Count", expression="DISTINCTCOUNT(MyFact[CustomerID])"),
#     ],
# )
#
# my_result = translate(my_model)
# print(my_result.sql)
# deploy_metric_view(my_result.sql)
