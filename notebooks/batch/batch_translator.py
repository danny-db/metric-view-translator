# Databricks notebook source
# MAGIC %md
# MAGIC # Batch DAX/MDX → Metric View Translator
# MAGIC
# MAGIC Translates exported Power BI (BIM, CSV) or SSAS (MDX Script) measure definitions
# MAGIC into Databricks Metric Views at scale using `ai_query()` for parallel LLM inference.
# MAGIC
# MAGIC ## Supported Formats
# MAGIC | Format | Source Tool | Notes |
# MAGIC |--------|-----------|-------|
# MAGIC | **Model.bim** (JSON) | Tabular Editor / PBIX extract | Full model: tables, columns, relationships, measures |
# MAGIC | **DAX Studio CSV** | DAX Studio DMV query | Measures only — requires `sample_mapping.yaml` for schema context |
# MAGIC | **MDX Script** (.mdx) | SSMS / Cube Designer | Calculated members with `CREATE MEMBER` blocks |
# MAGIC
# MAGIC ## How It Works
# MAGIC 1. Parse source file → staging table (one row per measure)
# MAGIC 2. Resolve composite measure dependencies (topological sort + inline expansion)
# MAGIC 3. `ai_query()` translates all measures in parallel via Claude
# MAGIC 4. Post-process: OVER() check, group by source table, build YAML/SQL
# MAGIC 5. Deploy metric views + log to audit table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Configuration

# COMMAND ----------

dbutils.widgets.text("source_file", "/Volumes/main/dax_translator_test/batch_samples/sample_powerbi_export.bim")
dbutils.widgets.dropdown("source_format", "auto", ["auto", "bim", "csv", "mdx_script"])
dbutils.widgets.text("mapping_file", "", "Mapping YAML (required for CSV)")
dbutils.widgets.text("target_catalog", "main")
dbutils.widgets.text("target_schema", "dax_translator_test")
dbutils.widgets.text("serving_endpoint", "databricks-claude-sonnet-4-6")
dbutils.widgets.dropdown("auto_deploy", "false", ["true", "false"])

# COMMAND ----------

SOURCE_FILE = dbutils.widgets.get("source_file")
SOURCE_FORMAT = dbutils.widgets.get("source_format")
MAPPING_FILE = dbutils.widgets.get("mapping_file")
TARGET_CATALOG = dbutils.widgets.get("target_catalog")
TARGET_SCHEMA = dbutils.widgets.get("target_schema")
SERVING_ENDPOINT = dbutils.widgets.get("serving_endpoint")
AUTO_DEPLOY = dbutils.widgets.get("auto_deploy") == "true"

print(f"Source:    {SOURCE_FILE}")
print(f"Format:    {SOURCE_FORMAT}")
print(f"Mapping:   {MAPPING_FILE or '(none — BIM has schema embedded)'}")
print(f"Target:    {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"Endpoint:  {SERVING_ENDPOINT}")
print(f"Deploy:    {AUTO_DEPLOY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Format Parsers → Staging Table

# COMMAND ----------

import csv
import io
import json
import re
import textwrap
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import yaml

BATCH_ID = str(uuid.uuid4())[:12]
STAGING_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.batch_staging"
RAW_TABLE = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.batch_raw"
AUDIT_TABLE = f"main.metric_view_translator.translation_history"

print(f"Batch ID: {BATCH_ID}")

# COMMAND ----------

def _detect_format(path: str) -> str:
    """Auto-detect source format from file extension."""
    lower = path.lower()
    if lower.endswith(".bim"):
        return "bim"
    elif lower.endswith(".csv"):
        return "csv"
    elif lower.endswith(".mdx"):
        return "mdx_script"
    raise ValueError(f"Cannot auto-detect format for: {path}. Set source_format explicitly.")


def _read_file(path: str) -> str:
    """Read a file from Volumes or DBFS."""
    # Try direct file read (works for /Volumes and /dbfs paths)
    try:
        with open(path.replace("dbfs:", "/dbfs"), "r") as f:
            return f.read()
    except FileNotFoundError:
        pass
    # Fallback: dbutils
    return dbutils.fs.head(path, maxBytes=10_000_000)


def _snake_case(name: str) -> str:
    """Convert PascalCase/camelCase to snake_case."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower().replace(" ", "_")

# COMMAND ----------

# MAGIC %md
# MAGIC ### BIM Parser

# COMMAND ----------

def parse_bim(content: str, catalog: str, schema: str) -> list[dict]:
    """Parse a Power BI Model.bim (Tabular Model JSON) into staging rows."""
    bim = json.loads(content)
    model = bim.get("model", bim)  # Handle both wrapped and unwrapped
    tables_raw = model.get("tables", [])

    # Build table metadata: columns, databricks FQN
    table_meta = {}  # DAX table name -> {fqn, columns: [snake_case names], col_map: {DAX -> snake}}
    fact_table_name = None
    for t in tables_raw:
        tname = t["name"]
        cols = t.get("columns", [])
        col_map = {}
        for c in cols:
            dax_name = c["name"]
            # Prefer sourceColumn if present (actual DB column), else snake_case the DAX name
            sc = c.get("sourceColumn", _snake_case(dax_name))
            col_map[dax_name] = sc

        # Infer Databricks FQN from partition M expression or name convention
        fqn = _infer_fqn_from_bim(t, catalog, schema)
        table_meta[tname] = {"fqn": fqn, "columns": list(col_map.values()), "col_map": col_map}

        # First table with measures is the fact table
        if t.get("measures") and not fact_table_name:
            fact_table_name = tname

    # Build relationship map
    relationships = model.get("relationships", [])

    # Build dimension table info for the fact table
    dim_tables = []
    for rel in relationships:
        from_t = rel.get("fromTable")
        to_t = rel.get("toTable")
        from_col = rel.get("fromColumn")
        to_col = rel.get("toColumn")
        # In Power BI, "from" is typically the many-side (fact), "to" is the one-side (dim)
        if from_t == fact_table_name and to_t in table_meta:
            dim_meta = table_meta[to_t]
            from_sc = table_meta[from_t]["col_map"].get(from_col, _snake_case(from_col))
            to_sc = dim_meta["col_map"].get(to_col, _snake_case(to_col))
            dim_tables.append({
                "dax_name": to_t,
                "fqn": dim_meta["fqn"],
                "join_key": from_sc,
                "dim_key": to_sc,
                "columns": dim_meta["columns"],
            })

    fact_meta = table_meta.get(fact_table_name, {})
    fact_fqn = fact_meta.get("fqn", f"{catalog}.{schema}.fact_sales")
    fact_cols = fact_meta.get("columns", [])
    dim_json = json.dumps([
        {"table": d["fqn"], "join_key": d["join_key"], "dim_key": d["dim_key"], "columns": d["columns"]}
        for d in dim_tables
    ])

    # Extract measures from all tables (usually just the fact table)
    rows = []
    for t in tables_raw:
        for m in t.get("measures", []):
            rows.append({
                "batch_id": BATCH_ID,
                "source_format": "bim",
                "source_table": t["name"],
                "measure_name": m["name"],
                "measure_expr": m["expression"],
                "description": m.get("description", ""),
                "mode": "dax",
                "fact_table_fqn": fact_fqn,
                "fact_columns_json": json.dumps(fact_cols),
                "dim_tables_json": dim_json,
                "resolved_expr": None,
                "final_status": "pending",
                "translation_json": None,
                "error_message": None,
                "warnings_json": None,
                "needs_window": False,
            })
    return rows


def _infer_fqn_from_bim(table_def: dict, catalog: str, schema: str) -> str:
    """Try to extract Databricks FQN from the BIM partition M expression."""
    for p in table_def.get("partitions", []):
        src = p.get("source", {})
        expr = src.get("expression", "")
        # Look for patterns like Item="fact_sales" or Item="dim_customer"
        match = re.search(r'Item="(\w+)"', expr)
        if match:
            return f"{catalog}.{schema}.{match.group(1)}"
    # Fallback: snake_case the table name
    return f"{catalog}.{schema}.{_snake_case(table_def['name'])}"

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV Parser (DAX Studio export)

# COMMAND ----------

def parse_csv(content: str, mapping: dict) -> list[dict]:
    """Parse a DAX Studio DMV CSV export into staging rows."""
    reader = csv.DictReader(io.StringIO(content))

    fact_fqn = mapping["fact_table"]
    dim_tables = mapping.get("dimension_tables", [])
    dim_json = json.dumps(dim_tables)

    # We don't have column info from CSV — will be enriched in Section 3 via DESCRIBE
    rows = []
    for r in reader:
        rows.append({
            "batch_id": BATCH_ID,
            "source_format": "csv",
            "source_table": r.get("TableName", "Unknown"),
            "measure_name": r.get("MeasureName", r.get("Name", "")),
            "measure_expr": r.get("Expression", ""),
            "description": r.get("Description", ""),
            "mode": "dax",
            "fact_table_fqn": fact_fqn,
            "fact_columns_json": "[]",  # Will be enriched
            "dim_tables_json": dim_json,
            "resolved_expr": None,
            "final_status": "pending",
            "translation_json": None,
            "error_message": None,
            "warnings_json": None,
            "needs_window": False,
        })
    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ### MDX Script Parser

# COMMAND ----------

def parse_mdx_script(content: str, mapping: dict) -> list[dict]:
    """Parse an MDX Script file into staging rows (one per CREATE MEMBER)."""
    fact_fqn = mapping["fact_table"]
    dim_tables = mapping.get("dimension_tables", [])
    dim_json = json.dumps(dim_tables)

    members = []
    # Split on CREATE MEMBER and extract name + expression
    blocks = re.split(r"(?i)CREATE\s+MEMBER\s+CURRENTCUBE\.", content)
    for block in blocks[1:]:  # Skip preamble before first CREATE MEMBER
        block = block.strip()
        # Extract [Measures].[Name]
        name_match = re.match(r"\[Measures\]\.\[([^\]]+)\]", block)
        if not name_match:
            continue
        name = name_match.group(1)

        # Expression is between AS and the next FORMAT_STRING, VISIBLE, or end of block
        remainder = block[name_match.end():].strip()
        # Remove leading "AS" (case insensitive)
        remainder = re.sub(r"^(?:AS\s+)", "", remainder, flags=re.IGNORECASE).strip()

        # Find expression end: terminated by comma + FORMAT_STRING/VISIBLE/NON_EMPTY_BEHAVIOR or semicolon
        expr_match = re.split(
            r",\s*(?:FORMAT_STRING|VISIBLE|NON_EMPTY_BEHAVIOR|ASSOCIATED_MEASURE_GROUP)\s*=",
            remainder,
            maxsplit=1,
            flags=re.IGNORECASE,
        )
        expression = expr_match[0].rstrip(",; \n\r\t")

        members.append({
            "batch_id": BATCH_ID,
            "source_format": "mdx_script",
            "source_table": "Cube",
            "measure_name": name,
            "measure_expr": expression,
            "description": "",
            "mode": "mdx",
            "fact_table_fqn": fact_fqn,
            "fact_columns_json": "[]",  # Will be enriched
            "dim_tables_json": dim_json,
            "resolved_expr": None,
            "final_status": "pending",
            "translation_json": None,
            "error_message": None,
            "warnings_json": None,
            "needs_window": False,
        })
    return members

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse Source File

# COMMAND ----------

# Detect format
fmt = SOURCE_FORMAT if SOURCE_FORMAT != "auto" else _detect_format(SOURCE_FILE)
print(f"Detected format: {fmt}")

# Read source file
source_content = _read_file(SOURCE_FILE)
print(f"Read {len(source_content)} bytes from {SOURCE_FILE}")

# Read mapping file if needed
mapping = {}
if fmt in ("csv", "mdx_script"):
    if not MAPPING_FILE:
        raise ValueError(f"Format '{fmt}' requires a mapping_file widget to be set (YAML with fact_table + dimension_tables).")
    mapping_content = _read_file(MAPPING_FILE)
    mapping = yaml.safe_load(mapping_content)
    print(f"Loaded mapping: fact_table={mapping['fact_table']}, {len(mapping.get('dimension_tables', []))} dimension tables")

# Parse
if fmt == "bim":
    staging_rows = parse_bim(source_content, TARGET_CATALOG, TARGET_SCHEMA)
elif fmt == "csv":
    staging_rows = parse_csv(source_content, mapping)
elif fmt == "mdx_script":
    staging_rows = parse_mdx_script(source_content, mapping)
else:
    raise ValueError(f"Unsupported format: {fmt}")

print(f"Parsed {len(staging_rows)} measures")
for r in staging_rows:
    print(f"  - {r['measure_name']}: {r['measure_expr'][:80]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Enrich Context

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enrich column metadata via DESCRIBE TABLE (for CSV/MDX that lack schema info)

# COMMAND ----------

def _describe_table(fqn: str) -> list[str]:
    """Get column names for a table via DESCRIBE TABLE."""
    try:
        rows = spark.sql(f"DESCRIBE TABLE {fqn}").collect()
        return [r["col_name"] for r in rows if r["col_name"] and not r["col_name"].startswith("#")]
    except Exception as e:
        print(f"  WARNING: DESCRIBE {fqn} failed: {e}")
        return []


# Enrich fact table columns if empty
if staging_rows and staging_rows[0]["fact_columns_json"] == "[]":
    fact_fqn = staging_rows[0]["fact_table_fqn"]
    fact_cols = _describe_table(fact_fqn)
    if fact_cols:
        fact_cols_json = json.dumps(fact_cols)
        for r in staging_rows:
            r["fact_columns_json"] = fact_cols_json
        print(f"Enriched fact columns from {fact_fqn}: {fact_cols}")

    # Also enrich dimension table columns
    if staging_rows[0]["dim_tables_json"] != "[]":
        dim_tables = json.loads(staging_rows[0]["dim_tables_json"])
        enriched = False
        for dt in dim_tables:
            if "columns" not in dt or not dt["columns"]:
                dim_cols = _describe_table(dt["table"])
                if dim_cols:
                    dt["columns"] = dim_cols
                    enriched = True
        if enriched:
            dim_json = json.dumps(dim_tables)
            for r in staging_rows:
                r["dim_tables_json"] = dim_json
            print(f"Enriched dimension table columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Composite Measure Dependency Resolution
# MAGIC
# MAGIC DAX measures can reference other measures: `[Total Sales] / DISTINCTCOUNT(...)`.
# MAGIC Since `ai_query()` processes each row independently, we pre-resolve these references
# MAGIC by expanding them inline via topological sort.

# COMMAND ----------

def resolve_composites(rows: list[dict], max_depth: int = 10) -> list[dict]:
    """
    Resolve composite measure references by inlining referenced expressions.

    For DAX: [Measure Name] references → expand to the referenced measure's expression.
    For MDX: [Measures].[Name] references → expand similarly.

    Uses topological sort to handle dependency chains.
    """
    # Build lookup: measure_name -> expression
    expr_lookup = {r["measure_name"]: r["measure_expr"] for r in rows}
    mode = rows[0]["mode"] if rows else "dax"

    # Build dependency graph
    deps = defaultdict(set)  # measure -> set of measures it references
    for name, expr in expr_lookup.items():
        refs = _find_measure_refs(expr, set(expr_lookup.keys()), mode)
        deps[name] = refs

    # Detect circular references
    circular = _detect_circular(deps)
    if circular:
        print(f"  WARNING: Circular references detected: {circular}")
        for name in circular:
            for r in rows:
                if r["measure_name"] == name:
                    r["error_message"] = f"Circular measure reference: {name}"
                    r["final_status"] = "error"

    # Topological sort
    order = _topological_sort(deps)

    # Expand in dependency order
    resolved = dict(expr_lookup)  # Start with original expressions
    for name in order:
        if name in circular:
            continue
        expr = resolved[name]
        for depth in range(max_depth):
            refs = _find_measure_refs(expr, set(expr_lookup.keys()), mode)
            if not refs:
                break
            for ref in refs:
                ref_expr = resolved.get(ref, "")
                if not ref_expr:
                    continue
                expr = _replace_measure_ref(expr, ref, ref_expr, mode)
            resolved[name] = expr

    # Update rows with resolved expressions
    for r in rows:
        name = r["measure_name"]
        if name in resolved and resolved[name] != r["measure_expr"]:
            r["resolved_expr"] = resolved[name]
            print(f"  Resolved [{name}]: {r['measure_expr'][:60]} → {resolved[name][:60]}")

    return rows


def _find_measure_refs(expr: str, known_measures: set, mode: str) -> set:
    """Find references to other measures in an expression."""
    refs = set()
    if mode == "dax":
        # Match [MeasureName] but not Table[Column] (which has a preceding word char)
        for m in re.finditer(r"(?<!\w)\[([^\]]+)\]", expr):
            name = m.group(1)
            if name in known_measures:
                refs.add(name)
    else:  # mdx
        for m in re.finditer(r"\[Measures\]\.\[([^\]]+)\]", expr):
            name = m.group(1)
            if name in known_measures:
                refs.add(name)
    return refs


def _replace_measure_ref(expr: str, ref_name: str, ref_expr: str, mode: str) -> str:
    """Replace a measure reference with its expression."""
    if mode == "dax":
        # Replace [MeasureName] (not preceded by a word char — avoids Table[Col])
        pattern = r"(?<!\w)\[" + re.escape(ref_name) + r"\]"
        replacement = f"({ref_expr})"
        return re.sub(pattern, replacement, expr)
    else:  # mdx
        pattern = r"\[Measures\]\.\[" + re.escape(ref_name) + r"\]"
        replacement = f"({ref_expr})"
        return re.sub(pattern, replacement, expr)


def _detect_circular(deps: dict) -> set:
    """Detect circular dependencies using DFS."""
    WHITE, GRAY, BLACK = 0, 1, 2
    color = defaultdict(int)
    circular = set()

    def dfs(node):
        color[node] = GRAY
        for dep in deps.get(node, set()):
            if color[dep] == GRAY:
                circular.add(node)
                circular.add(dep)
            elif color[dep] == WHITE:
                dfs(dep)
        color[node] = BLACK

    for node in deps:
        if color[node] == WHITE:
            dfs(node)
    return circular


def _topological_sort(deps: dict) -> list:
    """Topological sort — base measures first, composite measures last."""
    visited = set()
    order = []

    def visit(node):
        if node in visited:
            return
        visited.add(node)
        for dep in deps.get(node, set()):
            visit(dep)
        order.append(node)

    for node in deps:
        visit(node)
    return order

# COMMAND ----------

print("Resolving composite measure dependencies...")
staging_rows = resolve_composites(staging_rows)
print(f"Resolution complete. {sum(1 for r in staging_rows if r.get('resolved_expr'))} measures expanded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Staging Table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, BooleanType

staging_schema = StructType([
    StructField("batch_id", StringType()),
    StructField("source_format", StringType()),
    StructField("source_table", StringType()),
    StructField("measure_name", StringType()),
    StructField("measure_expr", StringType()),
    StructField("description", StringType()),
    StructField("mode", StringType()),
    StructField("fact_table_fqn", StringType()),
    StructField("fact_columns_json", StringType()),
    StructField("dim_tables_json", StringType()),
    StructField("resolved_expr", StringType()),
    StructField("final_status", StringType()),
    StructField("translation_json", StringType()),
    StructField("error_message", StringType()),
    StructField("warnings_json", StringType()),
    StructField("needs_window", BooleanType()),
])

staging_df = spark.createDataFrame(staging_rows, schema=staging_schema)
staging_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(STAGING_TABLE)
print(f"Wrote {staging_df.count()} rows to {STAGING_TABLE}")
display(staging_df.select("measure_name", "measure_expr", "resolved_expr", "final_status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: ai_query() Translation
# MAGIC
# MAGIC Each row is translated independently via `ai_query()` with `failOnError => false`.
# MAGIC Databricks auto-parallelizes across rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build System Prompt (batch-optimized, ~2K tokens)

# COMMAND ----------

# Trimmed system prompt for single-measure-at-a-time batch translation.
# Covers both DAX and MDX (mode is passed per-row).
BATCH_SYSTEM_PROMPT = r"""You are an expert at translating Power BI DAX measures and SSAS MDX calculated members into Databricks Metric View expressions.

# Your Task
Given ONE measure/calculated member with its source table and column context, return a JSON translation.

# Metric View Rules
- Measure `expr` must contain an aggregate: SUM, COUNT, AVG, MIN, MAX
- FILTER (WHERE ...) for conditional aggregation (space before parenthesis)
- NULLIF(denominator, 0) for safe division (DAX DIVIDE equivalent)
- SUMX(T, T[A]*T[B]) → SUM(a * b)
- COUNTROWS(T) → COUNT(1)
- DISTINCTCOUNT(T[Col]) → COUNT(DISTINCT col)
- IIF/IF → CASE WHEN ... THEN ... ELSE ... END
- Column naming: always snake_case (OrderID → order_id, Amount → amount)
- Reference fact columns directly (no prefix). Reference dim columns as join_alias.column.
- Inline composite measures: if expression contains other measure references already expanded, translate the full expression.
- TOTALYTD → window measure with needs_window=true
- RANKX, USERELATIONSHIP, ALLSELECTED → unsupported, emit warning
- SAMEPERIODLASTYEAR, DATEADD, ParallelPeriod → time_intelligence warning
- MDX RANK, TopCount, Generate, .Parent, .PrevMember → unsupported warning
- NEVER use OVER(), PARTITION BY, or ROWS BETWEEN in expr

# Joins
- For dimension table references, include join definitions
- Join format: {"name": "alias", "source": "catalog.schema.dim", "on": "source.fk = alias.pk"}

# Output JSON Schema
Return ONLY this JSON (no markdown):
{
  "expr": "aggregate SQL expression",
  "needs_window": false,
  "window": null,
  "warning_type": null,
  "warning_message": null,
  "dimensions_needed": ["dim_name:dim_expr", ...],
  "joins_needed": [{"name": "alias", "source": "fqn", "on": "join_condition"}]
}

For window measures (TOTALYTD etc):
{
  "expr": "SUM(amount)",
  "needs_window": true,
  "window": [{"order": "date_dim", "range": "cumulative", "semiadditive": "last"},
              {"order": "year_dim", "range": "current", "semiadditive": "last"}],
  "warning_type": null,
  "warning_message": null,
  "dimensions_needed": ["Date:calendar.date", "Year:calendar.year"],
  "joins_needed": [{"name": "calendar", "source": "cat.sch.dim_date", "on": "source.date_key = calendar.date"}]
}

For unsupported patterns:
{
  "expr": null,
  "needs_window": false,
  "window": null,
  "warning_type": "unsupported_dax",
  "warning_message": "RANKX has no Metric View equivalent",
  "dimensions_needed": [],
  "joins_needed": []
}"""

# Escape single quotes for SQL embedding
BATCH_PROMPT_SQL = BATCH_SYSTEM_PROMPT.replace("'", "\\'")

print(f"System prompt: {len(BATCH_SYSTEM_PROMPT)} chars")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run ai_query()

# COMMAND ----------

# Build and execute the ai_query() SQL
ai_query_sql = f"""
CREATE OR REPLACE TABLE {RAW_TABLE} AS
SELECT *, ai_query(
    '{SERVING_ENDPOINT}',
    CONCAT(
        '{BATCH_PROMPT_SQL}',
        '\\n\\nMode: ', mode,
        '\\nSource table: ', fact_table_fqn,
        '\\nFact columns: ', fact_columns_json,
        '\\nDimension tables: ', COALESCE(dim_tables_json, '[]'),
        '\\n\\nMeasure name: ', measure_name,
        '\\nExpression: ', COALESCE(resolved_expr, measure_expr),
        '\\n\\nReturn ONLY the JSON object. No markdown, no explanation.'
    ),
    modelParameters => named_struct('max_tokens', 1024, 'temperature', 0.0),
    failOnError => false
) AS ai_result
FROM {STAGING_TABLE}
WHERE batch_id = '{BATCH_ID}'
  AND final_status = 'pending'
"""

print(f"Running ai_query() on {sum(1 for r in staging_rows if r.get('final_status') == 'pending')} measures...")
print(f"Endpoint: {SERVING_ENDPOINT}")

spark.sql(ai_query_sql)
print("ai_query() complete!")

# COMMAND ----------

# Show raw results
raw_df = spark.table(RAW_TABLE)
display(raw_df.select("measure_name", "ai_result"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Post-Process
# MAGIC
# MAGIC - Parse ai_query() JSON results
# MAGIC - Check for OVER() in expressions (safety net)
# MAGIC - Group measures by source table → one metric view per table
# MAGIC - Build YAML + SQL for each view

# COMMAND ----------

def _extract_json_safe(text: str) -> dict | None:
    """Extract JSON from ai_query result, handling error structs and markdown fences."""
    if not text:
        return None
    text = text.strip()
    # ai_query failOnError=false returns error as a struct string
    if text.startswith("QUERY_EXECUTION_ERROR") or text.startswith("Error"):
        return None
    # Strip markdown fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*\n?", "", text)
        text = re.sub(r"\n?```\s*$", "", text)
        text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def post_process(raw_rows: list) -> dict:
    """
    Post-process ai_query results into metric view definitions.

    Returns: {
        source_table_fqn: {
            "version": "1.1" or "0.1",
            "source": fqn,
            "joins": [...],
            "dimensions": [...],
            "measures": [...],
            "warnings": [...],
            "measure_details": [...]
        }
    }
    """
    views = defaultdict(lambda: {
        "version": "1.1",
        "source": "",
        "joins": [],
        "dimensions": [],
        "measures": [],
        "warnings": [],
        "measure_details": [],
    })

    # Track joins/dims by name to avoid duplicates
    join_names = defaultdict(set)
    dim_names = defaultdict(set)

    for row in raw_rows:
        name = row["measure_name"]
        fact_fqn = row["fact_table_fqn"]
        ai_text = row["ai_result"]
        original_expr = row["measure_expr"]

        view = views[fact_fqn]
        view["source"] = fact_fqn

        result = _extract_json_safe(ai_text)
        if result is None:
            view["warnings"].append({
                "measure_name": name,
                "dax_expression": original_expr,
                "warning_type": "translation_error",
                "message": f"ai_query() failed or returned unparseable result: {(ai_text or '')[:200]}",
                "approximation": None,
            })
            view["measure_details"].append({"name": name, "status": "failed", "error": ai_text})
            continue

        expr = result.get("expr")
        warning_type = result.get("warning_type")
        warning_msg = result.get("warning_message")
        needs_window = result.get("needs_window", False)
        window_spec = result.get("window")

        # Handle warnings/unsupported
        if warning_type:
            view["warnings"].append({
                "measure_name": name,
                "dax_expression": original_expr,
                "warning_type": warning_type,
                "message": warning_msg or "",
                "approximation": expr,
            })
            if not expr:
                view["measure_details"].append({"name": name, "status": "warning", "warning_type": warning_type})
                continue

        # OVER() safety net
        if expr and re.search(r'\bOVER\s*\(', expr, re.IGNORECASE):
            view["warnings"].append({
                "measure_name": name,
                "dax_expression": original_expr,
                "warning_type": "unsupported_dax",
                "message": "Measure uses SQL window function (OVER), stripped from output.",
                "approximation": None,
            })
            view["measure_details"].append({"name": name, "status": "warning", "warning_type": "over_clause"})
            continue

        # Add joins (deduplicate by name)
        for j in result.get("joins_needed", []):
            jname = j.get("name", "")
            if jname and jname not in join_names[fact_fqn]:
                join_names[fact_fqn].add(jname)
                view["joins"].append(j)

        # Add dimensions (deduplicate by name)
        for d_str in result.get("dimensions_needed", []):
            if ":" in d_str:
                dname, dexpr = d_str.split(":", 1)
            else:
                dname = d_str
                dexpr = d_str
            if dname not in dim_names[fact_fqn]:
                dim_names[fact_fqn].add(dname)
                view["dimensions"].append({"name": dname, "expr": dexpr.strip()})

        # Add measure
        measure_def = {"name": name, "expr": expr}
        if needs_window and window_spec:
            view["version"] = "0.1"
            measure_def["window"] = window_spec

        view["measures"].append(measure_def)
        view["measure_details"].append({
            "name": name,
            "status": "success" if not warning_type else "partial",
            "expr": expr,
            "needs_window": needs_window,
        })

    return dict(views)


# Collect raw results
raw_results = raw_df.collect()
raw_dicts = [row.asDict() for row in raw_results]

views = post_process(raw_dicts)
print(f"\nGrouped into {len(views)} metric view(s):")
for fqn, v in views.items():
    success = sum(1 for d in v["measure_details"] if d["status"] == "success")
    warn = sum(1 for d in v["measure_details"] if d["status"] in ("warning", "partial"))
    fail = sum(1 for d in v["measure_details"] if d["status"] == "failed")
    print(f"  {fqn}: {len(v['measures'])} measures ({success} ok, {warn} warn, {fail} fail), {len(v['joins'])} joins, {len(v['dimensions'])} dims")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build YAML + SQL for Each View

# COMMAND ----------

def build_yaml_body(view_def: dict) -> str:
    """Build the YAML body for a metric view definition."""
    doc = {}
    v01 = view_def["version"] == "0.1"
    doc["version"] = view_def["version"]
    doc["source"] = view_def["source"]

    if view_def["joins"]:
        doc["joins"] = []
        for j in view_def["joins"]:
            jd = {"name": j["name"], "source": j["source"]}
            if j.get("on"):
                jd["on"] = j["on"]
            if j.get("using"):
                jd["using"] = j["using"]
            doc["joins"].append(jd)

    # Ensure at least one dimension — add a default if none
    dims = view_def["dimensions"]
    if not dims:
        # Add a sensible default dimension from the fact table
        dims = [{"name": "status", "expr": "status"}]

    doc["dimensions"] = [{"name": d["name"], "expr": d["expr"]} for d in dims]

    doc["measures"] = []
    for m in view_def["measures"]:
        md = {"name": m["name"], "expr": m["expr"]}
        if not v01 and m.get("comment"):
            md["comment"] = m["comment"]
        if m.get("window"):
            md["window"] = m["window"]
        doc["measures"].append(md)

    return yaml.dump(doc, default_flow_style=False, sort_keys=False, allow_unicode=True)


def build_sql(view_def: dict, view_name: str) -> str:
    """Build the CREATE OR REPLACE VIEW SQL for a metric view."""
    yaml_body = build_yaml_body(view_def)
    indented = textwrap.indent(yaml_body.rstrip(), "  ")
    return (
        f"CREATE OR REPLACE VIEW {view_name}\n"
        f"WITH METRICS\n"
        f"LANGUAGE YAML\n"
        f"AS $$\n"
        f"{indented}\n"
        f"$$"
    )


# Generate SQL for each view
view_outputs = {}
for fact_fqn, view_def in views.items():
    if not view_def["measures"]:
        print(f"  Skipping {fact_fqn} — no translatable measures")
        continue

    # Derive view name from fact table: fact_sales → mv_batch_fact_sales
    table_name = fact_fqn.split(".")[-1]
    view_name = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.mv_batch_{table_name}"

    sql = build_sql(view_def, view_name)
    yaml_body = build_yaml_body(view_def)
    view_outputs[view_name] = {"sql": sql, "yaml": yaml_body, "def": view_def}
    print(f"\n{'='*80}")
    print(f"View: {view_name}")
    print(f"{'='*80}")
    print(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Deploy + Audit

# COMMAND ----------

import base64

def _sql_str(s: str) -> str:
    """Encode string as base64, decode in SQL — safe for any content."""
    encoded = base64.b64encode(s.encode("utf-8")).decode("ascii")
    return f"cast(unbase64('{encoded}') as STRING)"


deploy_results = []

for view_name, output in view_outputs.items():
    view_def = output["def"]
    sql = output["sql"]
    yaml_body = output["yaml"]

    if AUTO_DEPLOY:
        print(f"\nDeploying {view_name}...")
        try:
            spark.sql(sql)
            print(f"  ✓ Deployed successfully")
            deploy_status = "success"
            deploy_error = ""
        except Exception as e:
            print(f"  ✗ Deploy failed: {e}")
            deploy_status = "failed"
            deploy_error = str(e)[:1000]
    else:
        print(f"\nSkipping deploy for {view_name} (auto_deploy=false)")
        print("  Set auto_deploy=true to deploy, or run the SQL manually above.")
        deploy_status = "skipped"
        deploy_error = ""

    # Compute stats
    success_count = sum(1 for d in view_def["measure_details"] if d["status"] == "success")
    warning_count = len(view_def["warnings"])
    fail_count = sum(1 for d in view_def["measure_details"] if d["status"] == "failed")
    total = success_count + warning_count + fail_count

    overall_status = "success" if warning_count == 0 and fail_count == 0 else "partial" if success_count > 0 else "failed"

    deploy_results.append({
        "view_name": view_name,
        "measures_total": total,
        "measures_success": success_count,
        "measures_warning": warning_count,
        "measures_failed": fail_count,
        "deploy_status": deploy_status,
        "deploy_error": deploy_error,
    })

    # Log to audit table
    try:
        warnings_json = json.dumps([
            {"name": w["measure_name"], "type": w["warning_type"], "msg": w["message"]}
            for w in view_def["warnings"]
        ])
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        mode = "batch_" + fmt  # e.g. "batch_bim", "batch_csv", "batch_mdx_script"

        # Collect all original measure expressions as input
        measures_input = "\n".join(
            f"{d['name']}: {next((r['measure_expr'] for r in raw_dicts if r['measure_name'] == d['name']), '')}"
            for d in view_def["measure_details"]
        )

        dim_tables_str = json.dumps([
            {"table": j["source"], "join_key": j.get("on", "")}
            for j in view_def["joins"]
        ])

        audit_sql = f"""
        INSERT INTO {AUDIT_TABLE}
            (timestamp, mode, model, source_table, dimension_tables,
             measures_input, status, version, measure_count, dimension_count,
             warning_count, warnings, yaml_body, sql_output,
             deployed_view, deploy_status, error_message, user_name)
        VALUES (
            '{now}',
            {_sql_str(mode)},
            {_sql_str(SERVING_ENDPOINT)},
            {_sql_str(view_def['source'])},
            {_sql_str(dim_tables_str[:4000])},
            {_sql_str(measures_input[:4000])},
            {_sql_str(overall_status)},
            {_sql_str(view_def['version'])},
            {len(view_def['measures'])},
            {len(view_def['dimensions'])},
            {warning_count},
            {_sql_str(warnings_json[:4000])},
            {_sql_str(yaml_body[:8000])},
            {_sql_str(sql[:8000])},
            {_sql_str(view_name if deploy_status == 'success' else '')},
            {_sql_str(deploy_status)},
            {_sql_str(deploy_error[:4000])},
            {_sql_str(spark.sql("SELECT current_user()").first()[0])}
        )
        """
        spark.sql(audit_sql)
        print(f"  Logged to {AUDIT_TABLE}")
    except Exception as e:
        print(f"  WARNING: Audit logging failed (non-fatal): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Summary Report

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deployment Summary

# COMMAND ----------

summary_data = []
for dr in deploy_results:
    summary_data.append(dr)

summary_df = spark.createDataFrame(summary_data)
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Per-Measure Results

# COMMAND ----------

detail_rows = []
for fact_fqn, view_def in views.items():
    for d in view_def["measure_details"]:
        detail_rows.append({
            "source_table": fact_fqn,
            "measure_name": d["name"],
            "status": d["status"],
            "expr": d.get("expr", ""),
            "needs_window": d.get("needs_window", False),
            "warning_type": d.get("warning_type", ""),
        })

if detail_rows:
    detail_df = spark.createDataFrame(detail_rows)
    display(detail_df)
else:
    print("No measures processed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Warnings

# COMMAND ----------

all_warnings = []
for fact_fqn, view_def in views.items():
    for w in view_def["warnings"]:
        all_warnings.append({
            "source_table": fact_fqn,
            "measure_name": w["measure_name"],
            "warning_type": w["warning_type"],
            "message": w["message"],
            "approximation": w.get("approximation", ""),
        })

if all_warnings:
    warnings_df = spark.createDataFrame(all_warnings)
    display(warnings_df)
else:
    print("No warnings — all measures translated successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validation Queries
# MAGIC
# MAGIC Run these against deployed views to verify translations.

# COMMAND ----------

if AUTO_DEPLOY:
    for view_name, output in view_outputs.items():
        view_def = output["def"]
        # Build a simple validation query
        measure_names = [m["name"] for m in view_def["measures"][:5]]  # First 5
        measure_select = ", ".join(f'MEASURE(`{m}`)' for m in measure_names)
        if view_def["dimensions"]:
            first_dim = view_def["dimensions"][0]["name"]
            query = f"SELECT `{first_dim}`, {measure_select}\nFROM {view_name}\nGROUP BY ALL\nLIMIT 10"
        else:
            query = f"SELECT {measure_select}\nFROM {view_name}\nGROUP BY ALL\nLIMIT 10"

        print(f"\n-- Validation: {view_name}")
        print(query)
        try:
            result_df = spark.sql(query)
            display(result_df)
        except Exception as e:
            print(f"  Query failed: {e}")
else:
    print("Auto-deploy is disabled. Enable it to run validation queries.")
    print("\nTo deploy manually, copy the CREATE VIEW SQL from Section 5 above and run it.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch Complete
# MAGIC
# MAGIC **Batch ID:** `{BATCH_ID}`
# MAGIC
# MAGIC Check `main.metric_view_translator.translation_history` for the audit trail.
# MAGIC
# MAGIC Staging data is in `{TARGET_CATALOG}.{TARGET_SCHEMA}.batch_staging` (batch_id = `{BATCH_ID}`).

# COMMAND ----------

print(f"""
Batch Translation Complete
==========================
Batch ID:    {BATCH_ID}
Source:      {SOURCE_FILE}
Format:      {fmt}
Measures:    {len(staging_rows)} parsed
Views:       {len(view_outputs)} generated
Deploy:      {'enabled' if AUTO_DEPLOY else 'disabled'}

Audit table: {AUDIT_TABLE}
Staging:     {STAGING_TABLE} (batch_id='{BATCH_ID}')
""")
