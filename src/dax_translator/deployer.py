"""Databricks deployer: create test tables, deploy metric views, run queries."""

from __future__ import annotations

import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from dotenv import load_dotenv

load_dotenv()

DEFAULT_PROFILE = os.getenv("DATABRICKS_PROFILE", "DEFAULT")
DEFAULT_CATALOG = "main"
DEFAULT_SCHEMA = "dax_translator_test"


def get_client(profile: str | None = None) -> WorkspaceClient:
    """Get a Databricks WorkspaceClient using the specified profile."""
    profile = profile or os.getenv("DATABRICKS_PROFILE", DEFAULT_PROFILE)
    return WorkspaceClient(profile=profile)


def _get_warehouse_id(client: WorkspaceClient) -> str:
    """Find a running SQL warehouse to execute statements."""
    warehouses = list(client.warehouses.list())
    # Prefer a running serverless warehouse
    for wh in warehouses:
        if wh.state and wh.state.value == "RUNNING":
            return wh.id
    # Fall back to any warehouse
    if warehouses:
        return warehouses[0].id
    raise RuntimeError("No SQL warehouse found. Create one in the workspace first.")


def execute_sql(
    client: WorkspaceClient,
    sql: str,
    warehouse_id: str | None = None,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> list[dict]:
    """Execute a SQL statement and return results as list of dicts."""
    warehouse_id = warehouse_id or _get_warehouse_id(client)

    response = client.statement_execution.execute_statement(
        statement=sql,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        wait_timeout="50s",
    )

    # Poll if still running (wait_timeout max is 50s, statement may need longer)
    while response.status and response.status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        time.sleep(2)
        response = client.statement_execution.get_statement(response.statement_id)

    if response.status and response.status.state == StatementState.FAILED:
        error = response.status.error
        raise RuntimeError(f"SQL execution failed: {error}")

    # Parse results
    if not response.result or not response.result.data_array:
        return []

    columns = [col.name for col in response.manifest.schema.columns]
    rows = []
    for row_data in response.result.data_array:
        rows.append(dict(zip(columns, row_data)))
    return rows


def create_test_schema(
    client: WorkspaceClient,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    warehouse_id: str | None = None,
) -> None:
    """Create the test catalog and schema if they don't exist."""
    wid = warehouse_id or _get_warehouse_id(client)
    execute_sql(client, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}", wid, catalog)


def create_test_data(
    client: WorkspaceClient,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    warehouse_id: str | None = None,
) -> None:
    """Create test tables with sample data for all 4 levels of test cases."""
    wid = warehouse_id or _get_warehouse_id(client)
    fq = f"{catalog}.{schema}"

    # Create schema
    create_test_schema(client, catalog, schema, wid)

    # ── dim_date: every day 2023-01-01 to 2025-12-31 ──
    execute_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {fq}.dim_date AS
        SELECT
            date AS date,
            YEAR(date) AS year,
            MONTH(date) AS month,
            CONCAT('Q', QUARTER(date)) AS quarter,
            DATE_TRUNC('MONTH', date) AS month_start
        FROM (
            SELECT EXPLODE(SEQUENCE(DATE'2023-01-01', DATE'2025-12-31', INTERVAL 1 DAY)) AS date
        )
        """,
        wid,
        catalog,
    )

    # ── dim_customer: ~15 rows ──
    execute_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {fq}.dim_customer AS
        SELECT * FROM VALUES
            (1, 'Acme Corp', 'Enterprise', 'Australia'),
            (2, 'Beta Inc', 'Mid-Market', 'Australia'),
            (3, 'Gamma Ltd', 'SMB', 'New Zealand'),
            (4, 'Delta Co', 'Enterprise', 'Australia'),
            (5, 'Epsilon Pty', 'Mid-Market', 'Australia'),
            (6, 'Zeta Group', 'Enterprise', 'Singapore'),
            (7, 'Eta Systems', 'SMB', 'Australia'),
            (8, 'Theta Data', 'Enterprise', 'Australia'),
            (9, 'Iota Labs', 'Mid-Market', 'New Zealand'),
            (10, 'Kappa Tech', 'SMB', 'Australia'),
            (11, 'Lambda AI', 'Enterprise', 'Singapore'),
            (12, 'Mu Analytics', 'Mid-Market', 'Australia'),
            (13, 'Nu Cloud', 'SMB', 'Australia'),
            (14, 'Xi Solutions', 'Enterprise', 'New Zealand'),
            (15, 'Omicron Digital', 'Mid-Market', 'Australia')
        AS t(customer_id, name, segment, country)
        """,
        wid,
        catalog,
    )

    # ── dim_product: ~10 rows ──
    execute_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {fq}.dim_product AS
        SELECT * FROM VALUES
            (1, 'Widget A', 'Hardware'),
            (2, 'Widget B', 'Hardware'),
            (3, 'Service X', 'Services'),
            (4, 'Service Y', 'Services'),
            (5, 'License Pro', 'Software'),
            (6, 'License Std', 'Software'),
            (7, 'Addon Z', 'Addons'),
            (8, 'Platform 1', 'Platform'),
            (9, 'Platform 2', 'Platform'),
            (10, 'Custom Sol', 'Services')
        AS t(product_id, product_name, category)
        """,
        wid,
        catalog,
    )

    # ── fact_sales: ~100 rows spanning 2023-2025 ──
    execute_sql(
        client,
        f"""
        CREATE OR REPLACE TABLE {fq}.fact_sales AS
        WITH base AS (
            SELECT EXPLODE(SEQUENCE(1, 100)) AS order_id
        )
        SELECT
            order_id,
            (order_id % 15) + 1 AS customer_id,
            (order_id % 10) + 1 AS product_id,
            DATE_ADD(DATE'2023-01-15', (order_id * 7) % 1050) AS date_key,
            ROUND(100 + (order_id * 37 % 4900), 2) AS amount,
            (order_id % 20) + 1 AS quantity,
            ROUND(10 + (order_id * 13 % 490), 2) AS unit_price,
            ROUND((100 + (order_id * 37 % 4900)) * 0.15, 2) AS profit,
            ROUND((100 + (order_id * 37 % 4900)) * 1.0, 2) AS revenue,
            CASE WHEN order_id % 3 = 0 THEN 'Inactive' ELSE 'Active' END AS status,
            CASE WHEN order_id % 4 = 0 THEN 'West'
                 WHEN order_id % 4 = 1 THEN 'East'
                 WHEN order_id % 4 = 2 THEN 'North'
                 ELSE 'South' END AS region,
            CASE WHEN order_id % 5 = 0 THEN 'Premium'
                 WHEN order_id % 5 = 1 THEN 'Gold'
                 ELSE 'Standard' END AS tier
        FROM base
        """,
        wid,
        catalog,
    )

    print(f"Test data created in {fq}")


def deploy_metric_view(
    client: WorkspaceClient,
    sql: str,
    warehouse_id: str | None = None,
    catalog: str = DEFAULT_CATALOG,
) -> None:
    """Deploy a metric view by executing the CREATE VIEW statement."""
    wid = warehouse_id or _get_warehouse_id(client)
    execute_sql(client, sql, wid, catalog)


def query_metric_view(
    client: WorkspaceClient,
    view_name: str,
    measures: list[str],
    dimensions: list[str] | None = None,
    where: str | None = None,
    warehouse_id: str | None = None,
    catalog: str = DEFAULT_CATALOG,
) -> list[dict]:
    """Query a metric view using MEASURE() syntax."""
    wid = warehouse_id or _get_warehouse_id(client)

    measure_cols = ", ".join(f"MEASURE(`{m}`) AS `{m}`" for m in measures)

    if dimensions:
        dim_cols = ", ".join(f"`{d}`" for d in dimensions)
        select = f"SELECT {dim_cols}, {measure_cols}"
    else:
        select = f"SELECT {measure_cols}"

    sql = f"{select}\nFROM {view_name}"
    if where:
        sql += f"\nWHERE {where}"
    sql += "\nGROUP BY ALL"

    return execute_sql(client, sql, wid, catalog)


def run_direct_sql(
    client: WorkspaceClient,
    sql: str,
    warehouse_id: str | None = None,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
) -> list[dict]:
    """Run a direct SQL query against source tables for validation comparison."""
    wid = warehouse_id or _get_warehouse_id(client)
    return execute_sql(client, sql, wid, catalog, schema)


def cleanup_test_views(
    client: WorkspaceClient,
    catalog: str = DEFAULT_CATALOG,
    schema: str = DEFAULT_SCHEMA,
    warehouse_id: str | None = None,
) -> None:
    """Drop all test metric views."""
    wid = warehouse_id or _get_warehouse_id(client)
    fq = f"{catalog}.{schema}"
    for level in ["level1_simple", "level2_medium", "level3_complex", "level4_very_complex"]:
        try:
            execute_sql(client, f"DROP VIEW IF EXISTS {fq}.mv_{level}", wid, catalog)
        except Exception:
            pass
