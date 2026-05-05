"""OBO-authenticated SQL execution for deploying and querying metric views.

Uses the Databricks Statement Execution REST API directly with the user's
forwarded OAuth token, avoiding both WorkspaceClient (SP env var conflict)
and databricks-sql-connector (connection issues).
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
import urllib.error

from .config import DATABRICKS_WAREHOUSE_ID


def _get_host() -> str:
    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def _api_request(user_token: str, path: str, body: dict | None = None) -> dict:
    """Make an authenticated REST API call."""
    host = _get_host()
    url = f"{host}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Authorization": f"Bearer {user_token}",
            "Content-Type": "application/json",
        },
        method="POST" if data else "GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else str(e)
        raise RuntimeError(f"API error {e.code}: {error_body}") from e


def _execute_sql(user_token: str, sql: str) -> dict:
    """Execute SQL via the Statement Execution API and wait for completion."""
    result = _api_request(user_token, "/api/2.0/sql/statements", {
        "warehouse_id": DATABRICKS_WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "50s",
        "disposition": "INLINE",
    })

    # Poll if still pending
    status = result.get("status", {})
    statement_id = result.get("statement_id", "")
    while status.get("state") in ("PENDING", "RUNNING"):
        time.sleep(2)
        result = _api_request(user_token, f"/api/2.0/sql/statements/{statement_id}")
        status = result.get("status", {})

    return result


def get_current_user(user_token: str) -> dict:
    """Get the current user's identity."""
    data = _api_request(user_token, "/api/2.0/preview/scim/v2/Me")
    return {
        "username": data.get("userName", ""),
        "display_name": data.get("displayName", data.get("userName", "")),
    }


def deploy_metric_view(user_token: str, sql_statement: str) -> dict:
    """Execute a CREATE OR REPLACE VIEW ... WITH METRICS statement as the user."""
    try:
        result = _execute_sql(user_token, sql_statement)
        state = result.get("status", {}).get("state", "")
        if state == "SUCCEEDED":
            return {"success": True, "message": "Metric view deployed successfully"}
        error = result.get("status", {}).get("error", {})
        msg = error.get("message", "") if isinstance(error, dict) else str(error)
        return {"success": False, "message": f"Deploy failed: {msg}"}
    except Exception as e:
        return {"success": False, "message": f"Deploy error: {str(e)}"}


def query_metric_view(
    user_token: str,
    catalog: str,
    schema_name: str,
    view_name: str,
    measures: list[str],
    group_by: list[str] | None = None,
    limit: int = 100,
) -> dict:
    """Query a deployed metric view using MEASURE() syntax."""
    full_name = f"{catalog}.{schema_name}.{view_name}"
    measure_exprs = ", ".join(f'MEASURE(`{m}`)' for m in measures)

    if group_by:
        group_cols = ", ".join(f'`{g}`' for g in group_by)
        sql = f"SELECT {group_cols}, {measure_exprs} FROM {full_name} GROUP BY {group_cols} LIMIT {limit}"
    else:
        sql = f"SELECT {measure_exprs} FROM {full_name} LIMIT {limit}"

    try:
        result = _execute_sql(user_token, sql)
        state = result.get("status", {}).get("state", "")
        if state != "SUCCEEDED":
            error = result.get("status", {}).get("error", {})
            msg = error.get("message", "") if isinstance(error, dict) else str(error)
            return {"columns": [], "rows": [], "row_count": 0, "error": msg}

        # Parse result
        manifest = result.get("manifest", {})
        schema = manifest.get("schema", {})
        columns = [col["name"] for col in schema.get("columns", [])]
        data_array = result.get("result", {}).get("data_array", [])
        rows = [dict(zip(columns, row)) for row in data_array]

        return {"columns": columns, "rows": rows, "row_count": len(rows)}
    except Exception as e:
        return {"columns": [], "rows": [], "row_count": 0, "error": str(e)}
