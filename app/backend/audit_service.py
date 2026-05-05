"""Audit trail — logs translations to a Delta table using OBO (user's token).

Uses the same REST API approach as deployer_service to avoid SP storage permission issues.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

from .config import DATABRICKS_WAREHOUSE_ID, get_config

logger = logging.getLogger(__name__)


def _get_host() -> str:
    host = os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    return host.rstrip("/")


def _exec_sql(token: str, sql: str, timeout: str = "30s") -> dict:
    """Execute SQL via Statement Execution REST API using the user's OBO token."""
    host = _get_host()
    body = json.dumps({
        "warehouse_id": DATABRICKS_WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": timeout,
        "disposition": "INLINE",
    }).encode()
    req = urllib.request.Request(
        f"{host}/api/2.0/sql/statements",
        data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read())
        # Poll if pending
        status = result.get("status", {})
        stmt_id = result.get("statement_id", "")
        while status.get("state") in ("PENDING", "RUNNING"):
            time.sleep(1)
            poll_req = urllib.request.Request(
                f"{host}/api/2.0/sql/statements/{stmt_id}",
                headers={"Authorization": f"Bearer {token}"},
            )
            with urllib.request.urlopen(poll_req) as poll_resp:
                result = json.loads(poll_resp.read())
            status = result.get("status", {})
        return result
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if e.fp else str(e)
        raise RuntimeError(f"SQL API error {e.code}: {error_body}") from e


_TABLE_DDL = """CREATE TABLE IF NOT EXISTS {fq} (
    id BIGINT GENERATED ALWAYS AS IDENTITY,
    timestamp TIMESTAMP,
    mode STRING,
    model STRING,
    source_table STRING,
    dimension_tables STRING,
    measures_input STRING,
    status STRING,
    version STRING,
    measure_count INT,
    dimension_count INT,
    warning_count INT,
    warnings STRING,
    yaml_body STRING,
    sql_output STRING,
    deployed_view STRING,
    deploy_status STRING,
    error_message STRING,
    user_name STRING
) USING DELTA"""

_table_verified = False


def _ensure_audit_table(token: str) -> str:
    """Ensure the audit table exists. Creates schema and table if needed."""
    global _table_verified
    cfg = get_config()
    catalog = cfg['audit_catalog']
    schema = cfg['audit_schema']
    fq = f"{catalog}.{schema}.{cfg['audit_table']}"

    if _table_verified:
        return fq

    # Step 1: Ensure the schema exists
    try:
        _exec_sql(token, f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    except Exception as schema_err:
        logger.info(f"CREATE SCHEMA skipped (may already exist or no permission): {schema_err}")

    # Step 2: Check if table already exists first
    try:
        result = _exec_sql(token, f"DESCRIBE TABLE {fq}")
        if result.get("status", {}).get("state") == "SUCCEEDED":
            _table_verified = True
            # Migrate: add columns if missing
            for col in ("deploy_status", "error_message"):
                try:
                    _exec_sql(token, f"ALTER TABLE {fq} ADD COLUMN IF NOT EXISTS {col} STRING")
                except Exception:
                    pass
            return fq
    except Exception:
        pass

    # Step 3: Table doesn't exist — create it
    try:
        _exec_sql(token, _TABLE_DDL.format(fq=fq))
        _table_verified = True
        return fq
    except Exception as create_err:
        raise RuntimeError(
            f"Audit table {fq} does not exist. Auto-create failed: {create_err}\n\n"
            f"Run this in a Databricks notebook:\n"
            f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};\n{_TABLE_DDL.format(fq=fq)}"
        ) from create_err


def _sql_str(s: str) -> str:
    """Encode string as base64, decode in SQL — safe for any content."""
    encoded = base64.b64encode(s.encode("utf-8")).decode("ascii")
    return f"cast(unbase64('{encoded}') as STRING)"


def log_translation(
    token: str,
    mode: str,
    model: str,
    source_table: str,
    dimension_tables: list[dict] | None,
    measures_input: str,
    status: str,
    version: str = "",
    measure_count: int = 0,
    dimension_count: int = 0,
    warning_count: int = 0,
    warnings: list[dict] | None = None,
    yaml_body: str = "",
    sql_output: str = "",
    error_message: str = "",
    user_name: str = "",
) -> None:
    """Log a translation (success or failure) to the audit Delta table."""
    if not token:
        logger.warning("No OBO token for audit logging — skipping")
        return
    try:
        fq = _ensure_audit_table(token)
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        dim_str = json.dumps(dimension_tables or [])
        warn_str = json.dumps([
            {"name": w_["measure_name"], "type": w_["warning_type"], "msg": w_["message"]}
            for w_ in (warnings or [])
        ])

        sql = f"""
        INSERT INTO {fq} (timestamp, mode, model, source_table, dimension_tables,
            measures_input, status, version, measure_count, dimension_count,
            warning_count, warnings, yaml_body, sql_output, deployed_view,
            deploy_status, error_message, user_name)
        VALUES (
            '{now}',
            {_sql_str(mode)},
            {_sql_str(model)},
            {_sql_str(source_table)},
            {_sql_str(dim_str)},
            {_sql_str(measures_input[:4000])},
            {_sql_str(status)},
            {_sql_str(version)},
            {measure_count},
            {dimension_count},
            {warning_count},
            {_sql_str(warn_str[:4000])},
            {_sql_str(yaml_body[:8000])},
            {_sql_str(sql_output[:8000])},
            '',
            '',
            {_sql_str(error_message[:4000])},
            {_sql_str(user_name)}
        )
        """
        _exec_sql(token, sql, timeout="15s")
    except Exception as e:
        logger.warning(f"Audit logging failed (non-fatal): {e}")


def log_deploy(token: str, view_name: str, success: bool, message: str, user_name: str = "") -> None:
    """Update the last audit record with deploy result."""
    if not token:
        return
    try:
        fq = _ensure_audit_table(token)
        deploy_status = "success" if success else "failed"
        sql = f"""
        UPDATE {fq}
        SET deployed_view = {_sql_str(view_name)},
            deploy_status = {_sql_str(deploy_status)},
            error_message = CASE WHEN error_message = '' THEN {_sql_str(message if not success else '')} ELSE error_message END,
            user_name = {_sql_str(user_name)}
        WHERE id = (SELECT MAX(id) FROM {fq})
        """
        _exec_sql(token, sql, timeout="15s")
    except Exception as e:
        logger.warning(f"Audit deploy log failed (non-fatal): {e}")


def query_audit_history(token: str) -> dict:
    """Return recent translation history."""
    if not token:
        return {"rows": [], "error": "No auth token"}
    try:
        fq = _ensure_audit_table(token)
        result = _exec_sql(token, f"SELECT id, timestamp, mode, model, source_table, status, version, measure_count, dimension_count, warning_count, deployed_view, deploy_status, error_message, user_name, measures_input, yaml_body, sql_output, warnings FROM {fq} ORDER BY id DESC LIMIT 50")
        state = result.get("status", {}).get("state", "")
        if state != "SUCCEEDED":
            err = result.get("status", {}).get("error", {})
            return {"rows": [], "error": err.get("message", str(err)) if isinstance(err, dict) else str(err)}
        columns = [col["name"] for col in result.get("manifest", {}).get("schema", {}).get("columns", [])]
        rows = [dict(zip(columns, row)) for row in result.get("result", {}).get("data_array", [])]
        return {"rows": rows}
    except Exception as e:
        return {"rows": [], "error": str(e)}
