"""Environment configuration for the Metric View Translator app."""

import os

DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
SERVING_ENDPOINT_NAME = os.getenv("SERVING_ENDPOINT_NAME", "databricks-claude-sonnet-4-6")
MAX_RETRIES = 2

# Audit trail defaults — overridable at runtime via /api/config
DEFAULT_AUDIT_CATALOG = os.getenv("AUDIT_CATALOG", "main")
DEFAULT_AUDIT_SCHEMA = os.getenv("AUDIT_SCHEMA", "metric_view_translator")
DEFAULT_AUDIT_TABLE = os.getenv("AUDIT_TABLE", "translation_history")

# Runtime config (mutable at runtime via API)
_runtime_config: dict = {}


def get_config() -> dict:
    return {
        "audit_catalog": _runtime_config.get("audit_catalog", DEFAULT_AUDIT_CATALOG),
        "audit_schema": _runtime_config.get("audit_schema", DEFAULT_AUDIT_SCHEMA),
        "audit_table": _runtime_config.get("audit_table", DEFAULT_AUDIT_TABLE),
        "serving_endpoint": _runtime_config.get("serving_endpoint", SERVING_ENDPOINT_NAME),
        "dax_prompt_suffix": _runtime_config.get("dax_prompt_suffix", ""),
        "mdx_prompt_suffix": _runtime_config.get("mdx_prompt_suffix", ""),
    }


def update_config(updates: dict) -> dict:
    allowed = {"audit_catalog", "audit_schema", "audit_table", "serving_endpoint", "dax_prompt_suffix", "mdx_prompt_suffix"}
    for k, v in updates.items():
        if k in allowed:
            _runtime_config[k] = v
    return get_config()
