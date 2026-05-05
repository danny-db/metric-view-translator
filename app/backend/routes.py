"""API route definitions."""

from __future__ import annotations

import threading

from fastapi import APIRouter, HTTPException, Request

from .audit_service import log_deploy, log_translation, query_audit_history
from .config import (
    DATABRICKS_WAREHOUSE_ID,
    SERVING_ENDPOINT_NAME,
    get_config,
    update_config,
)
from .deployer_service import deploy_metric_view, get_current_user, query_metric_view
from .models import (
    AppConfig,
    DeployRequest,
    DeployResponse,
    HealthResponse,
    QueryRequest,
    QueryResponse,
    TranslateDaxRequest,
    TranslateMdxRequest,
    TranslateResponse,
    TranslateTextRequest,
    UserInfo,
)
from .translator_service import translate_dax, translate_mdx, translate_text

router = APIRouter(prefix="/api")


def _get_user_token(request: Request) -> str:
    token = request.headers.get("x-forwarded-access-token", "")
    if not token:
        raise HTTPException(status_code=401, detail="Missing user token (x-forwarded-access-token)")
    return token


# ── Health & User ────────────────────────────────────────────────────────────


@router.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="healthy",
        warehouse_configured=bool(DATABRICKS_WAREHOUSE_ID),
        endpoint_configured=bool(SERVING_ENDPOINT_NAME),
    )


@router.get("/me", response_model=UserInfo)
async def me(request: Request):
    token = _get_user_token(request)
    user = get_current_user(token)
    return UserInfo(**user)


# ── Config ───────────────────────────────────────────────────────────────────


@router.get("/config", response_model=AppConfig)
async def get_app_config():
    return AppConfig(**get_config())


@router.put("/config", response_model=AppConfig)
async def update_app_config(body: AppConfig):
    updated = update_config(body.model_dump(exclude_unset=True))
    return AppConfig(**updated)


# ── Translation ──────────────────────────────────────────────────────────────


def _to_response(result) -> TranslateResponse:
    return TranslateResponse(
        status=result.status.value,
        version=result.version,
        source=result.source,
        yaml_body=result.yaml_body,
        sql=result.sql,
        dimensions=result.dimensions,
        measures=result.measures,
        joins=result.joins,
        warnings=result.warnings,
    )


@router.post("/translate/dax", response_model=TranslateResponse)
async def translate_dax_endpoint(body: TranslateDaxRequest):
    try:
        result = translate_dax(body.model)
        return _to_response(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/translate/mdx", response_model=TranslateResponse)
async def translate_mdx_endpoint(body: TranslateMdxRequest):
    try:
        result = translate_mdx(body.cube)
        return _to_response(result)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/translate/text", response_model=TranslateResponse)
async def translate_text_endpoint(body: TranslateTextRequest, request: Request):
    """Simple text mode — paste DAX/MDX measures as text."""
    token = request.headers.get("x-forwarded-access-token", "")
    user_name = ""
    try:
        if token:
            u = get_current_user(token)
            user_name = u.get("username", "")
    except Exception:
        pass

    dim_dicts = [d.model_dump() for d in body.dimension_tables] if body.dimension_tables else None

    try:
        parts = body.source_table.split(".")
        catalog = parts[0] if len(parts) >= 3 else "main"
        schema_name = parts[1] if len(parts) >= 3 else "default"

        result = translate_text(
            measures_text=body.measures_text,
            source_table=body.source_table,
            mode=body.mode,
            catalog=catalog,
            schema_name=schema_name,
            dimension_tables=dim_dicts,
            model=body.model,
        )
        resp = _to_response(result)

        # Audit success (OBO)
        threading.Thread(target=log_translation, kwargs={
            "token": token,
            "mode": body.mode,
            "model": body.model or SERVING_ENDPOINT_NAME,
            "source_table": body.source_table,
            "dimension_tables": dim_dicts,
            "measures_input": body.measures_text,
            "status": resp.status,
            "version": resp.version,
            "measure_count": len(resp.measures),
            "dimension_count": len(resp.dimensions),
            "warning_count": len(resp.warnings),
            "warnings": [w.model_dump() for w in resp.warnings],
            "yaml_body": resp.yaml_body,
            "sql_output": resp.sql,
            "user_name": user_name,
        }, daemon=True).start()

        return resp
    except Exception as e:
        # Audit failure (OBO)
        threading.Thread(target=log_translation, kwargs={
            "token": token,
            "mode": body.mode,
            "model": body.model or SERVING_ENDPOINT_NAME,
            "source_table": body.source_table,
            "dimension_tables": dim_dicts,
            "measures_input": body.measures_text,
            "status": "failed",
            "error_message": str(e),
            "user_name": user_name,
        }, daemon=True).start()
        raise HTTPException(status_code=500, detail=str(e))


# ── Deploy & Query ───────────────────────────────────────────────────────────


@router.post("/deploy", response_model=DeployResponse)
async def deploy(body: DeployRequest, request: Request):
    token = _get_user_token(request)
    full_view_name = f"{body.catalog}.{body.schema_name}.{body.view_name}"
    result = deploy_metric_view(token, body.sql)

    # Audit deploy result (OBO)
    user_name = ""
    try:
        u = get_current_user(token)
        user_name = u.get("username", "")
    except Exception:
        pass
    threading.Thread(target=log_deploy, kwargs={
        "token": token,
        "view_name": full_view_name,
        "success": result["success"],
        "message": result["message"],
        "user_name": user_name,
    }, daemon=True).start()

    return DeployResponse(
        success=result["success"],
        message=result["message"],
        full_view_name=full_view_name if result["success"] else "",
    )


@router.post("/query", response_model=QueryResponse)
async def query(body: QueryRequest, request: Request):
    token = _get_user_token(request)
    result = query_metric_view(
        user_token=token,
        catalog=body.catalog,
        schema_name=body.schema_name,
        view_name=body.view_name,
        measures=body.measures,
        group_by=body.group_by if body.group_by else None,
        limit=body.limit,
    )
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return QueryResponse(
        columns=result["columns"],
        rows=result["rows"],
        row_count=result["row_count"],
    )


# ── Audit History ────────────────────────────────────────────────────────────


@router.get("/audit")
async def get_audit_history(request: Request):
    """Return recent translation history from the audit table (OBO)."""
    token = request.headers.get("x-forwarded-access-token", "")
    return query_audit_history(token)
