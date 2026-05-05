"""Integration tests: deploy metric views to a Databricks workspace and validate."""

import os

import pytest

from dax_translator.deployer import (
    cleanup_test_views,
    create_test_data,
    deploy_metric_view,
    execute_sql,
    get_client,
    query_metric_view,
    run_direct_sql,
)
from dax_translator.translator import translate
from dax_translator.validator import validate_all

PROFILE = os.getenv("DATABRICKS_PROFILE", "DEFAULT")
CATALOG = "main"
SCHEMA = "dax_translator_test"


@pytest.fixture(scope="module")
def client():
    return get_client(PROFILE)


@pytest.fixture(scope="module")
def setup_test_data(client):
    """Create test data once for all integration tests."""
    create_test_data(client, CATALOG, SCHEMA)
    yield
    cleanup_test_views(client, CATALOG, SCHEMA)


# ── Level 1: Simple Aggregations ──────────────────────────────────────────────


@pytest.mark.integration
def test_level1_translate_and_deploy(client, setup_test_data):
    from dax_translator.dax_samples.level1_simple import EXPECTED, get_model

    model = get_model()
    result = translate(model)

    # Validate structure
    assert result.source == f"{CATALOG}.{SCHEMA}.fact_sales"
    assert len(result.dimensions) >= 1
    assert len(result.measures) >= 4

    # Deploy
    deploy_metric_view(client, result.sql)

    # Validate each measure
    fq_view = f"{CATALOG}.{SCHEMA}.mv_level1_simple"
    for measure_name in EXPECTED:
        mv_rows = query_metric_view(client, fq_view, measures=[measure_name])
        assert len(mv_rows) > 0, f"No results for MEASURE({measure_name})"

    # Cross-validate against direct SQL
    total_mv = query_metric_view(client, fq_view, measures=["Total Sales"])
    total_direct = run_direct_sql(
        client, f"SELECT SUM(amount) AS total FROM {CATALOG}.{SCHEMA}.fact_sales"
    )
    mv_val = float(total_mv[0]["Total Sales"])
    direct_val = float(total_direct[0]["total"])
    assert mv_val == pytest.approx(direct_val, rel=0.01)


# ── Level 2: Medium ──────────────────────────────────────────────────────────


@pytest.mark.integration
def test_level2_translate_and_deploy(client, setup_test_data):
    from dax_translator.dax_samples.level2_medium import get_model

    model = get_model()
    result = translate(model)

    assert len(result.joins) >= 1
    deploy_metric_view(client, result.sql)

    fq_view = f"{CATALOG}.{SCHEMA}.mv_level2_medium"

    # Validate filtered measure
    active_mv = query_metric_view(client, fq_view, measures=["Active Sales"])
    active_direct = run_direct_sql(
        client,
        f"SELECT SUM(amount) AS total FROM {CATALOG}.{SCHEMA}.fact_sales WHERE status = 'Active'",
    )
    assert float(active_mv[0]["Active Sales"]) == pytest.approx(
        float(active_direct[0]["total"]), rel=0.01
    )


# ── Level 3: Complex (Time Intelligence) ─────────────────────────────────────


@pytest.mark.integration
def test_level3_translate_and_deploy(client, setup_test_data):
    from dax_translator.dax_samples.level3_complex import EXPECTED_WARNINGS, get_model

    model = get_model()
    result = translate(model)

    # Should have warnings for time intelligence and RANKX
    warned_measures = {w.measure_name for w in result.warnings}
    for measure_name, expected_type in EXPECTED_WARNINGS.items():
        assert measure_name in warned_measures, f"Expected warning for {measure_name}"
        warning = next(w for w in result.warnings if w.measure_name == measure_name)
        assert warning.warning_type == expected_type

    # Deploy (even with warnings, non-warned measures should work)
    deploy_metric_view(client, result.sql)


# ── Level 4: Very Complex (Composite, Multi-table) ───────────────────────────


@pytest.mark.integration
def test_level4_translate_and_deploy(client, setup_test_data):
    from dax_translator.dax_samples.level4_very_complex import EXPECTED_MEASURES, get_model

    model = get_model()
    result = translate(model)

    # Should have joins for customer, product, calendar
    assert len(result.joins) >= 2

    deploy_metric_view(client, result.sql)

    fq_view = f"{CATALOG}.{SCHEMA}.mv_level4_very_complex"

    # Validate total sales
    total_mv = query_metric_view(client, fq_view, measures=["Total Sales"])
    total_direct = run_direct_sql(
        client, f"SELECT SUM(amount) AS total FROM {CATALOG}.{SCHEMA}.fact_sales"
    )
    assert float(total_mv[0]["Total Sales"]) == pytest.approx(
        float(total_direct[0]["total"]), rel=0.01
    )

    # Validate profit margin (safe division)
    margin_mv = query_metric_view(client, fq_view, measures=["Profit Margin"])
    margin_direct = run_direct_sql(
        client,
        f"SELECT SUM(profit) / NULLIF(SUM(revenue), 0) AS margin FROM {CATALOG}.{SCHEMA}.fact_sales",
    )
    assert float(margin_mv[0]["Profit Margin"]) == pytest.approx(
        float(margin_direct[0]["margin"]), rel=0.01
    )


# ── Full validation run ──────────────────────────────────────────────────────


@pytest.mark.integration
def test_validate_all_level1(client, setup_test_data):
    from dax_translator.dax_samples.level1_simple import get_model

    model = get_model()
    result = translate(model)
    deploy_metric_view(client, result.sql)

    validation_results = validate_all(result, CATALOG, SCHEMA, PROFILE)
    for vr in validation_results:
        assert vr.passed, f"Validation failed for {vr.case.measure_name}: {vr.error}"
