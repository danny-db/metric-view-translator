"""Unit tests for the translator — mocks Claude API to validate structure."""

import json
from unittest.mock import MagicMock, patch

import pytest

from dax_translator.models import TranslationStatus
from dax_translator.translator import _extract_json, _parse_response, translate


# ── Test JSON extraction ──────────────────────────────────────────────────────


def test_extract_json_plain():
    raw = '{"status": "success", "version": "1.1"}'
    result = _extract_json(raw)
    assert result["status"] == "success"


def test_extract_json_markdown_fenced():
    raw = '```json\n{"status": "success"}\n```'
    result = _extract_json(raw)
    assert result["status"] == "success"


def test_extract_json_markdown_no_lang():
    raw = '```\n{"status": "partial"}\n```'
    result = _extract_json(raw)
    assert result["status"] == "partial"


# ── Test parse_response ───────────────────────────────────────────────────────


def _make_dax_model():
    from dax_translator.dax_samples.level1_simple import get_model
    return get_model()


def test_parse_response_level1():
    model = _make_dax_model()
    raw = {
        "status": "success",
        "version": "1.1",
        "source": "main.dax_translator_test.fact_sales",
        "comment": "Simple aggregations",
        "joins": [],
        "dimensions": [
            {"name": "Region", "expr": "region"},
            {"name": "Status", "expr": "status"},
        ],
        "measures": [
            {"name": "Total Sales", "expr": "SUM(amount)"},
            {"name": "Order Count", "expr": "COUNT(order_id)"},
            {"name": "Unique Customers", "expr": "COUNT(DISTINCT customer_id)"},
            {"name": "Avg Order", "expr": "AVG(amount)"},
        ],
        "warnings": [],
    }
    result = _parse_response(raw, model)

    assert result.status == TranslationStatus.SUCCESS
    assert result.version == "1.1"
    assert result.source == "main.dax_translator_test.fact_sales"
    assert len(result.dimensions) == 2
    assert len(result.measures) == 4
    assert result.measures[0].name == "Total Sales"
    assert result.measures[0].expr == "SUM(amount)"
    assert "CREATE OR REPLACE VIEW" in result.sql
    assert "WITH METRICS" in result.sql
    assert "LANGUAGE YAML" in result.sql


def test_parse_response_with_warnings():
    model = _make_dax_model()
    raw = {
        "status": "partial",
        "version": "1.1",
        "source": "main.dax_translator_test.fact_sales",
        "dimensions": [{"name": "Region", "expr": "region"}],
        "measures": [{"name": "Total Sales", "expr": "SUM(amount)"}],
        "warnings": [
            {
                "measure_name": "Sales Rank",
                "dax_expression": "RANKX(ALL(Sales), [Total Sales])",
                "warning_type": "unsupported_dax",
                "message": "RANKX is not supported in Metric Views",
            }
        ],
    }
    result = _parse_response(raw, model)

    assert result.status == TranslationStatus.PARTIAL
    assert result.has_warnings
    assert result.warnings[0].warning_type == "unsupported_dax"


def test_parse_response_with_window_measure():
    model = _make_dax_model()
    raw = {
        "status": "success",
        "version": "1.1",  # Should be auto-corrected to 0.1
        "source": "main.dax_translator_test.fact_sales",
        "dimensions": [{"name": "date", "expr": "date_key"}],
        "measures": [
            {
                "name": "YTD Sales",
                "expr": "SUM(amount)",
                "window": [
                    {"order": "date", "range": "cumulative", "semiadditive": "last"},
                    {"order": "year", "range": "current", "semiadditive": "last"},
                ],
            }
        ],
        "warnings": [],
    }
    result = _parse_response(raw, model)

    assert result.version == "0.1"  # auto-detected
    assert result.has_window_measures
    assert len(result.measures[0].window) == 2


# ── Test translate (mocked FMAPI) ────────────────────────────────────────────


MOCK_LEVEL1_RESPONSE = json.dumps({
    "status": "success",
    "version": "1.1",
    "source": "main.dax_translator_test.fact_sales",
    "comment": "Level 1 simple aggregations",
    "joins": [],
    "dimensions": [
        {"name": "Region", "expr": "region"},
        {"name": "Status", "expr": "status"},
        {"name": "Date", "expr": "date_key"},
    ],
    "measures": [
        {"name": "Total Sales", "expr": "SUM(amount)"},
        {"name": "Order Count", "expr": "COUNT(order_id)"},
        {"name": "Unique Customers", "expr": "COUNT(DISTINCT customer_id)"},
        {"name": "Avg Order", "expr": "AVG(amount)"},
    ],
    "warnings": [],
})


@patch("dax_translator.translator._call_fmapi")
def test_translate_level1(mock_fmapi):
    """Test that translate() correctly calls FMAPI and parses the response."""
    mock_fmapi.return_value = MOCK_LEVEL1_RESPONSE

    model = _make_dax_model()
    result = translate(model)

    assert result.status == TranslationStatus.SUCCESS
    assert len(result.measures) == 4
    assert result.measures[0].expr == "SUM(amount)"
    assert "CREATE OR REPLACE VIEW" in result.sql

    # Verify FMAPI was called with system prompt containing DAX reference
    call_args = mock_fmapi.call_args
    assert "DAX" in call_args[0][0]  # system prompt


MOCK_LEVEL2_RESPONSE = json.dumps({
    "status": "success",
    "version": "1.1",
    "source": "main.dax_translator_test.fact_sales",
    "joins": [
        {
            "name": "customer",
            "source": "main.dax_translator_test.dim_customer",
            "on": "source.customer_id = customer.customer_id",
        }
    ],
    "dimensions": [
        {"name": "Region", "expr": "region"},
        {"name": "Order Size", "expr": "CASE WHEN amount > 1000 THEN 'Large' ELSE 'Small' END"},
        {"name": "Customer Name", "expr": "customer.name"},
    ],
    "measures": [
        {"name": "Active Sales", "expr": "SUM(amount) FILTER (WHERE status = 'Active')"},
        {"name": "Active West Sales", "expr": "SUM(amount) FILTER (WHERE status = 'Active' AND region = 'West')"},
        {"name": "Line Total", "expr": "SUM(quantity * unit_price)"},
    ],
    "warnings": [],
})


@patch("dax_translator.translator._call_fmapi")
def test_translate_level2(mock_fmapi):
    """Test level 2 with joins, FILTER, CASE dimensions."""
    from dax_translator.dax_samples.level2_medium import get_model

    mock_fmapi.return_value = MOCK_LEVEL2_RESPONSE

    model = get_model()
    result = translate(model)

    assert result.status == TranslationStatus.SUCCESS
    assert len(result.joins) == 1
    assert result.joins[0].name == "customer"
    assert "FILTER" in result.measures[0].expr


@patch("dax_translator.translator._call_fmapi")
def test_translate_retries_on_json_error(mock_fmapi):
    """Test that translator retries on malformed JSON response."""
    # First call returns bad JSON, second returns good
    mock_fmapi.side_effect = ["This is not JSON", MOCK_LEVEL1_RESPONSE]

    model = _make_dax_model()
    result = translate(model)

    assert result.status == TranslationStatus.SUCCESS
    assert mock_fmapi.call_count == 2
