"""Comprehensive tests covering all DAX patterns and edge cases.

Tests are organized by DAX category and validate that the translator
produces correct Metric View definitions for every supported pattern.
"""

import json
from unittest.mock import patch

import pytest

from dax_translator.models import (
    DaxColumn,
    DaxMeasure,
    DaxModel,
    DaxRelationship,
    DaxTable,
    TranslationStatus,
)
from dax_translator.translator import _extract_json, _parse_response, translate


CATALOG = "main"
SCHEMA = "dax_translator_test"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _fact_table():
    return DaxTable(
        name="Sales",
        databricks_table=f"{CATALOG}.{SCHEMA}.fact_sales",
        columns=[
            DaxColumn(name="OrderID", data_type="INT"),
            DaxColumn(name="CustomerID", data_type="INT"),
            DaxColumn(name="ProductID", data_type="INT"),
            DaxColumn(name="DateKey", data_type="DATE"),
            DaxColumn(name="Amount", data_type="DECIMAL"),
            DaxColumn(name="Quantity", data_type="INT"),
            DaxColumn(name="UnitPrice", data_type="DECIMAL"),
            DaxColumn(name="Profit", data_type="DECIMAL"),
            DaxColumn(name="Revenue", data_type="DECIMAL"),
            DaxColumn(name="Status", data_type="STRING"),
            DaxColumn(name="Region", data_type="STRING"),
            DaxColumn(name="Tier", data_type="STRING"),
            DaxColumn(name="Discount", data_type="DECIMAL"),
            DaxColumn(name="Cost", data_type="DECIMAL"),
            DaxColumn(name="Weight", data_type="DECIMAL"),
        ],
    )


def _dim_customer():
    return DaxTable(
        name="DimCustomer",
        databricks_table=f"{CATALOG}.{SCHEMA}.dim_customer",
        columns=[
            DaxColumn(name="CustomerID", data_type="INT"),
            DaxColumn(name="Name", data_type="STRING"),
            DaxColumn(name="Segment", data_type="STRING"),
            DaxColumn(name="Country", data_type="STRING"),
            DaxColumn(name="City", data_type="STRING"),
        ],
    )


def _dim_product():
    return DaxTable(
        name="DimProduct",
        databricks_table=f"{CATALOG}.{SCHEMA}.dim_product",
        columns=[
            DaxColumn(name="ProductID", data_type="INT"),
            DaxColumn(name="ProductName", data_type="STRING"),
            DaxColumn(name="Category", data_type="STRING"),
            DaxColumn(name="SubCategory", data_type="STRING"),
        ],
    )


def _dim_date():
    return DaxTable(
        name="Calendar",
        databricks_table=f"{CATALOG}.{SCHEMA}.dim_date",
        columns=[
            DaxColumn(name="Date", data_type="DATE"),
            DaxColumn(name="Year", data_type="INT"),
            DaxColumn(name="Month", data_type="INT"),
            DaxColumn(name="Quarter", data_type="STRING"),
        ],
    )


def _make_model(name, measures, tables=None, relationships=None):
    return DaxModel(
        name=name,
        fact_table="Sales",
        catalog=CATALOG,
        schema_name=SCHEMA,
        tables=tables or [_fact_table()],
        relationships=relationships or [],
        measures=measures,
    )


def _mock_response(measures, dimensions=None, joins=None, warnings=None, version="1.1", status="success"):
    """Build a mock Claude JSON response."""
    return json.dumps({
        "status": status,
        "version": version,
        "source": f"{CATALOG}.{SCHEMA}.fact_sales",
        "joins": joins or [],
        "dimensions": dimensions or [{"name": "Region", "expr": "region"}],
        "measures": measures,
        "warnings": warnings or [],
    })


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 1: Simple Aggregations
# ══════════════════════════════════════════════════════════════════════════════


class TestSimpleAggregations:
    """SUM, COUNT, DISTINCTCOUNT, AVERAGE, MIN, MAX, COUNTROWS."""

    def test_sum(self):
        raw = {"status": "success", "version": "1.1",
               "source": f"{CATALOG}.{SCHEMA}.fact_sales",
               "dimensions": [{"name": "R", "expr": "region"}],
               "measures": [{"name": "Total", "expr": "SUM(amount)"}], "warnings": []}
        result = _parse_response(raw, _make_model("t", [DaxMeasure(name="Total", expression="SUM(Sales[Amount])")]))
        assert result.measures[0].expr == "SUM(amount)"

    def test_count(self):
        raw = {"status": "success", "version": "1.1",
               "source": f"{CATALOG}.{SCHEMA}.fact_sales",
               "dimensions": [{"name": "R", "expr": "region"}],
               "measures": [{"name": "Cnt", "expr": "COUNT(order_id)"}], "warnings": []}
        result = _parse_response(raw, _make_model("t", [DaxMeasure(name="Cnt", expression="COUNT(Sales[OrderID])")]))
        assert "COUNT" in result.measures[0].expr

    def test_distinctcount(self):
        raw = {"status": "success", "version": "1.1",
               "source": f"{CATALOG}.{SCHEMA}.fact_sales",
               "dimensions": [{"name": "R", "expr": "region"}],
               "measures": [{"name": "UC", "expr": "COUNT(DISTINCT customer_id)"}], "warnings": []}
        result = _parse_response(raw, _make_model("t", [DaxMeasure(name="UC", expression="DISTINCTCOUNT(Sales[CustomerID])")]))
        assert "DISTINCT" in result.measures[0].expr

    def test_average(self):
        raw = {"status": "success", "version": "1.1",
               "source": f"{CATALOG}.{SCHEMA}.fact_sales",
               "dimensions": [{"name": "R", "expr": "region"}],
               "measures": [{"name": "Avg", "expr": "AVG(amount)"}], "warnings": []}
        result = _parse_response(raw, _make_model("t", [DaxMeasure(name="Avg", expression="AVERAGE(Sales[Amount])")]))
        assert "AVG" in result.measures[0].expr

    def test_min(self):
        raw = {"status": "success", "version": "1.1",
               "source": f"{CATALOG}.{SCHEMA}.fact_sales",
               "dimensions": [{"name": "R", "expr": "region"}],
               "measures": [{"name": "Min", "expr": "MIN(amount)"}], "warnings": []}
        result = _parse_response(raw, _make_model("t", [DaxMeasure(name="Min", expression="MIN(Sales[Amount])")]))
        assert "MIN" in result.measures[0].expr

    def test_max(self):
        raw = {"status": "success", "version": "1.1",
               "source": f"{CATALOG}.{SCHEMA}.fact_sales",
               "dimensions": [{"name": "R", "expr": "region"}],
               "measures": [{"name": "Max", "expr": "MAX(amount)"}], "warnings": []}
        result = _parse_response(raw, _make_model("t", [DaxMeasure(name="Max", expression="MAX(Sales[Amount])")]))
        assert "MAX" in result.measures[0].expr

    def test_countrows(self):
        raw = {"status": "success", "version": "1.1",
               "source": f"{CATALOG}.{SCHEMA}.fact_sales",
               "dimensions": [{"name": "R", "expr": "region"}],
               "measures": [{"name": "Rows", "expr": "COUNT(1)"}], "warnings": []}
        result = _parse_response(raw, _make_model("t", [DaxMeasure(name="Rows", expression="COUNTROWS(Sales)")]))
        assert "COUNT" in result.measures[0].expr


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 2: CALCULATE with Filters
# ══════════════════════════════════════════════════════════════════════════════


class TestCalculateFilters:
    """CALCULATE with single/multiple filters → FILTER (WHERE ...)."""

    def test_single_filter(self):
        measures = [{"name": "Active", "expr": "SUM(amount) FILTER (WHERE status = 'Active')"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="Active", expression='CALCULATE(SUM(Sales[Amount]), Sales[Status]="Active")')
        ]))
        assert "FILTER" in result.measures[0].expr
        assert "Active" in result.measures[0].expr

    def test_multiple_filters_and(self):
        measures = [{"name": "AW", "expr": "SUM(amount) FILTER (WHERE status = 'Active' AND region = 'West')"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="AW", expression='CALCULATE(SUM(Sales[Amount]), Sales[Status]="Active", Sales[Region]="West")')
        ]))
        assert "AND" in result.measures[0].expr
        assert "FILTER" in result.measures[0].expr

    def test_three_filters(self):
        measures = [{"name": "F3", "expr": "SUM(amount) FILTER (WHERE status = 'Active' AND region = 'West' AND tier = 'Premium')"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="F3", expression='CALCULATE(SUM(Sales[Amount]), Sales[Status]="Active", Sales[Region]="West", Sales[Tier]="Premium")')
        ]))
        expr = result.measures[0].expr.upper()
        assert expr.count("AND") == 2

    def test_numeric_filter(self):
        measures = [{"name": "Big", "expr": "SUM(amount) FILTER (WHERE amount > 1000)"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="Big", expression='CALCULATE(SUM(Sales[Amount]), Sales[Amount]>1000)')
        ]))
        assert "1000" in result.measures[0].expr

    def test_not_equal_filter(self):
        measures = [{"name": "NonInactive", "expr": "SUM(amount) FILTER (WHERE status <> 'Inactive')"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="NonInactive", expression='CALCULATE(SUM(Sales[Amount]), Sales[Status]<>"Inactive")')
        ]))
        assert "Inactive" in result.measures[0].expr


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 3: Row-Level Operations (SUMX, AVERAGEX, etc.)
# ══════════════════════════════════════════════════════════════════════════════


class TestRowLevelOperations:
    """SUMX, AVERAGEX, MAXX, MINX — row-by-row calculations."""

    def test_sumx_multiply(self):
        measures = [{"name": "LineTotal", "expr": "SUM(quantity * unit_price)"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="LineTotal", expression="SUMX(Sales, Sales[Quantity]*Sales[UnitPrice])")
        ]))
        assert "*" in result.measures[0].expr or "quantity" in result.measures[0].expr.lower()

    def test_sumx_subtract(self):
        measures = [{"name": "TotalProfit", "expr": "SUM(revenue - cost)"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="TotalProfit", expression="SUMX(Sales, Sales[Revenue]-Sales[Cost])")
        ]))
        assert "-" in result.measures[0].expr or "revenue" in result.measures[0].expr.lower()

    def test_sumx_complex_expression(self):
        measures = [{"name": "DiscountedTotal", "expr": "SUM(quantity * unit_price * (1 - discount))"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="DiscountedTotal", expression="SUMX(Sales, Sales[Quantity]*Sales[UnitPrice]*(1-Sales[Discount]))")
        ]))
        assert "SUM" in result.measures[0].expr.upper()

    def test_averagex(self):
        measures = [{"name": "AvgLine", "expr": "AVG(quantity * unit_price)"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="AvgLine", expression="AVERAGEX(Sales, Sales[Quantity]*Sales[UnitPrice])")
        ]))
        assert "AVG" in result.measures[0].expr.upper()


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 4: IF/SWITCH Dimensions
# ══════════════════════════════════════════════════════════════════════════════


class TestConditionalDimensions:
    """IF and SWITCH as calculated dimensions → CASE WHEN."""

    def test_if_two_outcomes(self):
        dims = [{"name": "Size", "expr": "CASE WHEN amount > 1000 THEN 'Large' ELSE 'Small' END"}]
        raw = json.loads(_mock_response([], dimensions=dims))
        result = _parse_response(raw, _make_model("t", []))
        assert "CASE" in result.dimensions[0].expr
        assert "WHEN" in result.dimensions[0].expr

    def test_nested_if(self):
        dims = [{"name": "Tier", "expr": "CASE WHEN amount > 5000 THEN 'Enterprise' WHEN amount > 1000 THEN 'Mid' ELSE 'SMB' END"}]
        raw = json.loads(_mock_response([], dimensions=dims))
        result = _parse_response(raw, _make_model("t", []))
        assert result.dimensions[0].expr.count("WHEN") >= 2

    def test_switch_equivalent(self):
        """DAX SWITCH(TRUE(), ...) → multi-WHEN CASE."""
        dims = [{"name": "Priority", "expr": "CASE WHEN status = 'Active' THEN 'High' WHEN status = 'Pending' THEN 'Medium' ELSE 'Low' END"}]
        raw = json.loads(_mock_response([], dimensions=dims))
        result = _parse_response(raw, _make_model("t", []))
        assert "CASE" in result.dimensions[0].expr


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 5: DIVIDE / Safe Division
# ══════════════════════════════════════════════════════════════════════════════


class TestDivision:
    """DIVIDE function → NULLIF for safe division."""

    def test_divide_with_default(self):
        measures = [{"name": "Margin", "expr": "SUM(profit) / NULLIF(SUM(revenue), 0)"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="Margin", expression="DIVIDE(SUM(Sales[Profit]), SUM(Sales[Revenue]), 0)")
        ]))
        assert "NULLIF" in result.measures[0].expr

    def test_simple_ratio(self):
        measures = [{"name": "RPC", "expr": "SUM(amount) / COUNT(DISTINCT customer_id)"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="RPC", expression="SUM(Sales[Amount]) / DISTINCTCOUNT(Sales[CustomerID])")
        ]))
        assert "/" in result.measures[0].expr

    def test_divide_count_by_count(self):
        measures = [{"name": "Rate", "expr": "COUNT(1) FILTER (WHERE status = 'Active') * 1.0 / NULLIF(COUNT(1), 0)"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="Rate", expression='DIVIDE(CALCULATE(COUNT(Sales[OrderID]), Sales[Status]="Active"), COUNT(Sales[OrderID]), 0)')
        ]))
        assert "/" in result.measures[0].expr


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 6: Composite / Derived Measures
# ══════════════════════════════════════════════════════════════════════════════


class TestCompositeMeasures:
    """Measures referencing other measures — should be inlined."""

    def test_composite_ratio(self):
        measures = [
            {"name": "Total Sales", "expr": "SUM(amount)"},
            {"name": "Sales per Customer", "expr": "SUM(amount) / COUNT(DISTINCT customer_id)"},
        ]
        raw = json.loads(_mock_response(measures))
        model = _make_model("t", [
            DaxMeasure(name="Total Sales", expression="SUM(Sales[Amount])"),
            DaxMeasure(name="Sales per Customer", expression="[Total Sales] / DISTINCTCOUNT(Sales[CustomerID])"),
        ])
        result = _parse_response(raw, model)
        assert len(result.measures) == 2
        assert "/" in result.measures[1].expr

    def test_filtered_composite(self):
        """CALCULATE([OtherMeasure], filter) → inlined with FILTER."""
        measures = [
            {"name": "Total", "expr": "SUM(amount)"},
            {"name": "Premium", "expr": "SUM(amount) FILTER (WHERE tier = 'Premium')"},
            {"name": "Premium Pct", "expr": "SUM(amount) FILTER (WHERE tier = 'Premium') / NULLIF(SUM(amount), 0)"},
        ]
        raw = json.loads(_mock_response(measures))
        model = _make_model("t", [
            DaxMeasure(name="Total", expression="SUM(Sales[Amount])"),
            DaxMeasure(name="Premium", expression='CALCULATE([Total], Sales[Tier]="Premium")'),
            DaxMeasure(name="Premium Pct", expression="DIVIDE([Premium], [Total], 0)"),
        ])
        result = _parse_response(raw, model)
        assert len(result.measures) == 3


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 7: Joins / RELATED
# ══════════════════════════════════════════════════════════════════════════════


class TestJoinsAndRelated:
    """Star schema joins and RELATED dimension columns."""

    def test_single_join(self):
        joins = [{"name": "customer", "source": f"{CATALOG}.{SCHEMA}.dim_customer",
                  "on": "source.customer_id = customer.customer_id"}]
        dims = [{"name": "Customer Name", "expr": "customer.name"}]
        raw = json.loads(_mock_response([], dimensions=dims, joins=joins))
        model = _make_model("t", [], tables=[_fact_table(), _dim_customer()],
                           relationships=[DaxRelationship(from_table="Sales", from_column="CustomerID",
                                                          to_table="DimCustomer", to_column="CustomerID")])
        result = _parse_response(raw, model)
        assert len(result.joins) == 1
        assert result.joins[0].name == "customer"
        assert "customer.name" in result.dimensions[0].expr

    def test_multiple_joins(self):
        joins = [
            {"name": "customer", "source": f"{CATALOG}.{SCHEMA}.dim_customer",
             "on": "source.customer_id = customer.customer_id"},
            {"name": "product", "source": f"{CATALOG}.{SCHEMA}.dim_product",
             "on": "source.product_id = product.product_id"},
        ]
        dims = [
            {"name": "Customer", "expr": "customer.name"},
            {"name": "Category", "expr": "product.category"},
        ]
        raw = json.loads(_mock_response([], dimensions=dims, joins=joins))
        model = _make_model("t", [], tables=[_fact_table(), _dim_customer(), _dim_product()],
                           relationships=[
                               DaxRelationship(from_table="Sales", from_column="CustomerID",
                                               to_table="DimCustomer", to_column="CustomerID"),
                               DaxRelationship(from_table="Sales", from_column="ProductID",
                                               to_table="DimProduct", to_column="ProductID"),
                           ])
        result = _parse_response(raw, model)
        assert len(result.joins) == 2

    def test_three_dimension_tables(self):
        """Full star schema with customer, product, date joins."""
        joins = [
            {"name": "customer", "source": f"{CATALOG}.{SCHEMA}.dim_customer",
             "on": "source.customer_id = customer.customer_id"},
            {"name": "product", "source": f"{CATALOG}.{SCHEMA}.dim_product",
             "on": "source.product_id = product.product_id"},
            {"name": "calendar", "source": f"{CATALOG}.{SCHEMA}.dim_date",
             "on": "source.date_key = calendar.date"},
        ]
        raw = json.loads(_mock_response(
            measures=[{"name": "Total", "expr": "SUM(amount)"}],
            dimensions=[
                {"name": "Segment", "expr": "customer.segment"},
                {"name": "Category", "expr": "product.category"},
                {"name": "Year", "expr": "calendar.year"},
            ],
            joins=joins
        ))
        model = _make_model("t",
                           [DaxMeasure(name="Total", expression="SUM(Sales[Amount])")],
                           tables=[_fact_table(), _dim_customer(), _dim_product(), _dim_date()],
                           relationships=[
                               DaxRelationship(from_table="Sales", from_column="CustomerID",
                                               to_table="DimCustomer", to_column="CustomerID"),
                               DaxRelationship(from_table="Sales", from_column="ProductID",
                                               to_table="DimProduct", to_column="ProductID"),
                               DaxRelationship(from_table="Sales", from_column="DateKey",
                                               to_table="Calendar", to_column="Date"),
                           ])
        result = _parse_response(raw, model)
        assert len(result.joins) == 3


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 8: Time Intelligence
# ══════════════════════════════════════════════════════════════════════════════


class TestTimeIntelligence:
    """TOTALYTD, SAMEPERIODLASTYEAR, DATEADD — window measures and warnings."""

    def test_totalytd_window_measure(self):
        measures = [{
            "name": "YTD",
            "expr": "SUM(amount)",
            "window": [
                {"order": "date", "range": "cumulative", "semiadditive": "last"},
                {"order": "year", "range": "current", "semiadditive": "last"},
            ],
        }]
        raw = json.loads(_mock_response(measures, version="0.1"))
        model = _make_model("t", [DaxMeasure(name="YTD", expression="TOTALYTD(SUM(Sales[Amount]), Calendar[Date])")])
        result = _parse_response(raw, model)
        assert result.version == "0.1"
        assert result.has_window_measures
        assert len(result.measures[0].window) == 2

    def test_sameperiodlastyear_warning(self):
        measures = [{"name": "LY", "expr": "SUM(amount)", "window": [{"order": "date", "range": "trailing 1 year", "semiadditive": "last"}]}]
        warnings = [{"measure_name": "LY", "dax_expression": "CALCULATE(SUM(Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date]))",
                     "warning_type": "time_intelligence", "message": "Approximated"}]
        raw = json.loads(_mock_response(measures, warnings=warnings, version="0.1", status="partial"))
        model = _make_model("t", [DaxMeasure(name="LY", expression="CALCULATE(SUM(Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date]))")])
        result = _parse_response(raw, model)
        assert result.has_warnings
        assert result.warnings[0].warning_type == "time_intelligence"

    def test_dateadd_warning(self):
        warnings = [{"measure_name": "LM", "dax_expression": "CALCULATE(SUM(Sales[Amount]), DATEADD(Calendar[Date], -1, MONTH))",
                     "warning_type": "time_intelligence", "message": "Approximated as trailing 1 month"}]
        raw = json.loads(_mock_response(
            [{"name": "LM", "expr": "SUM(amount)", "window": [{"order": "date", "range": "trailing 1 month", "semiadditive": "last"}]}],
            warnings=warnings, version="0.1", status="partial"
        ))
        model = _make_model("t", [DaxMeasure(name="LM", expression="CALCULATE(SUM(Sales[Amount]), DATEADD(Calendar[Date], -1, MONTH))")])
        result = _parse_response(raw, model)
        assert result.has_warnings


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 9: Unsupported DAX
# ══════════════════════════════════════════════════════════════════════════════


class TestUnsupportedDax:
    """RANKX, USERELATIONSHIP, ALLSELECTED, etc. → warnings."""

    def test_rankx_warning(self):
        warnings = [{"measure_name": "Rank", "dax_expression": "RANKX(ALL(Sales), [Total])",
                     "warning_type": "unsupported_dax", "message": "RANKX not supported"}]
        raw = json.loads(_mock_response([], warnings=warnings, status="partial"))
        model = _make_model("t", [DaxMeasure(name="Rank", expression="RANKX(ALL(Sales), [Total])")])
        result = _parse_response(raw, model)
        assert result.status == TranslationStatus.PARTIAL
        assert result.warnings[0].warning_type == "unsupported_dax"

    def test_userelationship_warning(self):
        warnings = [{"measure_name": "Alt", "dax_expression": "CALCULATE(SUM(Sales[Amount]), USERELATIONSHIP(Sales[ShipDate], Calendar[Date]))",
                     "warning_type": "unsupported_dax", "message": "USERELATIONSHIP not supported"}]
        raw = json.loads(_mock_response([], warnings=warnings, status="partial"))
        model = _make_model("t", [DaxMeasure(name="Alt", expression="CALCULATE(SUM(Sales[Amount]), USERELATIONSHIP(Sales[ShipDate], Calendar[Date]))")])
        result = _parse_response(raw, model)
        assert result.warnings[0].warning_type == "unsupported_dax"

    def test_allselected_warning(self):
        warnings = [{"measure_name": "Pct", "dax_expression": "DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALLSELECTED()))",
                     "warning_type": "unsupported_dax", "message": "ALLSELECTED not supported"}]
        raw = json.loads(_mock_response([], warnings=warnings, status="partial"))
        model = _make_model("t", [DaxMeasure(name="Pct", expression="DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALLSELECTED()))")])
        result = _parse_response(raw, model)
        assert result.warnings[0].warning_type == "unsupported_dax"

    def test_topn_warning(self):
        warnings = [{"measure_name": "TopN", "dax_expression": "CALCULATE(SUM(Sales[Amount]), TOPN(10, Sales, Sales[Amount]))",
                     "warning_type": "unsupported_dax", "message": "TOPN not supported"}]
        raw = json.loads(_mock_response([], warnings=warnings, status="partial"))
        model = _make_model("t", [DaxMeasure(name="TopN", expression="CALCULATE(SUM(Sales[Amount]), TOPN(10, Sales, Sales[Amount]))")])
        result = _parse_response(raw, model)
        assert result.warnings[0].warning_type == "unsupported_dax"


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 10: Window Measures (version 0.1 specifics)
# ══════════════════════════════════════════════════════════════════════════════


class TestWindowMeasures:
    """Window measure structure and version auto-detection."""

    def test_version_auto_downgrade(self):
        """If any measure has a window, version should be 0.1."""
        measures = [
            {"name": "Simple", "expr": "SUM(amount)"},
            {"name": "Windowed", "expr": "SUM(amount)",
             "window": [{"order": "date", "range": "cumulative", "semiadditive": "last"}]},
        ]
        raw = json.loads(_mock_response(measures, version="1.1"))
        model = _make_model("t", [
            DaxMeasure(name="Simple", expression="SUM(Sales[Amount])"),
            DaxMeasure(name="Windowed", expression="TOTALYTD(SUM(Sales[Amount]), Calendar[Date])"),
        ])
        result = _parse_response(raw, model)
        assert result.version == "0.1"

    def test_trailing_window(self):
        measures = [{"name": "T7D", "expr": "COUNT(DISTINCT customer_id)",
                     "window": [{"order": "date", "range": "trailing 7 day", "semiadditive": "last"}]}]
        raw = json.loads(_mock_response(measures, version="0.1"))
        model = _make_model("t", [DaxMeasure(name="T7D", expression="WINDOW_7DAY")])
        result = _parse_response(raw, model)
        assert result.measures[0].window[0].range == "trailing 7 day"

    def test_no_comments_in_v01_yaml(self):
        """Version 0.1 YAML should not contain comments."""
        measures = [{"name": "W", "expr": "SUM(amount)",
                     "window": [{"order": "d", "range": "current", "semiadditive": "last"}]}]
        raw = json.loads(_mock_response(measures, version="0.1"))
        raw["comment"] = "Should be stripped"
        raw["measures"][0]["comment"] = "Also stripped"
        raw["dimensions"] = [{"name": "d", "expr": "date_key", "comment": "Stripped too"}]
        model = _make_model("t", [DaxMeasure(name="W", expression="X")])
        result = _parse_response(raw, model)
        assert "comment" not in result.yaml_body.split("measures")[0] or result.version != "0.1"


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 11: Edge Cases
# ══════════════════════════════════════════════════════════════════════════════


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_single_measure_model(self):
        measures = [{"name": "Total", "expr": "SUM(amount)"}]
        raw = json.loads(_mock_response(measures))
        model = _make_model("single", [DaxMeasure(name="Total", expression="SUM(Sales[Amount])")])
        result = _parse_response(raw, model)
        assert len(result.measures) == 1

    def test_many_measures(self):
        measures = [{"name": f"M{i}", "expr": f"SUM(amount)"} for i in range(20)]
        raw = json.loads(_mock_response(measures))
        dax_measures = [DaxMeasure(name=f"M{i}", expression="SUM(Sales[Amount])") for i in range(20)]
        model = _make_model("many", dax_measures)
        result = _parse_response(raw, model)
        assert len(result.measures) == 20

    def test_all_warnings_partial_status(self):
        warnings = [
            {"measure_name": "R1", "dax_expression": "RANKX(...)", "warning_type": "unsupported_dax", "message": "n/a"},
            {"measure_name": "R2", "dax_expression": "TOPN(...)", "warning_type": "unsupported_dax", "message": "n/a"},
        ]
        raw = json.loads(_mock_response([], warnings=warnings, status="partial"))
        model = _make_model("t", [
            DaxMeasure(name="R1", expression="RANKX(...)"),
            DaxMeasure(name="R2", expression="TOPN(...)"),
        ])
        result = _parse_response(raw, model)
        assert result.status == TranslationStatus.PARTIAL
        assert len(result.warnings) == 2

    def test_sql_contains_all_required_parts(self):
        measures = [{"name": "T", "expr": "SUM(amount)"}]
        raw = json.loads(_mock_response(measures))
        model = _make_model("t", [DaxMeasure(name="T", expression="SUM(Sales[Amount])")])
        result = _parse_response(raw, model)
        assert "CREATE OR REPLACE VIEW" in result.sql
        assert "WITH METRICS" in result.sql
        assert "LANGUAGE YAML" in result.sql
        assert "AS $$" in result.sql
        assert "$$" in result.sql
        assert "version:" in result.yaml_body
        assert "source:" in result.yaml_body
        assert "dimensions:" in result.yaml_body
        assert "measures:" in result.yaml_body

    def test_source_fallback_from_model(self):
        """If Claude returns empty source, fall back to the model's fact table."""
        raw = {"status": "success", "version": "1.1", "source": "",
               "dimensions": [{"name": "R", "expr": "region"}],
               "measures": [{"name": "T", "expr": "SUM(amount)"}], "warnings": []}
        model = _make_model("t", [DaxMeasure(name="T", expression="SUM(Sales[Amount])")])
        result = _parse_response(raw, model)
        assert result.source == f"{CATALOG}.{SCHEMA}.fact_sales"


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 12: JSON Extraction Edge Cases
# ══════════════════════════════════════════════════════════════════════════════


class TestJsonExtraction:
    """Edge cases in extracting JSON from Claude responses."""

    def test_plain_json(self):
        assert _extract_json('{"a": 1}') == {"a": 1}

    def test_json_with_whitespace(self):
        assert _extract_json('  \n  {"a": 1}  \n  ') == {"a": 1}

    def test_markdown_json_fence(self):
        assert _extract_json('```json\n{"a": 1}\n```') == {"a": 1}

    def test_markdown_plain_fence(self):
        assert _extract_json('```\n{"a": 1}\n```') == {"a": 1}

    def test_nested_json(self):
        text = '{"a": {"b": [1, 2, 3]}}'
        assert _extract_json(text)["a"]["b"] == [1, 2, 3]

    def test_invalid_json_raises(self):
        with pytest.raises(Exception):
            _extract_json("not json at all")

    def test_empty_string_raises(self):
        with pytest.raises(Exception):
            _extract_json("")


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 13: Filtered Measures (FILTER clause variants)
# ══════════════════════════════════════════════════════════════════════════════


class TestFilteredMeasures:
    """Various FILTER clause patterns."""

    def test_count_with_filter(self):
        measures = [{"name": "ActiveOrders", "expr": "COUNT(1) FILTER (WHERE status = 'Active')"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="ActiveOrders", expression='CALCULATE(COUNTROWS(Sales), Sales[Status]="Active")')
        ]))
        assert "FILTER" in result.measures[0].expr

    def test_filtered_ratio(self):
        measures = [{"name": "FR", "expr": "SUM(amount) FILTER (WHERE status = 'Active') / NULLIF(SUM(amount), 0)"}]
        raw = json.loads(_mock_response(measures))
        result = _parse_response(raw, _make_model("t", [
            DaxMeasure(name="FR", expression='DIVIDE(CALCULATE(SUM(Sales[Amount]), Sales[Status]="Active"), SUM(Sales[Amount]), 0)')
        ]))
        assert "FILTER" in result.measures[0].expr
        assert "NULLIF" in result.measures[0].expr


# ══════════════════════════════════════════════════════════════════════════════
# CATEGORY 14: FMAPI Integration (Mocked)
# ══════════════════════════════════════════════════════════════════════════════


class TestFMAPIIntegration:
    """Test translate() with mocked FMAPI calls."""

    @patch("dax_translator.translator._call_fmapi")
    def test_uses_fmapi_by_default(self, mock_fmapi):
        mock_fmapi.return_value = _mock_response(
            [{"name": "T", "expr": "SUM(amount)"}]
        )
        model = _make_model("t", [DaxMeasure(name="T", expression="SUM(Sales[Amount])")])
        result = translate(model)
        assert mock_fmapi.called
        assert result.status == TranslationStatus.SUCCESS

    @patch("dax_translator.translator._call_fmapi")
    def test_retries_on_bad_json(self, mock_fmapi):
        good = _mock_response([{"name": "T", "expr": "SUM(amount)"}])
        mock_fmapi.side_effect = ["bad json", good]
        model = _make_model("t", [DaxMeasure(name="T", expression="SUM(Sales[Amount])")])
        result = translate(model)
        assert mock_fmapi.call_count == 2
        assert result.status == TranslationStatus.SUCCESS

    @patch("dax_translator.translator._call_fmapi")
    def test_fails_after_max_retries(self, mock_fmapi):
        mock_fmapi.return_value = "not json"
        model = _make_model("t", [DaxMeasure(name="T", expression="SUM(Sales[Amount])")])
        with pytest.raises(RuntimeError, match="Failed to translate"):
            translate(model)
        assert mock_fmapi.call_count == 3  # MAX_RETRIES + 1

    @patch("dax_translator.translator._call_fmapi")
    def test_custom_endpoint(self, mock_fmapi):
        mock_fmapi.return_value = _mock_response([{"name": "T", "expr": "SUM(amount)"}])
        model = _make_model("t", [DaxMeasure(name="T", expression="SUM(Sales[Amount])")])
        translate(model, fmapi_endpoint="custom-claude-endpoint")
        assert mock_fmapi.call_args[0][2] == "custom-claude-endpoint"
