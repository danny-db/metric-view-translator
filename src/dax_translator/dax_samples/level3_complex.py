"""Level 3: Complex — Time intelligence with dim_date, window measures."""

from ..models import DaxColumn, DaxMeasure, DaxModel, DaxRelationship, DaxTable

CATALOG = "main"
SCHEMA = "dax_translator_test"


def get_model() -> DaxModel:
    return DaxModel(
        name="level3_complex",
        fact_table="Sales",
        catalog=CATALOG,
        schema_name=SCHEMA,
        tables=[
            DaxTable(
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
                    DaxColumn(name="Status", data_type="STRING"),
                    DaxColumn(name="Region", data_type="STRING"),
                    DaxColumn(name="Tier", data_type="STRING"),
                ],
            ),
            DaxTable(
                name="Calendar",
                databricks_table=f"{CATALOG}.{SCHEMA}.dim_date",
                columns=[
                    DaxColumn(name="Date", data_type="DATE"),
                    DaxColumn(name="Year", data_type="INT"),
                    DaxColumn(name="Month", data_type="INT"),
                    DaxColumn(name="Quarter", data_type="STRING"),
                ],
            ),
        ],
        relationships=[
            DaxRelationship(
                from_table="Sales",
                from_column="DateKey",
                to_table="Calendar",
                to_column="Date",
            ),
        ],
        measures=[
            DaxMeasure(
                name="YTD Sales",
                expression="TOTALYTD(SUM(Sales[Amount]), Calendar[Date])",
                description="Year-to-date sales total",
            ),
            DaxMeasure(
                name="Sales Last Year",
                expression="CALCULATE(SUM(Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date]))",
                description="Sales for the same period last year",
            ),
            DaxMeasure(
                name="Sales Last Month",
                expression="CALCULATE(SUM(Sales[Amount]), DATEADD(Calendar[Date], -1, MONTH))",
                description="Sales shifted back one month",
            ),
            DaxMeasure(
                name="Sales Rank",
                expression="RANKX(ALL(Sales), [Total Sales])",
                description="Rank of sales — unsupported in Metric Views",
            ),
        ],
    )


# Expected: YTD should become a window measure with cumulative + current year
# SAMEPERIODLASTYEAR and DATEADD should produce warnings
# RANKX should produce an unsupported_dax warning
EXPECTED_WARNINGS = {
    "Sales Last Year": "time_intelligence",
    "Sales Last Month": "time_intelligence",
    "Sales Rank": "unsupported_dax",
}
