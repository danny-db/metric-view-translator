"""Level 6: Expert DAX — patterns that push the limits of Metric View translation.

Many of these should produce warnings or partial translations.
Tests the graceful degradation of the translator.
"""

from ..models import DaxColumn, DaxMeasure, DaxModel, DaxRelationship, DaxTable

CATALOG = "main"
SCHEMA = "dax_translator_test"


def get_model() -> DaxModel:
    return DaxModel(
        name="level6_expert",
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
                    DaxColumn(name="Revenue", data_type="DECIMAL"),
                    DaxColumn(name="Status", data_type="STRING"),
                    DaxColumn(name="Region", data_type="STRING"),
                    DaxColumn(name="Tier", data_type="STRING"),
                ],
            ),
            DaxTable(
                name="DimCustomer",
                databricks_table=f"{CATALOG}.{SCHEMA}.dim_customer",
                columns=[
                    DaxColumn(name="CustomerID", data_type="INT"),
                    DaxColumn(name="Name", data_type="STRING"),
                    DaxColumn(name="Segment", data_type="STRING"),
                    DaxColumn(name="Country", data_type="STRING"),
                ],
            ),
            DaxTable(
                name="DimProduct",
                databricks_table=f"{CATALOG}.{SCHEMA}.dim_product",
                columns=[
                    DaxColumn(name="ProductID", data_type="INT"),
                    DaxColumn(name="ProductName", data_type="STRING"),
                    DaxColumn(name="Category", data_type="STRING"),
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
            DaxRelationship(from_table="Sales", from_column="CustomerID",
                           to_table="DimCustomer", to_column="CustomerID"),
            DaxRelationship(from_table="Sales", from_column="ProductID",
                           to_table="DimProduct", to_column="ProductID"),
            DaxRelationship(from_table="Sales", from_column="DateKey",
                           to_table="Calendar", to_column="Date"),
        ],
        measures=[
            # 6.1: Semi-additive balance — LASTDATE pattern
            DaxMeasure(
                name="Closing Balance",
                expression="CALCULATE(SUM(Sales[Amount]), LASTDATE(Calendar[Date]))",
                description="Semi-additive: value at last date in period",
            ),
            # 6.2: Rolling 7-month average — AVERAGEX + DATESINPERIOD
            DaxMeasure(
                name="Rolling 7M Avg",
                expression="AVERAGEX(DATESINPERIOD(Calendar[Date], MAX(Calendar[Date]), -7, MONTH), CALCULATE(SUM(Sales[Amount])))",
                description="7-month rolling average of sales",
            ),
            # 6.3: New customer count — CALCULATE + FILTER + earliest purchase
            DaxMeasure(
                name="New Customers",
                expression='COUNTROWS(FILTER(VALUES(Sales[CustomerID]), CALCULATE(MIN(Sales[DateKey])) >= MIN(Calendar[Date]) && CALCULATE(MIN(Sales[DateKey])) <= MAX(Calendar[Date])))',
                description="Customers whose first purchase is in the current period",
            ),
            # 6.4: TOPN-based measure — Top 10 customers revenue
            DaxMeasure(
                name="Top 10 Revenue",
                expression="SUMX(TOPN(10, VALUES(Sales[CustomerID]), CALCULATE(SUM(Sales[Amount]))), CALCULATE(SUM(Sales[Amount])))",
                description="Total revenue from top 10 customers",
            ),
            # 6.5: ALLSELECTED — percentage of filtered total
            DaxMeasure(
                name="Pct of Filtered Total",
                expression="DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALLSELECTED(Sales)), 0)",
                description="Sales as % of whatever the user has filtered to",
            ),
            # 6.6: CROSSFILTER / USERELATIONSHIP — alternate relationship
            DaxMeasure(
                name="Ship Date Sales",
                expression="CALCULATE(SUM(Sales[Amount]), USERELATIONSHIP(Sales[DateKey], Calendar[Date]))",
                description="Sales by ship date instead of order date",
            ),
            # 6.7: RANKX — product rank by revenue
            DaxMeasure(
                name="Product Revenue Rank",
                expression="RANKX(ALL(DimProduct), CALCULATE(SUM(Sales[Amount])))",
                description="Rank of each product by revenue",
            ),
            # 6.8: EARLIER / nested row context — should be unsupported
            DaxMeasure(
                name="Cumulative Pct",
                expression="COUNTROWS(FILTER(Sales, EARLIER(Sales[Amount]) >= Sales[Amount])) / COUNTROWS(Sales)",
                description="Cumulative percentage using EARLIER",
            ),
            # 6.9: CONCATENATEX — string aggregation
            DaxMeasure(
                name="Region List",
                expression='CONCATENATEX(VALUES(Sales[Region]), Sales[Region], ", ")',
                description="Comma-separated list of regions",
            ),
            # 6.10: Complex nested CALCULATE — multiple context transitions
            DaxMeasure(
                name="Category Contribution",
                expression="DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALL(DimProduct[ProductName]), ALL(DimProduct[ProductID])), 0)",
                description="Product's contribution to its category total",
            ),
        ],
    )
