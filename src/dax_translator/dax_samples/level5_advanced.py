"""Level 5: Advanced business logic — % of total, ALLEXCEPT, SWITCH, VAR/RETURN, running totals."""

from ..models import DaxColumn, DaxMeasure, DaxModel, DaxRelationship, DaxTable

CATALOG = "main"
SCHEMA = "dax_translator_test"


def get_model() -> DaxModel:
    return DaxModel(
        name="level5_advanced",
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
                    DaxColumn(name="Cost", data_type="DECIMAL"),
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
            # 5.1: Percentage of Grand Total — CALCULATE + ALL removes all filters
            DaxMeasure(
                name="Pct of Grand Total",
                expression="DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALL(Sales)), 0)",
                description="Each row's sales as percentage of grand total",
            ),
            # 5.2: Percentage of Category Total — ALLEXCEPT keeps category filter
            DaxMeasure(
                name="Pct of Category",
                expression="DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALLEXCEPT(Sales, DimProduct[Category])), 0)",
                description="Sales as percentage of category total",
            ),
            # 5.3: SWITCH-based tier classification (as calculated measure with COUNTROWS)
            DaxMeasure(
                name="Premium Count",
                expression='CALCULATE(COUNTROWS(Sales), Sales[Amount] > 1000, Sales[Status]="Active")',
                description="Count of active high-value orders",
            ),
            # 5.4: Profit Margin with VAR/RETURN pattern
            DaxMeasure(
                name="Net Margin",
                expression="VAR TotalRevenue = SUM(Sales[Revenue]) VAR TotalCost = SUM(Sales[Profit]) RETURN IF(TotalRevenue > 0, TotalCost / TotalRevenue, 0)",
                description="Profit margin using VAR/RETURN",
            ),
            # 5.5: Cumulative sum / running total via CALCULATE + FILTER + ALL
            DaxMeasure(
                name="Running Total",
                expression="CALCULATE(SUM(Sales[Amount]), FILTER(ALL(Calendar), Calendar[Date] <= MAX(Calendar[Date])))",
                description="Running total of sales up to current date",
            ),
            # 5.6: Year-over-Year Growth %
            DaxMeasure(
                name="YoY Growth",
                expression="VAR CurrentYear = SUM(Sales[Amount]) VAR LastYear = CALCULATE(SUM(Sales[Amount]), SAMEPERIODLASTYEAR(Calendar[Date])) RETURN DIVIDE(CurrentYear - LastYear, LastYear, 0)",
                description="Year-over-year growth percentage",
            ),
            # 5.7: Count distinct with filter — active customers only
            DaxMeasure(
                name="Active Customers",
                expression='CALCULATE(DISTINCTCOUNT(Sales[CustomerID]), Sales[Status]="Active")',
                description="Count of distinct active customers",
            ),
            # 5.8: Weighted average (SUMX pattern)
            DaxMeasure(
                name="Weighted Avg Price",
                expression="DIVIDE(SUMX(Sales, Sales[UnitPrice] * Sales[Quantity]), SUM(Sales[Quantity]), 0)",
                description="Quantity-weighted average price",
            ),
        ],
    )
