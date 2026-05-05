"""Level 4: Very complex — Composite measures, multi-table star schema, filtered ratios."""

from ..models import DaxColumn, DaxMeasure, DaxModel, DaxRelationship, DaxTable

CATALOG = "main"
SCHEMA = "dax_translator_test"


def get_model() -> DaxModel:
    return DaxModel(
        name="level4_very_complex",
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
            DaxRelationship(
                from_table="Sales",
                from_column="CustomerID",
                to_table="DimCustomer",
                to_column="CustomerID",
            ),
            DaxRelationship(
                from_table="Sales",
                from_column="ProductID",
                to_table="DimProduct",
                to_column="ProductID",
            ),
            DaxRelationship(
                from_table="Sales",
                from_column="DateKey",
                to_table="Calendar",
                to_column="Date",
            ),
        ],
        measures=[
            # 4.1: Composite ratio — Total Sales / Unique Customers
            DaxMeasure(
                name="Sales per Customer",
                expression="[Total Sales] / DISTINCTCOUNT(Sales[CustomerID])",
                description="Average sales per unique customer (composite measure)",
            ),
            # 4.2: Filtered composite — Premium Sales + ratio
            DaxMeasure(
                name="Premium Sales",
                expression='CALCULATE([Total Sales], Sales[Status]="Active", Sales[Tier]="Premium")',
                description="Total sales for active premium customers",
            ),
            DaxMeasure(
                name="Premium Ratio",
                expression="DIVIDE([Premium Sales], [Total Sales], 0)",
                description="Ratio of premium sales to total sales",
            ),
            # 4.3: Safe division — profit margin
            DaxMeasure(
                name="Profit Margin",
                expression="DIVIDE(SUM(Sales[Profit]), SUM(Sales[Revenue]), 0)",
                description="Profit as a percentage of revenue",
            ),
            # 4.4: Multi-table context — needs all 3 dimension joins
            DaxMeasure(
                name="Total Sales",
                expression="SUM(Sales[Amount])",
                description="Base total sales measure",
            ),
        ],
    )


# Expected Metric View expressions
EXPECTED_MEASURES = {
    "Sales per Customer": "SUM(amount) / COUNT(DISTINCT customer_id)",
    "Premium Sales": "SUM(amount) FILTER (WHERE status = 'Active' AND tier = 'Premium')",
    "Profit Margin": "SUM(profit) / NULLIF(SUM(revenue), 0)",
    "Total Sales": "SUM(amount)",
}

# Premium Ratio should inline both measures:
# SUM(amount) FILTER (WHERE status = 'Active' AND tier = 'Premium') / NULLIF(SUM(amount), 0)
EXPECTED_JOINS = ["customer", "product", "calendar"]
