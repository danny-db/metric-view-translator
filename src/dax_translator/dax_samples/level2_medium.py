"""Level 2: Medium complexity — CALCULATE filters, SUMX, RELATED, IF dimensions."""

from ..models import DaxColumn, DaxMeasure, DaxModel, DaxRelationship, DaxTable

CATALOG = "main"
SCHEMA = "dax_translator_test"


def get_model() -> DaxModel:
    return DaxModel(
        name="level2_medium",
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
                name="DimCustomer",
                databricks_table=f"{CATALOG}.{SCHEMA}.dim_customer",
                columns=[
                    DaxColumn(name="CustomerID", data_type="INT"),
                    DaxColumn(name="Name", data_type="STRING"),
                    DaxColumn(name="Segment", data_type="STRING"),
                    DaxColumn(name="Country", data_type="STRING"),
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
        ],
        measures=[
            DaxMeasure(
                name="Active Sales",
                expression='CALCULATE(SUM(Sales[Amount]), Sales[Status]="Active")',
                description="Sales amount for active orders only",
            ),
            DaxMeasure(
                name="Active West Sales",
                expression='CALCULATE(SUM(Sales[Amount]), Sales[Status]="Active", Sales[Region]="West")',
                description="Active sales in West region",
            ),
            DaxMeasure(
                name="Line Total",
                expression="SUMX(Sales, Sales[Quantity]*Sales[UnitPrice])",
                description="Sum of quantity times unit price",
            ),
        ],
    )


# Expected expressions (approximate — Claude may vary on exact formatting)
EXPECTED_MEASURES = {
    "Active Sales": "SUM(amount) FILTER (WHERE status = 'Active')",
    "Active West Sales": "SUM(amount) FILTER (WHERE status = 'Active' AND region = 'West')",
    "Line Total": "SUM(quantity * unit_price)",
}

# Expected dimensions from IF and RELATED
EXPECTED_DIMENSIONS = {
    "Order Size": "CASE WHEN amount > 1000 THEN 'Large' ELSE 'Small' END",
    "Customer Name": "customer.name",
}
