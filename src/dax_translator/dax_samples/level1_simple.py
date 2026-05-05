"""Level 1: Simple aggregations on a single fact_sales table."""

from ..models import DaxColumn, DaxMeasure, DaxModel, DaxTable

CATALOG = "main"
SCHEMA = "dax_translator_test"


def get_model() -> DaxModel:
    return DaxModel(
        name="level1_simple",
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
        ],
        measures=[
            DaxMeasure(
                name="Total Sales",
                expression="SUM(Sales[Amount])",
                description="Sum of all sales amounts",
            ),
            DaxMeasure(
                name="Order Count",
                expression="COUNT(Sales[OrderID])",
                description="Count of orders",
            ),
            DaxMeasure(
                name="Unique Customers",
                expression="DISTINCTCOUNT(Sales[CustomerID])",
                description="Count of unique customers",
            ),
            DaxMeasure(
                name="Avg Order",
                expression="AVERAGE(Sales[Amount])",
                description="Average order amount",
            ),
        ],
    )


# Expected Metric View expressions for validation
EXPECTED = {
    "Total Sales": "SUM(amount)",
    "Order Count": "COUNT(order_id)",
    "Unique Customers": "COUNT(DISTINCT customer_id)",
    "Avg Order": "AVG(amount)",
}
