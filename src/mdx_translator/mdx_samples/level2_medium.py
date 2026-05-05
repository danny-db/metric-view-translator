"""Level 2: Medium MDX — IIF conditions, dimension joins, tuple filters."""

from ..models import MdxAttribute, MdxCalculatedMember, MdxCube, MdxDimension, MdxHierarchy, MdxMeasureGroup

CATALOG = "main"
SCHEMA = "mdx_translator_test"


def get_cube() -> MdxCube:
    return MdxCube(
        name="level2_medium",
        catalog=CATALOG,
        schema_name=SCHEMA,
        measure_group=MdxMeasureGroup(
            name="Sales",
            databricks_table=f"{CATALOG}.{SCHEMA}.fact_sales",
            columns=[
                MdxAttribute(name="OrderID", data_type="INT"),
                MdxAttribute(name="CustomerID", data_type="INT"),
                MdxAttribute(name="ProductID", data_type="INT"),
                MdxAttribute(name="Amount", data_type="DECIMAL"),
                MdxAttribute(name="Quantity", data_type="INT"),
                MdxAttribute(name="UnitPrice", data_type="DECIMAL"),
                MdxAttribute(name="Profit", data_type="DECIMAL"),
                MdxAttribute(name="Revenue", data_type="DECIMAL"),
                MdxAttribute(name="Status", data_type="STRING"),
                MdxAttribute(name="Region", data_type="STRING"),
                MdxAttribute(name="Tier", data_type="STRING"),
            ],
        ),
        dimensions=[
            MdxDimension(
                name="Customer",
                databricks_table=f"{CATALOG}.{SCHEMA}.dim_customer",
                join_key="customer_id", dim_key="customer_id",
                attributes=[
                    MdxAttribute(name="CustomerID", data_type="INT"),
                    MdxAttribute(name="Name"), MdxAttribute(name="Segment"), MdxAttribute(name="Country"),
                ],
            ),
            MdxDimension(
                name="Product",
                databricks_table=f"{CATALOG}.{SCHEMA}.dim_product",
                join_key="product_id", dim_key="product_id",
                attributes=[
                    MdxAttribute(name="ProductID", data_type="INT"),
                    MdxAttribute(name="ProductName"), MdxAttribute(name="Category"),
                ],
            ),
        ],
        calculated_members=[
            # 2.1: Active-only sales (tuple filter)
            MdxCalculatedMember(
                name="Active Sales",
                expression="([Status].&[Active], [Measures].[Sales Amount])",
                description="Sales filtered to Status=Active using tuple notation",
            ),
            # 2.2: Profit margin with IIF
            MdxCalculatedMember(
                name="Profit Margin",
                expression="IIF([Measures].[Revenue] > 0, [Measures].[Profit] / [Measures].[Revenue], 0)",
                format_string="Percent",
                description="Profit divided by Revenue, 0 if no revenue",
            ),
            # 2.3: Line total (unit price * quantity)
            MdxCalculatedMember(
                name="Line Total",
                expression="[Measures].[Unit Price] * [Measures].[Quantity]",
                description="Revenue calculated as UnitPrice * Quantity (SUM of product)",
            ),
            # 2.4: Contribution ratio
            MdxCalculatedMember(
                name="Sales Contribution",
                expression="[Measures].[Sales Amount] / ([Measures].[Sales Amount], [Product].[Category].Parent)",
                description="Sales as percentage of parent category",
            ),
        ],
    )
