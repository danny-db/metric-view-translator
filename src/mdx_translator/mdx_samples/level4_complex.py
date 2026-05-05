"""Level 4: Complex — multi-dimension star schema, composite calculated members, FILTER."""

from ..models import MdxAttribute, MdxCalculatedMember, MdxCube, MdxDimension, MdxHierarchy, MdxMeasureGroup

CATALOG = "main"
SCHEMA = "mdx_translator_test"


def get_cube() -> MdxCube:
    return MdxCube(
        name="level4_complex",
        catalog=CATALOG,
        schema_name=SCHEMA,
        measure_group=MdxMeasureGroup(
            name="Sales",
            databricks_table=f"{CATALOG}.{SCHEMA}.fact_sales",
            columns=[
                MdxAttribute(name="OrderID", data_type="INT"),
                MdxAttribute(name="CustomerID", data_type="INT"),
                MdxAttribute(name="ProductID", data_type="INT"),
                MdxAttribute(name="DateKey", data_type="DATE"),
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
            MdxDimension(
                name="Date",
                databricks_table=f"{CATALOG}.{SCHEMA}.dim_date",
                join_key="date_key", dim_key="date",
                attributes=[
                    MdxAttribute(name="Date", data_type="DATE"),
                    MdxAttribute(name="Year", data_type="INT"),
                    MdxAttribute(name="Month", data_type="INT"),
                ],
                hierarchies=[MdxHierarchy(name="Calendar", levels=["Year", "Month", "Date"])],
            ),
        ],
        calculated_members=[
            # 4.1: Revenue per customer (composite)
            MdxCalculatedMember(
                name="Revenue per Customer",
                expression="[Measures].[Sales Amount] / [Measures].[Distinct Customer Count]",
                format_string="$#,#.00",
                description="Total sales divided by distinct customer count",
            ),
            # 4.2: Premium active sales (multi-filter)
            MdxCalculatedMember(
                name="Premium Active Sales",
                expression="([Status].&[Active], [Tier].&[Premium], [Measures].[Sales Amount])",
                description="Sales for Active status AND Premium tier",
            ),
            # 4.3: Premium ratio
            MdxCalculatedMember(
                name="Premium Ratio",
                expression="IIF([Measures].[Sales Amount] > 0, ([Status].&[Active], [Tier].&[Premium], [Measures].[Sales Amount]) / [Measures].[Sales Amount], 0)",
                format_string="Percent",
                description="Premium active sales as % of total",
            ),
            # 4.4: Gross margin
            MdxCalculatedMember(
                name="Gross Margin",
                expression="IIF([Measures].[Revenue] > 0, ([Measures].[Revenue] - [Measures].[Sales Amount]) / [Measures].[Revenue], 0)",
                format_string="Percent",
                description="Gross margin percentage",
            ),
            # 4.5: Weighted average price
            MdxCalculatedMember(
                name="Weighted Avg Price",
                expression="[Measures].[Line Total] / [Measures].[Total Quantity]",
                description="Weighted average price = SUM(UnitPrice*Qty) / SUM(Qty)",
            ),
        ],
    )
