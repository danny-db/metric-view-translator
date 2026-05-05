"""Level 1: Simple MDX base measures — SUM, COUNT, DistinctCount, AVG."""

from ..models import MdxAttribute, MdxCalculatedMember, MdxCube, MdxDimension, MdxMeasureGroup

CATALOG = "main"
SCHEMA = "mdx_translator_test"


def get_cube() -> MdxCube:
    return MdxCube(
        name="level1_simple",
        catalog=CATALOG,
        schema_name=SCHEMA,
        measure_group=MdxMeasureGroup(
            name="Sales",
            databricks_table=f"{CATALOG}.{SCHEMA}.fact_sales",
            columns=[
                MdxAttribute(name="OrderID", data_type="INT"),
                MdxAttribute(name="CustomerID", data_type="INT"),
                MdxAttribute(name="Amount", data_type="DECIMAL"),
                MdxAttribute(name="Quantity", data_type="INT"),
                MdxAttribute(name="UnitPrice", data_type="DECIMAL"),
                MdxAttribute(name="Status", data_type="STRING"),
                MdxAttribute(name="Region", data_type="STRING"),
            ],
        ),
        dimensions=[
            MdxDimension(name="Region", attributes=[MdxAttribute(name="Region")]),
            MdxDimension(name="Status", attributes=[MdxAttribute(name="Status")]),
        ],
        calculated_members=[
            MdxCalculatedMember(
                name="Total Sales",
                expression="[Measures].[Sales Amount]",
                description="Base SUM measure on Amount column (aggregation type: Sum)",
            ),
            MdxCalculatedMember(
                name="Order Count",
                expression="[Measures].[Order Count]",
                description="Base COUNT measure on OrderID column (aggregation type: Count)",
            ),
            MdxCalculatedMember(
                name="Unique Customers",
                expression="[Measures].[Distinct Customer Count]",
                description="DistinctCount measure on CustomerID (aggregation type: DistinctCount)",
            ),
            MdxCalculatedMember(
                name="Avg Order Value",
                expression="[Measures].[Sales Amount] / [Measures].[Order Count]",
                description="Average order value = Total Sales / Order Count",
            ),
        ],
    )
