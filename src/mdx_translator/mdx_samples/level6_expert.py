"""Level 6: Expert MDX — pushing limits with RANK, TopCount, Generate, complex SCOPE patterns."""

from ..models import MdxAttribute, MdxCalculatedMember, MdxCube, MdxDimension, MdxHierarchy, MdxMeasureGroup

CATALOG = "main"
SCHEMA = "mdx_translator_test"


def get_cube() -> MdxCube:
    return MdxCube(
        name="level6_expert",
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
            MdxDimension(name="Customer", databricks_table=f"{CATALOG}.{SCHEMA}.dim_customer",
                        join_key="customer_id", dim_key="customer_id",
                        attributes=[MdxAttribute(name="CustomerID", data_type="INT"),
                                    MdxAttribute(name="Name"), MdxAttribute(name="Segment")]),
            MdxDimension(name="Product", databricks_table=f"{CATALOG}.{SCHEMA}.dim_product",
                        join_key="product_id", dim_key="product_id",
                        attributes=[MdxAttribute(name="ProductID", data_type="INT"),
                                    MdxAttribute(name="ProductName"), MdxAttribute(name="Category")]),
            MdxDimension(name="Date", databricks_table=f"{CATALOG}.{SCHEMA}.dim_date",
                        join_key="date_key", dim_key="date",
                        attributes=[MdxAttribute(name="Date", data_type="DATE"),
                                    MdxAttribute(name="Year", data_type="INT")],
                        hierarchies=[MdxHierarchy(name="Calendar", levels=["Year", "Date"])]),
        ],
        calculated_members=[
            # 6.1: RANK — product ranking
            MdxCalculatedMember(
                name="Product Rank",
                expression="RANK([Product].[ProductName].CurrentMember, ORDER([Product].[ProductName].Members, [Measures].[Sales Amount], BDESC))",
                description="Rank of current product by sales descending",
            ),
            # 6.2: TopCount — top 5 customers revenue
            MdxCalculatedMember(
                name="Top 5 Customer Revenue",
                expression="SUM(TopCount([Customer].[Name].Members, 5, [Measures].[Sales Amount]), [Measures].[Sales Amount])",
                description="Total revenue from top 5 customers",
            ),
            # 6.3: Generate — cross-join pattern
            MdxCalculatedMember(
                name="Multi Region Sales",
                expression="SUM(Generate([Region].Members, {[Region].CurrentMember}), [Measures].[Sales Amount])",
                description="Sales across generated region set",
            ),
            # 6.4: Distinct count with filter
            MdxCalculatedMember(
                name="Active Customer Count",
                expression="COUNT(FILTER([Customer].[Name].Members, ([Status].&[Active], [Measures].[Sales Amount]) > 0))",
                description="Count of customers with active sales > 0",
            ),
            # 6.5: YTD + ParallelPeriod combined — YTD vs Prior Year YTD
            MdxCalculatedMember(
                name="YTD vs PY YTD",
                expression="SUM(PeriodsToDate([Date].[Calendar].[Calendar Year]), [Measures].[Sales Amount]) - SUM(PeriodsToDate([Date].[Calendar].[Calendar Year]), (ParallelPeriod([Date].[Calendar].[Calendar Year], 1), [Measures].[Sales Amount]))",
                description="YTD sales minus prior year YTD sales",
            ),
            # 6.6: Unary operator / custom rollup — unsupported
            MdxCalculatedMember(
                name="Custom Rollup",
                expression="[Measures].[Sales Amount] * [Product].[Category].CurrentMember.Properties('UNARY_OPERATOR')",
                description="Custom rollup using unary operator property",
            ),
            # 6.7: String expression on member name
            MdxCalculatedMember(
                name="Region Label",
                expression="[Region].CurrentMember.Name + ' - ' + FORMAT([Measures].[Sales Amount], '$#,#')",
                description="String concatenation of region name and formatted sales",
            ),
            # 6.8: Nested calculated member referencing other calc members
            MdxCalculatedMember(
                name="Profit per Customer",
                expression="IIF([Measures].[Distinct Customer Count] > 0, [Measures].[Profit] / [Measures].[Distinct Customer Count], 0)",
                description="Profit divided by distinct customer count",
            ),
        ],
    )
