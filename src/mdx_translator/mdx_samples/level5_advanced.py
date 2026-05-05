"""Level 5: Advanced — SCOPE-like patterns, KPI expressions, complex IIF nesting."""

from ..models import MdxAttribute, MdxCalculatedMember, MdxCube, MdxDimension, MdxHierarchy, MdxMeasureGroup

CATALOG = "main"
SCHEMA = "mdx_translator_test"


def get_cube() -> MdxCube:
    return MdxCube(
        name="level5_advanced",
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
                                    MdxAttribute(name="Year", data_type="INT"),
                                    MdxAttribute(name="Month", data_type="INT")],
                        hierarchies=[MdxHierarchy(name="Calendar", levels=["Year", "Month", "Date"])]),
        ],
        calculated_members=[
            # 5.1: Nested IIF — traffic light KPI
            MdxCalculatedMember(
                name="Sales KPI Status",
                expression='IIF([Measures].[Sales Amount] > 3000, "Green", IIF([Measures].[Sales Amount] > 1000, "Yellow", "Red"))',
                description="Traffic light: Green > 3000, Yellow > 1000, Red otherwise",
            ),
            # 5.2: FILTER-based conditional count
            MdxCalculatedMember(
                name="High Value Order Count",
                expression="COUNT(FILTER([Status].Members, [Measures].[Sales Amount] > 1000))",
                description="Count of status members where sales exceed 1000",
            ),
            # 5.3: Cumulative total using SUM + NULL:CurrentMember
            MdxCalculatedMember(
                name="Cumulative Sales",
                expression="SUM(NULL:[Date].[Calendar].CurrentMember, [Measures].[Sales Amount])",
                description="Running total from beginning to current date member",
            ),
            # 5.4: Percentage of total (root member)
            MdxCalculatedMember(
                name="Pct of Total",
                expression="[Measures].[Sales Amount] / ([Measures].[Sales Amount], [Product].[Category].[All])",
                description="Sales as percentage of grand total across all products",
            ),
            # 5.5: Moving average (3-month)
            MdxCalculatedMember(
                name="3M Moving Avg",
                expression="AVG(LastPeriods(3, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])",
                description="3-month moving average of sales",
            ),
            # 5.6: Max single order amount
            MdxCalculatedMember(
                name="Max Order",
                expression="MAX([Customer].[Name].Members, [Measures].[Sales Amount])",
                description="Maximum sales amount across all customers",
            ),
        ],
    )
