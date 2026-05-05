"""Level 3: Time intelligence — YTD, ParallelPeriod, LastPeriods, ClosingPeriod."""

from ..models import MdxAttribute, MdxCalculatedMember, MdxCube, MdxDimension, MdxHierarchy, MdxMeasureGroup

CATALOG = "main"
SCHEMA = "mdx_translator_test"


def get_cube() -> MdxCube:
    return MdxCube(
        name="level3_time_intel",
        catalog=CATALOG,
        schema_name=SCHEMA,
        measure_group=MdxMeasureGroup(
            name="Sales",
            databricks_table=f"{CATALOG}.{SCHEMA}.fact_sales",
            columns=[
                MdxAttribute(name="OrderID", data_type="INT"),
                MdxAttribute(name="CustomerID", data_type="INT"),
                MdxAttribute(name="DateKey", data_type="DATE"),
                MdxAttribute(name="Amount", data_type="DECIMAL"),
                MdxAttribute(name="Status", data_type="STRING"),
            ],
        ),
        dimensions=[
            MdxDimension(
                name="Date",
                databricks_table=f"{CATALOG}.{SCHEMA}.dim_date",
                join_key="date_key", dim_key="date",
                attributes=[
                    MdxAttribute(name="Date", data_type="DATE"),
                    MdxAttribute(name="Year", data_type="INT"),
                    MdxAttribute(name="Month", data_type="INT"),
                    MdxAttribute(name="Quarter"),
                ],
                hierarchies=[
                    MdxHierarchy(name="Calendar", levels=["Year", "Quarter", "Month", "Date"]),
                ],
            ),
        ],
        calculated_members=[
            # 3.1: Year-to-Date using PeriodsToDate
            MdxCalculatedMember(
                name="YTD Sales",
                expression="AGGREGATE(PeriodsToDate([Date].[Calendar].[Calendar Year], [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])",
                description="Year-to-date sales using PeriodsToDate",
            ),
            # 3.2: Prior year same period using ParallelPeriod
            MdxCalculatedMember(
                name="Prior Year Sales",
                expression="(ParallelPeriod([Date].[Calendar].[Calendar Year], 1, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])",
                format_string="$#,#.00",
                description="Sales for the same period one year ago",
            ),
            # 3.3: 6-month rolling average using LastPeriods
            MdxCalculatedMember(
                name="Rolling 6M Avg",
                expression="AVG(LastPeriods(6, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])",
                description="Average sales over last 6 periods",
            ),
            # 3.4: YoY Growth percentage
            MdxCalculatedMember(
                name="YoY Growth",
                expression="IIF((ParallelPeriod([Date].[Calendar].[Calendar Year], 1, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount]) > 0, ([Measures].[Sales Amount] - (ParallelPeriod([Date].[Calendar].[Calendar Year], 1, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])) / (ParallelPeriod([Date].[Calendar].[Calendar Year], 1, [Date].[Calendar].CurrentMember), [Measures].[Sales Amount]), NULL)",
                format_string="Percent",
                description="Year-over-year growth rate",
            ),
            # 3.5: Closing balance (semi-additive)
            MdxCalculatedMember(
                name="Closing Balance",
                expression="(ClosingPeriod([Date].[Calendar].[Month], [Date].[Calendar].CurrentMember), [Measures].[Sales Amount])",
                description="Value at the closing period of the month",
            ),
        ],
    )
