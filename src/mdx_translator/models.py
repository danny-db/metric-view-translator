"""Pydantic models for MDX OLAP cube input and Metric View translation output.

Reuses TranslationResult and related output models from dax_translator.
"""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

# Reuse output models from DAX translator — same Metric View target format
from dax_translator.models import (  # noqa: F401
    MeasureWarning,
    TranslatedDimension,
    TranslatedJoin,
    TranslatedMeasure,
    TranslationResult,
    TranslationStatus,
    WindowSpec,
)


# ── MDX Input Models ──────────────────────────────────────────────────────────


class MdxAttribute(BaseModel):
    """An attribute (column) in an OLAP dimension."""

    name: str
    data_type: str = "STRING"


class MdxHierarchy(BaseModel):
    """A hierarchy within an OLAP dimension (e.g., Date → Year → Quarter → Month → Day)."""

    name: str
    levels: list[str] = Field(default_factory=list)  # e.g., ["Year", "Quarter", "Month", "Day"]


class MdxDimension(BaseModel):
    """An OLAP dimension (e.g., [Date], [Product], [Customer])."""

    name: str  # e.g., "Date", "Product" — MDX uses [Date].[Calendar] syntax
    attributes: list[MdxAttribute] = Field(default_factory=list)
    hierarchies: list[MdxHierarchy] = Field(default_factory=list)
    databricks_table: Optional[str] = None  # fully qualified dim table
    join_key: Optional[str] = None  # FK column in fact table
    dim_key: Optional[str] = None  # PK column in dim table


class MdxMeasureGroup(BaseModel):
    """A measure group (fact table) in the OLAP cube."""

    name: str
    databricks_table: str  # fully qualified fact table
    columns: list[MdxAttribute] = Field(default_factory=list)


class MdxCalculatedMember(BaseModel):
    """A calculated member / measure in the OLAP cube.

    MDX syntax examples:
      [Measures].[Total Sales]  — base measure (mapped to SUM aggregation)
      WITH MEMBER [Measures].[Profit Margin] AS [Measures].[Profit] / [Measures].[Revenue]
    """

    name: str  # Display name, e.g., "Total Sales"
    expression: str  # MDX expression
    format_string: Optional[str] = None  # e.g., "$#,#.00", "Percent"
    description: Optional[str] = None


class MdxCube(BaseModel):
    """Complete OLAP cube definition for translation."""

    name: str  # Cube name, e.g., "Adventure Works"
    measure_group: MdxMeasureGroup  # Primary fact table
    dimensions: list[MdxDimension] = Field(default_factory=list)
    calculated_members: list[MdxCalculatedMember] = Field(default_factory=list)
    # Target Databricks location
    catalog: str = "main"
    schema_name: str = "mdx_translator_test"

    def get_dimension(self, name: str) -> Optional[MdxDimension]:
        for d in self.dimensions:
            if d.name == name:
                return d
        return None
