"""Pydantic models for API request/response types."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ── DAX Input Models ─────────────────────────────────────────────────────────


class DaxColumn(BaseModel):
    name: str
    data_type: str = "STRING"


class DaxTable(BaseModel):
    name: str
    columns: list[DaxColumn] = Field(default_factory=list)
    databricks_table: Optional[str] = None


class DaxRelationship(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str


class DaxMeasure(BaseModel):
    name: str
    expression: str
    description: Optional[str] = None


class DaxModel(BaseModel):
    name: str
    fact_table: str
    tables: list[DaxTable]
    relationships: list[DaxRelationship] = Field(default_factory=list)
    measures: list[DaxMeasure]
    catalog: str = "main"
    schema_name: str = "default"

    def get_table(self, name: str) -> Optional[DaxTable]:
        for t in self.tables:
            if t.name == name:
                return t
        return None


# ── MDX Input Models ─────────────────────────────────────────────────────────


class MdxAttribute(BaseModel):
    name: str
    data_type: str = "STRING"


class MdxHierarchy(BaseModel):
    name: str
    levels: list[str] = Field(default_factory=list)


class MdxDimension(BaseModel):
    name: str
    attributes: list[MdxAttribute] = Field(default_factory=list)
    hierarchies: list[MdxHierarchy] = Field(default_factory=list)
    databricks_table: Optional[str] = None
    join_key: Optional[str] = None
    dim_key: Optional[str] = None


class MdxMeasureGroup(BaseModel):
    name: str
    databricks_table: str
    columns: list[MdxAttribute] = Field(default_factory=list)


class MdxCalculatedMember(BaseModel):
    name: str
    expression: str
    format_string: Optional[str] = None
    description: Optional[str] = None


class MdxCube(BaseModel):
    name: str
    measure_group: MdxMeasureGroup
    dimensions: list[MdxDimension] = Field(default_factory=list)
    calculated_members: list[MdxCalculatedMember] = Field(default_factory=list)
    catalog: str = "main"
    schema_name: str = "default"

    def get_dimension(self, name: str) -> Optional[MdxDimension]:
        for d in self.dimensions:
            if d.name == name:
                return d
        return None


# ── Translation Output Models ────────────────────────────────────────────────


class TranslationStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class TranslatedDimension(BaseModel):
    name: str
    expr: str
    comment: Optional[str] = None


class WindowSpec(BaseModel):
    order: str
    range: str
    semiadditive: str = "last"


class TranslatedMeasure(BaseModel):
    name: str
    expr: str
    comment: Optional[str] = None
    window: Optional[list[WindowSpec]] = None


class TranslatedJoin(BaseModel):
    name: str
    source: str
    on: Optional[str] = None
    using: Optional[list[str]] = None


class MeasureWarning(BaseModel):
    measure_name: str
    dax_expression: str
    warning_type: str
    message: str
    approximation: Optional[str] = None


class TranslationResult(BaseModel):
    status: TranslationStatus
    version: str = "1.1"
    source: str
    comment: Optional[str] = None
    joins: list[TranslatedJoin] = Field(default_factory=list)
    dimensions: list[TranslatedDimension] = Field(default_factory=list)
    measures: list[TranslatedMeasure] = Field(default_factory=list)
    warnings: list[MeasureWarning] = Field(default_factory=list)
    sql: str = ""
    yaml_body: str = ""


# ── API Request/Response Models ──────────────────────────────────────────────


class TranslateDaxRequest(BaseModel):
    model: DaxModel


class TranslateMdxRequest(BaseModel):
    cube: MdxCube


class DimensionTableInput(BaseModel):
    """A dimension table for star-schema joins."""
    table: str  # Fully qualified: catalog.schema.dim_table
    join_key: str = ""  # FK column in fact table
    dim_key: str = ""  # PK column in dimension table


class TranslateTextRequest(BaseModel):
    """Simple text-mode input — user pastes DAX/MDX measures as text."""

    measures_text: str
    source_table: str  # Fact table: catalog.schema.fact_table
    mode: str = "dax"
    dimension_tables: list[DimensionTableInput] = Field(default_factory=list)
    model: Optional[str] = None  # Override serving endpoint name


class TranslateResponse(BaseModel):
    status: str
    version: str
    source: str
    yaml_body: str
    sql: str
    dimensions: list[TranslatedDimension]
    measures: list[TranslatedMeasure]
    joins: list[TranslatedJoin]
    warnings: list[MeasureWarning]


class DeployRequest(BaseModel):
    sql: str
    catalog: str
    schema_name: str
    view_name: str


class DeployResponse(BaseModel):
    success: bool
    message: str
    full_view_name: str = ""


class QueryRequest(BaseModel):
    catalog: str
    schema_name: str
    view_name: str
    measures: list[str]
    group_by: list[str] = Field(default_factory=list)
    limit: int = 100


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[dict]
    row_count: int


class UserInfo(BaseModel):
    username: str
    display_name: str = ""


class HealthResponse(BaseModel):
    status: str
    warehouse_configured: bool
    endpoint_configured: bool


class AppConfig(BaseModel):
    audit_catalog: str = ""
    audit_schema: str = ""
    audit_table: str = ""
    serving_endpoint: str = ""
    dax_prompt_suffix: str = ""
    mdx_prompt_suffix: str = ""
