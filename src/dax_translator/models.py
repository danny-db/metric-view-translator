"""Pydantic models for DAX input and Metric View translation output."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


# ── DAX Input Models ──────────────────────────────────────────────────────────


class DaxColumn(BaseModel):
    """A column in a DAX table."""

    name: str
    data_type: str = "STRING"


class DaxTable(BaseModel):
    """A table referenced in the DAX model (fact or dimension)."""

    name: str
    columns: list[DaxColumn] = Field(default_factory=list)
    databricks_table: Optional[str] = None  # fully qualified: catalog.schema.table


class DaxRelationship(BaseModel):
    """A relationship between two DAX tables (for star/snowflake joins)."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str


class DaxMeasure(BaseModel):
    """A single DAX measure to translate."""

    name: str
    expression: str
    description: Optional[str] = None


class DaxModel(BaseModel):
    """Complete DAX model containing tables, relationships, and measures."""

    name: str
    fact_table: str  # name of the primary fact table
    tables: list[DaxTable]
    relationships: list[DaxRelationship] = Field(default_factory=list)
    measures: list[DaxMeasure]
    # Target Databricks location
    catalog: str = "main"
    schema_name: str = "dax_translator_test"

    def get_table(self, name: str) -> Optional[DaxTable]:
        for t in self.tables:
            if t.name == name:
                return t
        return None


# ── Translation Output Models ─────────────────────────────────────────────────


class TranslationStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"  # some measures had warnings
    FAILED = "failed"


class TranslatedDimension(BaseModel):
    """A dimension in the generated Metric View."""

    name: str
    expr: str
    comment: Optional[str] = None


class WindowSpec(BaseModel):
    """Window specification for window measures."""

    order: str
    range: str
    semiadditive: str = "last"


class TranslatedMeasure(BaseModel):
    """A measure in the generated Metric View."""

    name: str
    expr: str
    comment: Optional[str] = None
    window: Optional[list[WindowSpec]] = None


class TranslatedJoin(BaseModel):
    """A join in the generated Metric View."""

    name: str
    source: str
    on: Optional[str] = None
    using: Optional[list[str]] = None


class MeasureWarning(BaseModel):
    """Warning for a DAX measure that couldn't be fully translated."""

    measure_name: str
    dax_expression: str
    warning_type: str  # e.g. "unsupported_dax", "approximation", "time_intelligence"
    message: str
    approximation: Optional[str] = None  # best-effort SQL if available


class TranslationResult(BaseModel):
    """Complete result of translating a DAX model to a Metric View."""

    status: TranslationStatus
    version: str = "1.1"  # "0.1" if window measures needed
    source: str  # fully qualified source table
    comment: Optional[str] = None
    joins: list[TranslatedJoin] = Field(default_factory=list)
    dimensions: list[TranslatedDimension] = Field(default_factory=list)
    measures: list[TranslatedMeasure] = Field(default_factory=list)
    warnings: list[MeasureWarning] = Field(default_factory=list)
    sql: str = ""  # full CREATE VIEW ... WITH METRICS statement
    yaml_body: str = ""  # just the YAML portion

    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    @property
    def has_window_measures(self) -> bool:
        return any(m.window for m in self.measures)
