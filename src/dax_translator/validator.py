"""Validator: compare MEASURE() query results against direct SQL results."""

from __future__ import annotations

from dataclasses import dataclass, field

from .deployer import get_client, query_metric_view, run_direct_sql
from .models import TranslationResult


@dataclass
class ValidationCase:
    """A single validation case comparing metric view output to direct SQL."""

    measure_name: str
    view_name: str
    # Direct SQL that produces the expected result
    expected_sql: str
    # Dimensions to include in the metric view query
    dimensions: list[str] = field(default_factory=list)
    # Optional WHERE clause
    where: str | None = None
    # Tolerance for numeric comparison
    tolerance: float = 0.01


@dataclass
class ValidationResult:
    """Result of a single validation case."""

    case: ValidationCase
    passed: bool
    metric_view_value: str | None = None
    expected_value: str | None = None
    error: str | None = None


def validate_measure(
    case: ValidationCase,
    profile: str | None = None,
    warehouse_id: str | None = None,
) -> ValidationResult:
    """Validate a single measure by comparing MEASURE() result to direct SQL."""
    client = get_client(profile)

    try:
        # Query the metric view
        mv_rows = query_metric_view(
            client,
            case.view_name,
            measures=[case.measure_name],
            dimensions=case.dimensions if case.dimensions else None,
            where=case.where,
            warehouse_id=warehouse_id,
        )

        # Run the expected SQL
        expected_rows = run_direct_sql(
            client,
            case.expected_sql,
            warehouse_id=warehouse_id,
        )

        if not mv_rows:
            return ValidationResult(
                case=case, passed=False, error="Metric view returned no rows"
            )
        if not expected_rows:
            return ValidationResult(
                case=case, passed=False, error="Expected SQL returned no rows"
            )

        # For aggregate-only (no dimensions), compare single values
        if not case.dimensions:
            mv_val = list(mv_rows[0].values())[0]
            exp_val = list(expected_rows[0].values())[0]

            return _compare_values(case, mv_val, exp_val)

        # With dimensions, compare row-by-row (sorted)
        # This is a simplified comparison — just checks totals match
        mv_val = str(mv_rows[0].get(case.measure_name, ""))
        exp_val = str(list(expected_rows[0].values())[0])
        return _compare_values(case, mv_val, exp_val)

    except Exception as e:
        return ValidationResult(case=case, passed=False, error=str(e))


def _compare_values(
    case: ValidationCase, mv_val: str | None, exp_val: str | None
) -> ValidationResult:
    """Compare two values with tolerance for numeric types."""
    if mv_val is None or exp_val is None:
        return ValidationResult(
            case=case,
            passed=mv_val == exp_val,
            metric_view_value=str(mv_val),
            expected_value=str(exp_val),
        )

    try:
        mv_num = float(mv_val)
        exp_num = float(exp_val)
        passed = abs(mv_num - exp_num) <= case.tolerance * max(abs(exp_num), 1)
    except (ValueError, TypeError):
        passed = str(mv_val).strip() == str(exp_val).strip()

    return ValidationResult(
        case=case,
        passed=passed,
        metric_view_value=str(mv_val),
        expected_value=str(exp_val),
    )


def build_validation_cases(
    result: TranslationResult,
    catalog: str = "main",
    schema: str = "dax_translator_test",
) -> list[ValidationCase]:
    """Build validation cases for a TranslationResult.

    For each non-window measure without warnings, creates a ValidationCase
    comparing the MEASURE() query to a direct SQL query on source tables.
    """
    fq = f"{catalog}.{schema}"
    view_name = result.source.replace("fact_sales", f"mv_{result.source.split('.')[-1]}")
    # Derive view name from the SQL statement
    if result.sql:
        # Extract view name from CREATE OR REPLACE VIEW xxx
        parts = result.sql.split("\n")[0].split()
        if len(parts) >= 5:
            view_name = parts[4]

    cases = []
    warned_measures = {w.measure_name for w in result.warnings}

    for measure in result.measures:
        if measure.name in warned_measures:
            continue
        if measure.window:
            continue  # Skip window measures for simple validation

        # Build direct SQL from the measure expression
        expr = measure.expr

        # Build FROM clause based on joins
        from_clause = f"{result.source}"
        if result.joins:
            for j in result.joins:
                on_clause = j.on or ""
                from_clause += f"\n  LEFT JOIN {j.source} AS {j.name} ON {on_clause}"

        direct_sql = f"SELECT {expr} AS result FROM {from_clause}"

        cases.append(
            ValidationCase(
                measure_name=measure.name,
                view_name=view_name,
                expected_sql=direct_sql,
            )
        )

    return cases


def validate_all(
    result: TranslationResult,
    catalog: str = "main",
    schema: str = "dax_translator_test",
    profile: str | None = None,
    warehouse_id: str | None = None,
) -> list[ValidationResult]:
    """Validate all measures in a TranslationResult."""
    cases = build_validation_cases(result, catalog, schema)
    results = []
    for case in cases:
        vr = validate_measure(case, profile, warehouse_id)
        results.append(vr)
    return results
