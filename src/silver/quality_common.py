"""
Shared Silver quality helpers.

This module exists so completeness, uniqueness, and later modules use the same
row-level representation and cannot overwrite each other's failures.

Design constraints (frozen):
- Physical grain is the Bronze row. Identity is ``_ingest_row_id``.
- Business keys (customer_id, order_id, product_id) stay the keys being
  evaluated. ``_ingest_row_id`` is never a substitute for them.
- Bad rows are flagged, never deleted. No dropna / dropDuplicates / survivor
  selection lives here.
- Each module writes ``{module}_failed_checks`` (ARRAY<STRING>) and
  ``{module}_pass`` (BOOLEAN). Modules do **not** write last-writer
  ``quality_check_result``.
- Accumulating a later check concatenates codes (array_distinct). An earlier
  failure is never replaced by a later one.
- Metrics count physical rows, not distinct business keys.

Type, RI, and business-logic modules reuse these helpers. Shared column names,
code format, accumulation, combiner, and metrics stay the single quality
representation so later modules cannot overwrite earlier ones.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any, Sequence

_SILVER_DIR = Path(__file__).resolve().parent
_SRC_DIR = _SILVER_DIR.parent
_BRONZE_DIR = _SRC_DIR / "bronze"
for _path in (str(_SRC_DIR), str(_BRONZE_DIR), str(_SILVER_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from contracts import INGEST_ROW_ID_COLUMN  # noqa: E402

MODULE_COMPLETENESS = "completeness"
MODULE_UNIQUENESS = "uniqueness"
MODULE_TYPE = "type"
MODULE_RI = "ri"
MODULE_BUSINESS = "business"

# Pass-column names match data-model.md. Failed-check arrays are per-module so
# create_silver_tables.py can concat them later without last-writer-wins.
MODULE_PASS_COLUMNS: dict[str, str] = {
    MODULE_COMPLETENESS: "completeness_pass",
    MODULE_UNIQUENESS: "uniqueness_pass",
    MODULE_TYPE: "type_validation_pass",
    MODULE_RI: "referential_integrity_pass",
    MODULE_BUSINESS: "business_logic_pass",
}

MODULE_FAILED_CHECK_COLUMNS: dict[str, str] = {
    MODULE_COMPLETENESS: "completeness_failed_checks",
    MODULE_UNIQUENESS: "uniqueness_failed_checks",
    MODULE_TYPE: "type_failed_checks",
    MODULE_RI: "referential_integrity_failed_checks",
    MODULE_BUSINESS: "business_logic_failed_checks",
}

COMBINED_FAILED_CHECKS_COLUMN = "failed_checks"
QUALITY_CHECK_RESULT_COLUMN = "quality_check_result"
QUALITY_RESULT_PASS = "PASS"
QUALITY_RESULT_FAIL = "FAIL"

POPULATION_PHYSICAL_ROW = "physical_row"
POPULATION_DISTINCT_KEY = "distinct_key"
POPULATION_TABLE_OUTCOME = "table_outcome"

MODULE_ORDER: tuple[str, ...] = (
    MODULE_COMPLETENESS,
    MODULE_UNIQUENESS,
    MODULE_TYPE,
    MODULE_RI,
    MODULE_BUSINESS,
)

QUALITY_ATTRIBUTE_COLUMNS: frozenset[str] = frozenset(
    {
        *MODULE_PASS_COLUMNS.values(),
        *MODULE_FAILED_CHECK_COLUMNS.values(),
        COMBINED_FAILED_CHECKS_COLUMN,
        QUALITY_CHECK_RESULT_COLUMN,
    }
)

PCT_QUANTIZE = Decimal("0.0001")


class QualityError(RuntimeError):
    """Silver quality contract failure. Messages must not contain secrets."""


def _import_pyspark():
    try:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
    except ImportError as exc:
        raise QualityError(
            "PySpark is not installed. Silver quality checks require Spark. "
            "On Databricks, run this on a cluster with an active SparkSession. "
            "For local validation install PySpark and JDK 11 or 17. "
            "Do not treat a missing Spark runtime as a successful Databricks run."
        ) from exc
    return F, Window


def quality_code(module: str, table: str, rule: str) -> str:
    """Stable failed-check code, e.g. ``completeness:orders.customer_id``."""
    return f"{module}:{table}.{rule}"


def empty_string_array(F: Any) -> Any:
    """Typed empty ARRAY<STRING> (bare ``array()`` is ARRAY<NULL>)."""
    return F.array().cast("array<string>")


def require_ingest_row_id(dataframe: Any) -> None:
    if INGEST_ROW_ID_COLUMN not in dataframe.columns:
        raise QualityError(
            "Silver quality requires Bronze "
            f"{INGEST_ROW_ID_COLUMN} as physical-row identity. "
            "Do not substitute customer_id / order_id / product_id."
        )


def require_columns(dataframe: Any, columns: Sequence[str], *, context: str) -> None:
    missing = [name for name in columns if name not in dataframe.columns]
    if missing:
        raise QualityError(
            f"{context} is missing required columns {missing}. "
            f"Available: {list(dataframe.columns)}"
        )


def lineage_and_source_columns(dataframe: Any) -> list[str]:
    """Bronze source columns plus ``_ingest_row_id``; quality attributes excluded."""
    return [name for name in dataframe.columns if name not in QUALITY_ATTRIBUTE_COLUMNS]


def pass_column_name(module: str) -> str:
    try:
        return MODULE_PASS_COLUMNS[module]
    except KeyError as exc:
        raise QualityError(f"Unknown quality module {module!r}.") from exc


def failed_checks_column_name(module: str) -> str:
    try:
        return MODULE_FAILED_CHECK_COLUMNS[module]
    except KeyError as exc:
        raise QualityError(f"Unknown quality module {module!r}.") from exc


def codes_for_null_fields(
    table: str,
    fields: Sequence[str],
    *,
    module: str = MODULE_COMPLETENESS,
) -> Any:
    """
    One code per NULL field (completeness, or type parse-null on owned fields).

    Empty string is **not** NULL (Spark ``IS NULL`` only). Concatenating empty
    arrays for passing fields does not drop the row and does not invent codes.
    Callers choose ``module`` so completeness-critical NULLs are not emitted
    as ``type:`` codes.
    """
    F, _Window = _import_pyspark()
    if not fields:
        raise QualityError(f"No fields provided for {module} on {table}.")
    parts = []
    for field in fields:
        code = quality_code(module, table, field)
        parts.append(
            F.when(F.col(field).isNull(), F.array(F.lit(code))).otherwise(
                empty_string_array(F)
            )
        )
    if len(parts) == 1:
        return parts[0]
    return F.array_distinct(F.concat(*parts))


def codes_for_domain_violation(
    table: str,
    field: str,
    allowed_values: Sequence[str],
    *,
    module: str = MODULE_TYPE,
    rule: str | None = None,
) -> Any:
    """
    Closed-domain failure: non-null value not in the allowlist.

    NULL is not a domain violation (completeness or a typed-null rule owns it).
    Comparison is case-sensitive and does not trim. ``isin`` is used only on a
    tiny literal allowlist, never on collected dataset values.
    """
    F, _Window = _import_pyspark()
    if not allowed_values:
        raise QualityError(f"Empty allowlist for {module} on {table}.{field}.")
    code = quality_code(module, table, rule or f"{field}_domain")
    is_invalid = F.col(field).isNotNull() & (~F.col(field).isin(list(allowed_values)))
    return F.when(is_invalid, F.array(F.lit(code))).otherwise(empty_string_array(F))


def codes_for_orphan_fk(
    table: str,
    fk_field: str,
    parent_key_column: str,
    *,
    module: str = MODULE_RI,
    rule: str | None = None,
) -> Any:
    """
    Orphan FK: child key is non-null and the left-joined distinct parent key
    is null. NULL child FKs are not orphans (completeness owns them).
    """
    F, _Window = _import_pyspark()
    code = quality_code(module, table, rule or f"{fk_field}_orphan")
    is_orphan = F.col(fk_field).isNotNull() & F.col(parent_key_column).isNull()
    return F.when(is_orphan, F.array(F.lit(code))).otherwise(empty_string_array(F))


def concat_code_arrays(parts: Sequence[Any]) -> Any:
    """Distinct-concat of already-typed ARRAY<STRING> column expressions."""
    F, _Window = _import_pyspark()
    if not parts:
        return empty_string_array(F)
    if len(parts) == 1:
        return parts[0]
    return F.array_distinct(F.concat(*parts))


def codes_for_when(condition: Any, code: str) -> Any:
    """
    Emit ``code`` when ``condition`` is true; otherwise an empty ARRAY<STRING>.

    Callers must encode NULL-skip logic in ``condition``. This helper never
    rewrites source columns and never drops rows.
    """
    F, _Window = _import_pyspark()
    if not code:
        raise QualityError("codes_for_when requires a non-empty quality code.")
    return F.when(condition, F.array(F.lit(code))).otherwise(empty_string_array(F))


def uniqueness_codes_for_key(table: str, key: str, *, module: str = MODULE_UNIQUENESS) -> Any:
    """
    Flag **every** physical row whose non-null business key occurs more than once.

    NULL keys are not grouped as duplicates (completeness owns NULLs). The window
    still computes a partition count for NULLs, but those rows are not failed.
    """
    F, Window = _import_pyspark()
    window = Window.partitionBy(F.col(key))
    occurrences = F.count(F.lit(1)).over(window)
    is_duplicate_participant = F.col(key).isNotNull() & (occurrences > 1)
    code = quality_code(module, table, key)
    return F.when(is_duplicate_participant, F.array(F.lit(code))).otherwise(
        empty_string_array(F)
    )


def attach_module_result(dataframe: Any, module: str, new_codes: Any) -> Any:
    """
    Add or accumulate a module's failed-check array and boolean.

    If the module column already exists, codes are concatenated then
    ``array_distinct``-ed. This is the anti-overwrite rule: a later call cannot
    replace an earlier failure on the same physical row.
    """
    F, _Window = _import_pyspark()
    require_ingest_row_id(dataframe)
    failed_col = failed_checks_column_name(module)
    pass_col = pass_column_name(module)
    typed_new = F.coalesce(new_codes, empty_string_array(F))
    if failed_col in dataframe.columns:
        merged = F.array_distinct(
            F.concat(
                F.coalesce(F.col(failed_col), empty_string_array(F)),
                typed_new,
            )
        )
        result = dataframe.withColumn(failed_col, merged)
    else:
        result = dataframe.withColumn(failed_col, typed_new)
    return result.withColumn(pass_col, F.size(F.col(failed_col)) == 0)


def concatenate_failed_check_arrays(columns: Sequence[str]) -> Any:
    """Combiner helper: concat per-module arrays, then distinct. Does not overwrite."""
    F, _Window = _import_pyspark()
    if not columns:
        return empty_string_array(F)
    parts = [F.coalesce(F.col(name), empty_string_array(F)) for name in columns]
    if len(parts) == 1:
        return parts[0]
    return F.array_distinct(F.concat(*parts))


def combine_quality_status(dataframe: Any) -> Any:
    """
    Build combined ``failed_checks`` and ``quality_check_result``.

    Concatenates the five per-module arrays. Does not overwrite module booleans
    or module arrays. ``FAIL`` iff the combined array is non-empty.
    """
    F, _Window = _import_pyspark()
    require_ingest_row_id(dataframe)
    module_cols = [failed_checks_column_name(module) for module in MODULE_ORDER]
    require_columns(dataframe, module_cols, context="combine_quality_status")
    combined = concatenate_failed_check_arrays(module_cols)
    result = dataframe.withColumn(COMBINED_FAILED_CHECKS_COLUMN, combined)
    return result.withColumn(
        QUALITY_CHECK_RESULT_COLUMN,
        F.when(F.size(F.col(COMBINED_FAILED_CHECKS_COLUMN)) > 0, F.lit(QUALITY_RESULT_FAIL)).otherwise(
            F.lit(QUALITY_RESULT_PASS)
        ),
    )


@dataclass(frozen=True)
class CheckMetrics:
    """
    Metrics for one check.

    ``population_kind`` records the denominator:
    - ``physical_row``: every input row (default for field/module rules)
    - ``distinct_key``: distinct non-null business keys (uniqueness key counts)
    - ``table_outcome``: physical rows with combined ``quality_check_result``

    Percentages use ``total_evaluated`` for *this* check, not a mix of those
    populations. Empty inputs yield 0.0000 / 0.0000 (not an exception).
    Rule-level ``failed`` counts must not be summed and treated as distinct
    FAIL rows: one physical row can fail multiple rules.
    """

    table_name: str
    check_name: str
    total_evaluated: int
    passed: int
    failed: int
    pass_pct: Decimal
    fail_pct: Decimal
    expected_fail_count: int | None = None
    population_kind: str = POPULATION_PHYSICAL_ROW

    @property
    def pass_count(self) -> int:
        return self.passed

    @property
    def fail_count(self) -> int:
        return self.failed

    @property
    def pass_percentage(self) -> Decimal:
        return self.pass_pct

    @property
    def fail_percentage(self) -> Decimal:
        return self.fail_pct

    def with_expected(self, expected_fail_count: int | None) -> "CheckMetrics":
        if self.expected_fail_count == expected_fail_count:
            return self
        return CheckMetrics(
            table_name=self.table_name,
            check_name=self.check_name,
            total_evaluated=self.total_evaluated,
            passed=self.passed,
            failed=self.failed,
            pass_pct=self.pass_pct,
            fail_pct=self.fail_pct,
            expected_fail_count=expected_fail_count,
            population_kind=self.population_kind,
        )


def _ratio(part: int, total: int) -> Decimal:
    if total < 0:
        raise QualityError("Cannot compute quality percentages with a negative total_evaluated.")
    if total == 0:
        if part != 0:
            raise QualityError(
                f"Cannot compute quality percentages: total_evaluated=0 but part={part}."
            )
        return Decimal("0.0000")
    return (Decimal(part) / Decimal(total)).quantize(PCT_QUANTIZE, rounding=ROUND_HALF_EVEN)


def metrics_from_counts(
    *,
    table_name: str,
    check_name: str,
    total_evaluated: int,
    failed: int,
    expected_fail_count: int | None = None,
    population_kind: str = POPULATION_PHYSICAL_ROW,
) -> CheckMetrics:
    passed = total_evaluated - failed
    if failed < 0 or passed < 0:
        raise QualityError(
            f"Inconsistent metrics for {table_name}/{check_name}: "
            f"total={total_evaluated} failed={failed}"
        )
    return CheckMetrics(
        table_name=table_name,
        check_name=check_name,
        total_evaluated=total_evaluated,
        passed=passed,
        failed=failed,
        pass_pct=_ratio(passed, total_evaluated),
        fail_pct=_ratio(failed, total_evaluated),
        expected_fail_count=expected_fail_count,
        population_kind=population_kind,
    )


def collect_pass_fail_metrics(
    dataframe: Any,
    *,
    table_name: str,
    check_name: str,
    pass_column: str,
) -> CheckMetrics:
    """
    One-row aggregate. ``collect()`` is only this metrics row, never the dataset.

    total_evaluated is the physical validation population (all rows present).
    """
    F, _Window = _import_pyspark()
    require_columns(dataframe, [pass_column], context=f"metrics for {check_name}")
    row = dataframe.agg(
        F.count(F.lit(1)).alias("total_evaluated"),
        F.sum(F.when(F.col(pass_column), F.lit(1)).otherwise(F.lit(0))).alias("passed"),
        F.sum(F.when(~F.col(pass_column), F.lit(1)).otherwise(F.lit(0))).alias("failed"),
    ).collect()[0]
    total = int(row["total_evaluated"] or 0)
    failed = int(row["failed"] or 0)
    return metrics_from_counts(
        table_name=table_name,
        check_name=check_name,
        total_evaluated=total,
        failed=failed,
    )


def collect_code_fail_metrics(
    dataframe: Any,
    *,
    table_name: str,
    check_name: str,
    failed_checks_column: str,
    code: str,
) -> CheckMetrics:
    """Field/rule metrics: fail = physical rows whose module array contains ``code``."""
    F, _Window = _import_pyspark()
    require_columns(
        dataframe, [failed_checks_column], context=f"metrics for {check_name}"
    )
    row = dataframe.agg(
        F.count(F.lit(1)).alias("total_evaluated"),
        F.sum(
            F.when(
                F.array_contains(F.col(failed_checks_column), F.lit(code)),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("failed"),
    ).collect()[0]
    total = int(row["total_evaluated"] or 0)
    failed = int(row["failed"] or 0)
    return metrics_from_counts(
        table_name=table_name,
        check_name=check_name,
        total_evaluated=total,
        failed=failed,
    )


def collect_duplicate_key_count(dataframe: Any, key: str, pass_column: str) -> int:
    """
    Distinct non-null business keys that participate in a uniqueness failure.

    ``countDistinct`` ignores the nulls produced by ``when`` on passing rows, so
    this does not filter the validation dataset.
    """
    F, _Window = _import_pyspark()
    require_columns(dataframe, [key, pass_column], context="duplicate_key_count")
    failing_key = F.when(
        (~F.col(pass_column)) & F.col(key).isNotNull(),
        F.col(key),
    )
    row = dataframe.agg(F.countDistinct(failing_key).alias("duplicate_key_count")).collect()[0]
    return int(row["duplicate_key_count"] or 0)


def collect_distinct_key_metrics(
    dataframe: Any,
    *,
    table_name: str,
    check_name: str,
    key: str,
    pass_column: str,
    expected_fail_count: int | None = None,
) -> CheckMetrics:
    """
    Distinct-key uniqueness metrics.

    total_evaluated = distinct non-null business keys.
    failed = distinct keys that participate in a uniqueness failure.
    This denominator is *not* the physical row count.
    """
    F, _Window = _import_pyspark()
    require_columns(dataframe, [key, pass_column], context=f"key metrics for {check_name}")
    row = dataframe.agg(
        F.countDistinct(F.col(key)).alias("total_keys"),
        F.countDistinct(
            F.when((~F.col(pass_column)) & F.col(key).isNotNull(), F.col(key))
        ).alias("failed_keys"),
    ).collect()[0]
    return metrics_from_counts(
        table_name=table_name,
        check_name=check_name,
        total_evaluated=int(row["total_keys"] or 0),
        failed=int(row["failed_keys"] or 0),
        expected_fail_count=expected_fail_count,
        population_kind=POPULATION_DISTINCT_KEY,
    )


def collect_table_outcome_metrics(dataframe: Any, *, table_name: str) -> CheckMetrics:
    """Physical rows whose combined ``quality_check_result`` is FAIL."""
    F, _Window = _import_pyspark()
    require_columns(
        dataframe,
        [QUALITY_CHECK_RESULT_COLUMN],
        context=f"table outcome for {table_name}",
    )
    row = dataframe.agg(
        F.count(F.lit(1)).alias("total_evaluated"),
        F.sum(
            F.when(
                F.col(QUALITY_CHECK_RESULT_COLUMN) == F.lit(QUALITY_RESULT_FAIL),
                F.lit(1),
            ).otherwise(F.lit(0))
        ).alias("failed"),
    ).collect()[0]
    return metrics_from_counts(
        table_name=table_name,
        check_name=QUALITY_CHECK_RESULT_COLUMN,
        total_evaluated=int(row["total_evaluated"] or 0),
        failed=int(row["failed"] or 0),
        population_kind=POPULATION_TABLE_OUTCOME,
    )


def assert_no_row_loss(before: Any, after: Any, *, context: str) -> None:
    """Compare physical counts. Callers still keep both DataFrames unfiltered."""
    before_count = before.count()
    after_count = after.count()
    if before_count != after_count:
        raise QualityError(
            f"{context}: quality check changed physical row count "
            f"({before_count} -> {after_count}). Bad rows must not be deleted."
        )


def assert_source_columns_unchanged(before: Any, after: Any, *, context: str) -> None:
    """
    Source + lineage values must be identical (exceptAll both directions).

    Uses Spark set-difference on the lineage/source projection, not a driver
    loop over the full dataset.
    """
    cols = lineage_and_source_columns(before)
    missing = [name for name in cols if name not in after.columns]
    if missing:
        raise QualityError(f"{context}: quality output dropped source columns {missing}.")
    left = before.select(*cols)
    right = after.select(*cols)
    extra = left.exceptAll(right).count()
    missing_rows = right.exceptAll(left).count()
    if extra or missing_rows:
        raise QualityError(
            f"{context}: source/lineage values changed "
            f"(rows only in input={extra}, only in output={missing_rows})."
        )
