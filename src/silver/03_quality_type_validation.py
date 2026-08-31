"""
Silver quality: type validation.

Validates declared Bronze types and closed domains. Does not delete rows.
Does not coerce malformed values into valid values. Does not rewrite source
columns.

After PERMISSIVE Bronze ingest, unparsable INT/DATE/DECIMAL CSV tokens are
already NULL. This module therefore distinguishes three states:

- null/missing: SQL NULL on the typed column
- valid typed value: non-null, and in-domain when a closed set is declared
- invalid/malformed: (a) NULL on a typed field this module owns, which is how
  Spark represents an unparsable token; or (b) a non-null STRING outside a
  closed domain

Completeness-critical NULLs are **not** type failures. ``payment_date`` NULL
is valid (nullable DATE). Closed domains are case-sensitive and untrimmed.

This module writes ``type_failed_checks`` and ``type_validation_pass``.
It never overwrites completeness/uniqueness columns and never writes
last-writer ``quality_check_result``.

Exact type-fixture scenarios (see ``tests/fixtures/silver/type_validation/``):

- valid typed row passes
- malformed integer: orders.quantity = ``xyz`` → PERMISSIVE NULL → type fail
- malformed date: orders.order_date = ``13/01/2024`` (not yyyy-MM-dd) → NULL
- malformed decimal: orders.unit_price = ``12.34.56`` → NULL
- invalid domain: orders.order_status = ``Shipped``; customers.customer_segment
  = ``premium`` (wrong case)
- nullable payment_date NULL remains type-valid
- completeness-owned NULL customer_id is not a type failure
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

_SILVER_DIR = Path(__file__).resolve().parent
_SRC_DIR = _SILVER_DIR.parent
_BRONZE_DIR = _SRC_DIR / "bronze"
for _path in (str(_SRC_DIR), str(_BRONZE_DIR), str(_SILVER_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from config import PipelineConfig, load_config  # noqa: E402
from contracts import (  # noqa: E402
    CUSTOMER_SOURCE_FIELDS,
    ORDER_SOURCE_FIELDS,
    PRODUCT_SOURCE_FIELDS,
)
from ingest_core import get_spark_session  # noqa: E402
from quality_common import (  # noqa: E402
    MODULE_TYPE,
    CheckMetrics,
    QualityError,
    attach_module_result,
    codes_for_domain_violation,
    codes_for_null_fields,
    collect_code_fail_metrics,
    collect_pass_fail_metrics,
    concat_code_arrays,
    failed_checks_column_name,
    pass_column_name,
    quality_code,
    require_columns,
    require_ingest_row_id,
)

LOGGER = logging.getLogger("silver.type_validation")

# Must match 01_quality_completeness.COMPLETENESS_FIELDS. Contract tests assert
# equality so completeness-owned NULLs cannot be misclassified as type fails.
COMPLETENESS_OWNED_NULL_FIELDS: dict[str, tuple[str, ...]] = {
    "customers": ("email", "customer_id"),
    "orders": ("customer_id", "product_id", "order_id"),
    "products": ("product_id",),
}

# Documented nullable DATE. Legitimate NULL is not a malformed type.
NULLABLE_TYPED_FIELDS: dict[str, tuple[str, ...]] = {
    "customers": (),
    "orders": ("payment_date",),
    "products": (),
}

# Named closed domains only. Country / category are free STRING (not type).
DOMAIN_ALLOWLISTS: dict[str, tuple[tuple[str, tuple[str, ...]], ...]] = {
    "customers": (("customer_segment", ("Premium", "Standard", "Basic")),),
    "orders": (("order_status", ("Pending", "Completed", "Cancelled")),),
    "products": (),
}

# INT / DATE / DECIMAL(18,2) fields whose NULL after PERMISSIVE ingest is a
# type failure (missing or unparsable). Completeness-owned and payment_date
# are excluded.
TYPE_NULL_FIELDS: dict[str, tuple[str, ...]] = {
    "customers": ("signup_date", "lifetime_value"),
    "orders": ("order_date", "quantity", "unit_price", "total_amount"),
    "products": ("price", "cost", "stock_quantity", "reorder_level"),
}

SOURCE_FIELDS_BY_TABLE: dict[str, tuple[tuple[str, str], ...]] = {
    "customers": CUSTOMER_SOURCE_FIELDS,
    "orders": ORDER_SOURCE_FIELDS,
    "products": PRODUCT_SOURCE_FIELDS,
}

_PARSEABLE_TYPES = frozenset({"INT", "DATE", "DECIMAL(18,2)"})


def type_null_fields_for(table_name: str) -> tuple[str, ...]:
    try:
        return TYPE_NULL_FIELDS[table_name]
    except KeyError as exc:
        raise QualityError(
            f"No type-null field list for table {table_name!r}. "
            f"Known tables: {sorted(TYPE_NULL_FIELDS)}"
        ) from exc


def domain_allowlists_for(table_name: str) -> tuple[tuple[str, tuple[str, ...]], ...]:
    try:
        return DOMAIN_ALLOWLISTS[table_name]
    except KeyError as exc:
        raise QualityError(
            f"No domain allowlist for table {table_name!r}. "
            f"Known tables: {sorted(DOMAIN_ALLOWLISTS)}"
        ) from exc


def declared_source_fields(table_name: str) -> tuple[tuple[str, str], ...]:
    try:
        return SOURCE_FIELDS_BY_TABLE[table_name]
    except KeyError as exc:
        raise QualityError(f"No Bronze source contract for table {table_name!r}.") from exc


def derived_type_null_fields(table_name: str) -> tuple[str, ...]:
    """INT/DATE/DECIMAL fields not owned by completeness and not nullable-by-design."""
    completeness_owned = set(COMPLETENESS_OWNED_NULL_FIELDS[table_name])
    nullable = set(NULLABLE_TYPED_FIELDS[table_name])
    owned: list[str] = []
    for name, type_name in declared_source_fields(table_name):
        if type_name not in _PARSEABLE_TYPES:
            continue
        if name in completeness_owned or name in nullable:
            continue
        owned.append(name)
    return tuple(owned)


def apply_type_validation(dataframe: Any, table_name: str) -> Any:
    """
    Return every input row with type-validation quality columns added.

    Domain checks use non-null NOT IN allowlist. Parse-null checks use
    ``IS NULL`` only on fields this module owns. Source values and
    ``_ingest_row_id`` are not rewritten. No rows are dropped.
    """
    require_ingest_row_id(dataframe)
    domain_specs = domain_allowlists_for(table_name)
    null_fields = type_null_fields_for(table_name)
    required = [field for field, _allowed in domain_specs] + list(null_fields)
    if required:
        require_columns(dataframe, required, context=f"type:{table_name}")

    parts: list[Any] = []
    for field, allowed in domain_specs:
        parts.append(codes_for_domain_violation(table_name, field, allowed))
    if null_fields:
        parts.append(
            codes_for_null_fields(table_name, null_fields, module=MODULE_TYPE)
        )
    return attach_module_result(dataframe, MODULE_TYPE, concat_code_arrays(parts))


def type_validation_metrics(dataframe: Any, table_name: str) -> list[CheckMetrics]:
    """Module rollup plus one metric row per domain rule and type-null field."""
    failed_col = failed_checks_column_name(MODULE_TYPE)
    pass_col = pass_column_name(MODULE_TYPE)
    metrics = [
        collect_pass_fail_metrics(
            dataframe,
            table_name=table_name,
            check_name=MODULE_TYPE,
            pass_column=pass_col,
        )
    ]
    for field, _allowed in domain_allowlists_for(table_name):
        code = quality_code(MODULE_TYPE, table_name, f"{field}_domain")
        metrics.append(
            collect_code_fail_metrics(
                dataframe,
                table_name=table_name,
                check_name=code,
                failed_checks_column=failed_col,
                code=code,
            )
        )
    for field in type_null_fields_for(table_name):
        code = quality_code(MODULE_TYPE, table_name, field)
        metrics.append(
            collect_code_fail_metrics(
                dataframe,
                table_name=table_name,
                check_name=code,
                failed_checks_column=failed_col,
                code=code,
            )
        )
    return metrics


def check_type_validation(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    tables: tuple[str, ...] | None = None,
) -> dict[str, list[CheckMetrics]]:
    """
    Apply type validation to Bronze entity tables already ingested.

    Does not write Silver tables. Does not run completeness, uniqueness, RI,
    or business-logic checks. Safe after those modules: columns are separate.
    """
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    targets = tables or tuple(TYPE_NULL_FIELDS)
    unknown = [name for name in targets if name not in TYPE_NULL_FIELDS]
    if unknown:
        raise QualityError(f"Unknown type-validation table(s): {unknown}")
    results: dict[str, list[CheckMetrics]] = {}
    for table_name in targets:
        bronze = session.table(cfg.bronze_table(table_name))
        flagged = apply_type_validation(bronze, table_name)
        metrics = type_validation_metrics(flagged, table_name)
        results[table_name] = metrics
        for item in metrics:
            LOGGER.info(
                "type_validation table=%s check=%s total=%s passed=%s failed=%s "
                "pass_pct=%s fail_pct=%s",
                item.table_name,
                item.check_name,
                item.total_evaluated,
                item.passed,
                item.failed,
                item.pass_pct,
                item.fail_pct,
            )
    return results


def _cli() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Silver type validation: flag malformed types and domain "
        "violations; do not delete rows."
    )
    parser.add_argument("--data-path", default=None)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--bronze-schema", default=None)
    parser.add_argument(
        "--table-format",
        default=None,
        choices=["delta", "parquet"],
        help="Must match the format used when Bronze tables were written.",
    )
    args = parser.parse_args()
    config = load_config(
        data_path=args.data_path,
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        table_format=args.table_format,
    )
    check_type_validation(config=config)


if __name__ == "__main__":
    _cli()
