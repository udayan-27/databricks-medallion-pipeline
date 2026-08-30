"""
Silver quality: completeness.

Detects SQL NULL in critical fields. Does not delete rows. Does not treat
empty string as NULL. Does not classify NULL foreign keys as referential
integrity / orphan failures (RI is a later module).

Critical fields named by the assignment (injected-defect contract):
- customers.email
- orders.customer_id
- orders.product_id

PK NULL checks from the frozen data-quality strategy (expected fail_count 0
on generated data unless a generator bug appears):
- customers.customer_id
- orders.order_id
- products.product_id

This module writes ``completeness_failed_checks`` and ``completeness_pass``.
It never writes last-writer ``quality_check_result`` and never emits ``ri:``
or ``uniqueness:`` codes.
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
from ingest_core import get_spark_session  # noqa: E402
from quality_common import (  # noqa: E402
    MODULE_COMPLETENESS,
    CheckMetrics,
    QualityError,
    attach_module_result,
    codes_for_null_fields,
    collect_code_fail_metrics,
    collect_pass_fail_metrics,
    failed_checks_column_name,
    pass_column_name,
    quality_code,
    require_columns,
    require_ingest_row_id,
)

LOGGER = logging.getLogger("silver.completeness")

# Guide-named critical fields first; PK extensions follow the frozen strategy.
COMPLETENESS_FIELDS: dict[str, tuple[str, ...]] = {
    "customers": ("email", "customer_id"),
    "orders": ("customer_id", "product_id", "order_id"),
    "products": ("product_id",),
}

# Injected-defect fields this increment must detect exactly.
CRITICAL_COMPLETENESS_FIELDS: dict[str, tuple[str, ...]] = {
    "customers": ("email",),
    "orders": ("customer_id", "product_id"),
}


def completeness_fields_for(table_name: str) -> tuple[str, ...]:
    try:
        return COMPLETENESS_FIELDS[table_name]
    except KeyError as exc:
        raise QualityError(
            f"No completeness field list for table {table_name!r}. "
            f"Known tables: {sorted(COMPLETENESS_FIELDS)}"
        ) from exc


def apply_completeness(dataframe: Any, table_name: str) -> Any:
    """
    Return every input row with completeness quality columns added.

    NULL detection is Spark ``IS NULL`` on the typed Bronze column. Empty
    string therefore passes. Source values and ``_ingest_row_id`` are not
    rewritten. No rows are dropped.
    """
    fields = completeness_fields_for(table_name)
    require_ingest_row_id(dataframe)
    require_columns(dataframe, fields, context=f"completeness:{table_name}")
    codes = codes_for_null_fields(table_name, fields, module=MODULE_COMPLETENESS)
    return attach_module_result(dataframe, MODULE_COMPLETENESS, codes)


def completeness_metrics(dataframe: Any, table_name: str) -> list[CheckMetrics]:
    """Module rollup plus one metric row per completeness field (physical rows)."""
    fields = completeness_fields_for(table_name)
    failed_col = failed_checks_column_name(MODULE_COMPLETENESS)
    pass_col = pass_column_name(MODULE_COMPLETENESS)
    metrics = [
        collect_pass_fail_metrics(
            dataframe,
            table_name=table_name,
            check_name=MODULE_COMPLETENESS,
            pass_column=pass_col,
        )
    ]
    for field in fields:
        code = quality_code(MODULE_COMPLETENESS, table_name, field)
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


def check_completeness(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    tables: tuple[str, ...] | None = None,
) -> dict[str, list[CheckMetrics]]:
    """
    Apply completeness to Bronze entity tables already ingested.

    Does not write Silver tables (orchestration is later). Does not run
    uniqueness, type, RI, or business-logic checks.
    """
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    targets = tables or tuple(COMPLETENESS_FIELDS)
    results: dict[str, list[CheckMetrics]] = {}
    for table_name in tables_or_known(targets):
        bronze = session.table(cfg.bronze_table(table_name))
        flagged = apply_completeness(bronze, table_name)
        metrics = completeness_metrics(flagged, table_name)
        results[table_name] = metrics
        for item in metrics:
            LOGGER.info(
                "completeness table=%s check=%s total=%s passed=%s failed=%s "
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


def tables_or_known(tables: tuple[str, ...]) -> tuple[str, ...]:
    unknown = [name for name in tables if name not in COMPLETENESS_FIELDS]
    if unknown:
        raise QualityError(f"Unknown completeness table(s): {unknown}")
    return tables


def _cli() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Silver completeness: flag NULL critical fields; do not delete rows."
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
    check_completeness(config=config)


if __name__ == "__main__":
    _cli()
