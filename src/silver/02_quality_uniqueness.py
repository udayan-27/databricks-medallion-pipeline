"""
Silver quality: uniqueness.

Detects duplicate business keys. Flags **every** physical row that
participates in a duplicate-key group. Does not delete rows, does not
deduplicate, does not pick a survivor, and does not rewrite business keys.

Checks:
- customers.customer_id
- orders.order_id
- products.product_id (frozen strategy; expected 0 failures on generated data)

NULL keys are not treated as duplicates of each other (completeness owns NULLs).

This module writes ``uniqueness_failed_checks`` and ``uniqueness_pass``.
It never overwrites completeness columns, never writes last-writer
``quality_check_result``, and never emits ``ri:`` or ``completeness:`` codes.
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
    MODULE_UNIQUENESS,
    CheckMetrics,
    QualityError,
    attach_module_result,
    collect_duplicate_key_count,
    collect_pass_fail_metrics,
    pass_column_name,
    require_columns,
    require_ingest_row_id,
    uniqueness_codes_for_key,
)

LOGGER = logging.getLogger("silver.uniqueness")

UNIQUENESS_KEYS: dict[str, str] = {
    "customers": "customer_id",
    "orders": "order_id",
    "products": "product_id",
}


def uniqueness_key_for(table_name: str) -> str:
    try:
        return UNIQUENESS_KEYS[table_name]
    except KeyError as exc:
        raise QualityError(
            f"No uniqueness key for table {table_name!r}. "
            f"Known tables: {sorted(UNIQUENESS_KEYS)}"
        ) from exc


def apply_uniqueness(dataframe: Any, table_name: str) -> Any:
    """
    Return every input row with uniqueness quality columns added.

    Detection is a window ``COUNT(*)`` over the non-null business key. Every
    physical row in a group with count > 1 fails. The result is the input
    grain plus two columns — no join back on the business key, so duplicate
    keys cannot fan out.
    """
    key = uniqueness_key_for(table_name)
    require_ingest_row_id(dataframe)
    require_columns(dataframe, [key], context=f"uniqueness:{table_name}")
    codes = uniqueness_codes_for_key(table_name, key)
    return attach_module_result(dataframe, MODULE_UNIQUENESS, codes)


def uniqueness_metrics(dataframe: Any, table_name: str) -> list[CheckMetrics]:
    """
    Physical-row uniqueness metrics plus an optional distinct-key count.

    fail_count is participating physical rows (20 customers / 40 orders on
    generated data), not the extra-row issue-instance counts (10 / 20).
    """
    key = uniqueness_key_for(table_name)
    pass_col = pass_column_name(MODULE_UNIQUENESS)
    module_metrics = collect_pass_fail_metrics(
        dataframe,
        table_name=table_name,
        check_name=MODULE_UNIQUENESS,
        pass_column=pass_col,
    )
    key_metrics = collect_pass_fail_metrics(
        dataframe,
        table_name=table_name,
        check_name=f"{MODULE_UNIQUENESS}:{table_name}.{key}",
        pass_column=pass_col,
    )
    return [module_metrics, key_metrics]


def uniqueness_duplicate_key_count(dataframe: Any, table_name: str) -> int:
    key = uniqueness_key_for(table_name)
    pass_col = pass_column_name(MODULE_UNIQUENESS)
    return collect_duplicate_key_count(dataframe, key, pass_col)


def check_uniqueness(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    tables: tuple[str, ...] | None = None,
) -> dict[str, list[CheckMetrics]]:
    """
    Apply uniqueness to Bronze entity tables already ingested.

    Does not write Silver tables. Does not run completeness, type, RI, or
    business-logic checks. Applying this after completeness is safe: module
    columns are separate and ``attach_module_result`` will not overwrite an
    earlier module's codes.
    """
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    targets = tables or tuple(UNIQUENESS_KEYS)
    results: dict[str, list[CheckMetrics]] = {}
    unknown = [name for name in targets if name not in UNIQUENESS_KEYS]
    if unknown:
        raise QualityError(f"Unknown uniqueness table(s): {unknown}")
    for table_name in targets:
        bronze = session.table(cfg.bronze_table(table_name))
        flagged = apply_uniqueness(bronze, table_name)
        metrics = uniqueness_metrics(flagged, table_name)
        dup_keys = uniqueness_duplicate_key_count(flagged, table_name)
        results[table_name] = metrics
        for item in metrics:
            LOGGER.info(
                "uniqueness table=%s check=%s total=%s passed=%s failed=%s "
                "pass_pct=%s fail_pct=%s duplicate_keys=%s",
                item.table_name,
                item.check_name,
                item.total_evaluated,
                item.passed,
                item.failed,
                item.pass_pct,
                item.fail_pct,
                dup_keys,
            )
    return results


def _cli() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Silver uniqueness: flag all duplicate-key copies; do not delete rows."
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
    check_uniqueness(config=config)


if __name__ == "__main__":
    _cli()
