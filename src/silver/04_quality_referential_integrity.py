"""
Silver quality: referential integrity.

Required relationships (orders only):

- orders.customer_id → customers.customer_id
- orders.product_id → products.product_id

NULL child FKs are **not** orphans (completeness owns them). Non-null values
absent from the parent key set are RI failures. FK values are not rewritten.
Orphan rows are not deleted. Parent tables are not deduplicated as tables;
a **distinct existence set** of non-null parent keys is used so duplicate
parent IDs cannot fan out child rows.

Join safety: left-join each child FK to ``SELECT DISTINCT parent_pk`` (broadcast
the small key set). Because the parent side of the join key is unique, each
physical child row matches at most one parent-key row. Input physical row
count equals output physical row count. Identity is ``_ingest_row_id``.

A naïve ``orders JOIN customers ON customer_id`` against the full parent table
is forbidden here: duplicate customer_ids would multiply order rows.

This module writes ``referential_integrity_failed_checks`` and
``referential_integrity_pass``. Customers and products have no FKs: every row
passes. Completeness/uniqueness/type columns are never overwritten.
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
    MODULE_RI,
    CheckMetrics,
    QualityError,
    assert_no_row_loss,
    attach_module_result,
    codes_for_orphan_fk,
    collect_code_fail_metrics,
    collect_pass_fail_metrics,
    concat_code_arrays,
    empty_string_array,
    failed_checks_column_name,
    pass_column_name,
    quality_code,
    require_columns,
    require_ingest_row_id,
)

LOGGER = logging.getLogger("silver.referential_integrity")

CUSTOMER_ORPHAN_CODE = quality_code(MODULE_RI, "orders", "customer_id_orphan")
PRODUCT_ORPHAN_CODE = quality_code(MODULE_RI, "orders", "product_id_orphan")

_RI_CUSTOMER_KEY = "_ri_customer_key"
_RI_PRODUCT_KEY = "_ri_product_key"

RI_TABLES = ("customers", "orders", "products")


def _import_pyspark_functions() -> tuple[Any, Any]:
    from quality_common import _import_pyspark

    return _import_pyspark()


def distinct_parent_keys(parents: Any, key: str, alias: str) -> Any:
    """
    Distinct non-null parent identifiers for existence checks.

    This is a derived key set, not a rewrite of the parent table. Duplicate
    parent rows collapse to one existence value so a later left join cannot
    multiply child rows.
    """
    F, _Window = _import_pyspark_functions()
    require_columns(parents, [key], context=f"RI parent key {key}")
    return (
        parents.select(F.col(key).alias(alias))
        .where(F.col(alias).isNotNull())
        .distinct()
    )


def apply_referential_integrity(
    dataframe: Any,
    table_name: str,
    *,
    customers: Any | None = None,
    products: Any | None = None,
) -> Any:
    """
    Return every input row with RI quality columns added.

    Orders require ``customers`` and ``products`` Bronze (or equivalent) frames.
    Physical grain is preserved: left join to distinct parent keys only.
    """
    F, _Window = _import_pyspark_functions()
    require_ingest_row_id(dataframe)
    if table_name != "orders":
        if table_name not in RI_TABLES:
            raise QualityError(f"Unknown RI table {table_name!r}.")
        return attach_module_result(dataframe, MODULE_RI, empty_string_array(F))

    if customers is None or products is None:
        raise QualityError(
            "Referential integrity for orders requires parent customers and "
            "products DataFrames. Pass the Bronze tables, not a filtered PASS subset."
        )
    require_columns(dataframe, ["customer_id", "product_id"], context="ri:orders")
    require_columns(customers, ["customer_id"], context="ri:customers parent")
    require_columns(products, ["product_id"], context="ri:products parent")

    customer_keys = distinct_parent_keys(customers, "customer_id", _RI_CUSTOMER_KEY)
    product_keys = distinct_parent_keys(products, "product_id", _RI_PRODUCT_KEY)

    # Broadcast the small distinct key sets. Left join (not inner) keeps every
    # child row, including NULL FKs. Distinct parent keys ⇒ at most one match.
    with_customers = dataframe.join(
        F.broadcast(customer_keys),
        dataframe["customer_id"] == customer_keys[_RI_CUSTOMER_KEY],
        "left",
    )
    with_parents = with_customers.join(
        F.broadcast(product_keys),
        with_customers["product_id"] == product_keys[_RI_PRODUCT_KEY],
        "left",
    )

    codes = concat_code_arrays(
        [
            codes_for_orphan_fk("orders", "customer_id", _RI_CUSTOMER_KEY),
            codes_for_orphan_fk("orders", "product_id", _RI_PRODUCT_KEY),
        ]
    )
    flagged = attach_module_result(with_parents, MODULE_RI, codes).drop(
        _RI_CUSTOMER_KEY, _RI_PRODUCT_KEY
    )
    assert_no_row_loss(dataframe, flagged, context="referential_integrity:orders")
    return flagged


def referential_integrity_metrics(dataframe: Any, table_name: str) -> list[CheckMetrics]:
    """Module rollup; orders also emit per-FK orphan counts (physical rows)."""
    pass_col = pass_column_name(MODULE_RI)
    failed_col = failed_checks_column_name(MODULE_RI)
    metrics = [
        collect_pass_fail_metrics(
            dataframe,
            table_name=table_name,
            check_name=MODULE_RI,
            pass_column=pass_col,
        )
    ]
    if table_name == "orders":
        for code in (CUSTOMER_ORPHAN_CODE, PRODUCT_ORPHAN_CODE):
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


def check_referential_integrity(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    tables: tuple[str, ...] | None = None,
) -> dict[str, list[CheckMetrics]]:
    """
    Apply RI to Bronze entity tables already ingested.

    Parent existence uses Bronze customers/products (all non-null ids, including
    duplicates). Does not write Silver tables. Does not run other quality modules.
    """
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    targets = tables or RI_TABLES
    unknown = [name for name in targets if name not in RI_TABLES]
    if unknown:
        raise QualityError(f"Unknown RI table(s): {unknown}")

    customers = session.table(cfg.bronze_table("customers"))
    products = session.table(cfg.bronze_table("products"))
    results: dict[str, list[CheckMetrics]] = {}
    for table_name in targets:
        bronze = session.table(cfg.bronze_table(table_name))
        flagged = apply_referential_integrity(
            bronze,
            table_name,
            customers=customers,
            products=products,
        )
        metrics = referential_integrity_metrics(flagged, table_name)
        results[table_name] = metrics
        for item in metrics:
            LOGGER.info(
                "referential_integrity table=%s check=%s total=%s passed=%s failed=%s "
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
        description="Silver referential integrity: flag non-null orphan FKs; "
        "do not delete rows or multiply them."
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
    check_referential_integrity(config=config)


if __name__ == "__main__":
    _cli()
