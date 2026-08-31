"""
Silver quality: business logic.

Applies the frozen cross-field / policy rules from data-quality-strategy.md.
Does not invent extra rules. Does not delete rows. Does not rewrite source
columns or ``_ingest_row_id``.

NULL fields skip rules that need them (completeness/type already own those
failures). Enums are type, not business. Pending orders may have a payment
date or not; there is no extra pending rule.

Product cost-vs-price is **not** a frozen rule and is not implemented.

As-of date is frozen at 2026-08-31 (same as Stage 2 generation). Never
the Spark job clock date.

This module writes ``business_logic_failed_checks`` and
``business_logic_pass``. It never overwrites completeness / uniqueness / type /
RI columns and never writes last-writer ``quality_check_result``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

_SILVER_DIR = Path(__file__).resolve().parent
_SRC_DIR = _SILVER_DIR.parent
_BRONZE_DIR = _SRC_DIR / "bronze"
for _path in (str(_SRC_DIR), str(_BRONZE_DIR), str(_SILVER_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from config import PipelineConfig, load_config  # noqa: E402
from contracts import INGEST_ROW_ID_COLUMN  # noqa: E402
from ingest_core import get_spark_session  # noqa: E402
from quality_common import (  # noqa: E402
    MODULE_BUSINESS,
    CheckMetrics,
    QualityError,
    attach_module_result,
    codes_for_when,
    collect_code_fail_metrics,
    collect_pass_fail_metrics,
    concat_code_arrays,
    failed_checks_column_name,
    pass_column_name,
    quality_code,
    require_columns,
    require_ingest_row_id,
    assert_no_row_loss,
)

LOGGER = logging.getLogger("silver.business_logic")

# Frozen with Stage 2 generation. Not the Spark job clock date.
BUSINESS_AS_OF_DATE = date(2026, 8, 31)
AMOUNT_TOLERANCE = Decimal("0.01")

_BL_CUSTOMER_KEY = "_bl_customer_id"
_BL_SIGNUP_DATE = "_bl_signup_date"
_BL_PARENT_INGEST = "_bl_parent_ingest_id"

QUANTITY_POSITIVE = quality_code(MODULE_BUSINESS, "orders", "quantity_positive")
UNIT_PRICE_NON_NEGATIVE = quality_code(MODULE_BUSINESS, "orders", "unit_price_non_negative")
TOTAL_AMOUNT_NON_NEGATIVE = quality_code(MODULE_BUSINESS, "orders", "total_amount_non_negative")
AMOUNT_EQUALS_QTY_PRICE = quality_code(MODULE_BUSINESS, "orders", "amount_equals_qty_price")
COMPLETED_HAS_PAYMENT = quality_code(MODULE_BUSINESS, "orders", "completed_has_payment")
CANCELLED_WITHOUT_PAYMENT = quality_code(MODULE_BUSINESS, "orders", "cancelled_without_payment")
PAYMENT_ON_OR_AFTER_ORDER = quality_code(MODULE_BUSINESS, "orders", "payment_on_or_after_order")
ORDER_NOT_BEFORE_SIGNUP = quality_code(MODULE_BUSINESS, "orders", "order_not_before_signup")
SIGNUP_NOT_FUTURE = quality_code(MODULE_BUSINESS, "customers", "signup_not_future")
LIFETIME_VALUE_NON_NEGATIVE = quality_code(MODULE_BUSINESS, "customers", "lifetime_value_non_negative")
PRICE_NON_NEGATIVE = quality_code(MODULE_BUSINESS, "products", "price_non_negative")
COST_NON_NEGATIVE = quality_code(MODULE_BUSINESS, "products", "cost_non_negative")
STOCK_NON_NEGATIVE = quality_code(MODULE_BUSINESS, "products", "stock_non_negative")
REORDER_NON_NEGATIVE = quality_code(MODULE_BUSINESS, "products", "reorder_non_negative")

CUSTOMER_RULE_CODES: tuple[str, ...] = (
    SIGNUP_NOT_FUTURE,
    LIFETIME_VALUE_NON_NEGATIVE,
)
ORDER_RULE_CODES: tuple[str, ...] = (
    QUANTITY_POSITIVE,
    UNIT_PRICE_NON_NEGATIVE,
    TOTAL_AMOUNT_NON_NEGATIVE,
    AMOUNT_EQUALS_QTY_PRICE,
    COMPLETED_HAS_PAYMENT,
    CANCELLED_WITHOUT_PAYMENT,
    PAYMENT_ON_OR_AFTER_ORDER,
    ORDER_NOT_BEFORE_SIGNUP,
)
PRODUCT_RULE_CODES: tuple[str, ...] = (
    PRICE_NON_NEGATIVE,
    COST_NON_NEGATIVE,
    STOCK_NON_NEGATIVE,
    REORDER_NON_NEGATIVE,
)

RULE_CODES_BY_TABLE: dict[str, tuple[str, ...]] = {
    "customers": CUSTOMER_RULE_CODES,
    "orders": ORDER_RULE_CODES,
    "products": PRODUCT_RULE_CODES,
}

BUSINESS_TABLES = ("customers", "orders", "products")


def _import_pyspark_functions() -> tuple[Any, Any]:
    from quality_common import _import_pyspark

    return _import_pyspark()


def _decimal_18_2() -> Any:
    from pyspark.sql.types import DecimalType

    return DecimalType(18, 2)


def business_rule_codes_for(table_name: str) -> tuple[str, ...]:
    try:
        return RULE_CODES_BY_TABLE[table_name]
    except KeyError as exc:
        raise QualityError(
            f"No business-logic rules for table {table_name!r}. "
            f"Known tables: {sorted(RULE_CODES_BY_TABLE)}"
        ) from exc


def canonical_customer_signup(customers: Any) -> Any:
    """
    One parent row per non-null customer_id, ``min(_ingest_row_id)``.

    Duplicate customer profiles with disagreeing signup_dates use the earliest
    ingest lineage row. This is a derived lookup set, not a rewrite of the
    customers table, so a later left join cannot fan out orders.
    """
    F, Window = _import_pyspark_functions()
    require_ingest_row_id(customers)
    require_columns(customers, ["customer_id", "signup_date"], context="BL customer lookup")
    window = Window.partitionBy(_BL_CUSTOMER_KEY).orderBy(F.col(_BL_PARENT_INGEST).asc())
    ranked = (
        customers.where(F.col("customer_id").isNotNull())
        .select(
            F.col("customer_id").alias(_BL_CUSTOMER_KEY),
            F.col("signup_date").alias(_BL_SIGNUP_DATE),
            F.col(INGEST_ROW_ID_COLUMN).alias(_BL_PARENT_INGEST),
        )
        .withColumn("_bl_rn", F.row_number().over(window))
    )
    return ranked.where(F.col("_bl_rn") == 1).drop("_bl_rn", _BL_PARENT_INGEST)


def _customer_rule_codes(F: Any, as_of: date) -> Any:
    as_of_lit = F.lit(as_of)
    return concat_code_arrays(
        [
            codes_for_when(
                F.col("signup_date").isNotNull() & (F.col("signup_date") > as_of_lit),
                SIGNUP_NOT_FUTURE,
            ),
            codes_for_when(
                F.col("lifetime_value").isNotNull() & (F.col("lifetime_value") < 0),
                LIFETIME_VALUE_NON_NEGATIVE,
            ),
        ]
    )


def _product_rule_codes(F: Any) -> Any:
    return concat_code_arrays(
        [
            codes_for_when(
                F.col("price").isNotNull() & (F.col("price") < 0),
                PRICE_NON_NEGATIVE,
            ),
            codes_for_when(
                F.col("cost").isNotNull() & (F.col("cost") < 0),
                COST_NON_NEGATIVE,
            ),
            codes_for_when(
                F.col("stock_quantity").isNotNull() & (F.col("stock_quantity") < 0),
                STOCK_NON_NEGATIVE,
            ),
            codes_for_when(
                F.col("reorder_level").isNotNull() & (F.col("reorder_level") < 0),
                REORDER_NON_NEGATIVE,
            ),
        ]
    )


def _amount_mismatch_condition(F: Any) -> Any:
    """
    abs(total_amount - quantity * unit_price) > 0.01 when all three are non-null.

    Quantity is cast to DECIMAL(18,2) before multiplication so Spark does not
    promote money to floating point. The 0.01 boundary passes (<=); 0.02 fails.
    """
    decimal_type = _decimal_18_2()
    qty_dec = F.col("quantity").cast(decimal_type)
    expected = qty_dec * F.col("unit_price")
    diff = F.abs(F.col("total_amount") - expected)
    tolerance = F.lit(str(AMOUNT_TOLERANCE)).cast(decimal_type)
    return (
        F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
        & F.col("total_amount").isNotNull()
        & (diff > tolerance)
    )


def _order_local_rule_codes(F: Any) -> Any:
    return concat_code_arrays(
        [
            codes_for_when(
                F.col("quantity").isNotNull() & (F.col("quantity") <= 0),
                QUANTITY_POSITIVE,
            ),
            codes_for_when(
                F.col("unit_price").isNotNull() & (F.col("unit_price") < 0),
                UNIT_PRICE_NON_NEGATIVE,
            ),
            codes_for_when(
                F.col("total_amount").isNotNull() & (F.col("total_amount") < 0),
                TOTAL_AMOUNT_NON_NEGATIVE,
            ),
            codes_for_when(_amount_mismatch_condition(F), AMOUNT_EQUALS_QTY_PRICE),
            codes_for_when(
                (F.col("order_status") == F.lit("Completed")) & F.col("payment_date").isNull(),
                COMPLETED_HAS_PAYMENT,
            ),
            codes_for_when(
                (F.col("order_status") == F.lit("Cancelled")) & F.col("payment_date").isNotNull(),
                CANCELLED_WITHOUT_PAYMENT,
            ),
            codes_for_when(
                F.col("payment_date").isNotNull()
                & F.col("order_date").isNotNull()
                & (F.col("payment_date") < F.col("order_date")),
                PAYMENT_ON_OR_AFTER_ORDER,
            ),
        ]
    )


def _order_signup_rule_codes(F: Any) -> Any:
    """
    Skip when customer_id is NULL, the parent is missing (orphan), or either
    date is NULL. Those cases are completeness / RI / type, not this rule.
    """
    return codes_for_when(
        F.col("customer_id").isNotNull()
        & F.col(_BL_CUSTOMER_KEY).isNotNull()
        & F.col("order_date").isNotNull()
        & F.col(_BL_SIGNUP_DATE).isNotNull()
        & (F.col("order_date") < F.col(_BL_SIGNUP_DATE)),
        ORDER_NOT_BEFORE_SIGNUP,
    )


def apply_business_logic(
    dataframe: Any,
    table_name: str,
    *,
    customers: Any | None = None,
    as_of_date: date | None = None,
) -> Any:
    """
    Return every input row with business-logic quality columns added.

    Orders require Bronze (or equivalent) ``customers`` for signup lookup.
    Physical grain is preserved: left join to a unique canonical parent only.
    """
    F, _Window = _import_pyspark_functions()
    require_ingest_row_id(dataframe)
    as_of = as_of_date or BUSINESS_AS_OF_DATE

    if table_name == "customers":
        require_columns(
            dataframe,
            ["signup_date", "lifetime_value"],
            context="business:customers",
        )
        return attach_module_result(dataframe, MODULE_BUSINESS, _customer_rule_codes(F, as_of))

    if table_name == "products":
        require_columns(
            dataframe,
            ["price", "cost", "stock_quantity", "reorder_level"],
            context="business:products",
        )
        return attach_module_result(dataframe, MODULE_BUSINESS, _product_rule_codes(F))

    if table_name != "orders":
        raise QualityError(f"Unknown business-logic table {table_name!r}.")

    if customers is None:
        raise QualityError(
            "Business logic for orders requires parent customers DataFrames "
            "(Bronze, including duplicate ids). Do not pass a filtered PASS subset."
        )
    require_columns(
        dataframe,
        [
            "customer_id",
            "order_date",
            "quantity",
            "unit_price",
            "total_amount",
            "order_status",
            "payment_date",
        ],
        context="business:orders",
    )

    parent = canonical_customer_signup(customers)
    with_parent = dataframe.join(
        F.broadcast(parent),
        dataframe["customer_id"] == parent[_BL_CUSTOMER_KEY],
        "left",
    )
    codes = concat_code_arrays(
        [_order_local_rule_codes(F), _order_signup_rule_codes(F)]
    )
    flagged = attach_module_result(with_parent, MODULE_BUSINESS, codes).drop(
        _BL_CUSTOMER_KEY, _BL_SIGNUP_DATE
    )
    assert_no_row_loss(dataframe, flagged, context="business_logic:orders")
    return flagged


def business_logic_metrics(dataframe: Any, table_name: str) -> list[CheckMetrics]:
    """Module rollup plus one metric row per frozen business rule."""
    failed_col = failed_checks_column_name(MODULE_BUSINESS)
    pass_col = pass_column_name(MODULE_BUSINESS)
    metrics = [
        collect_pass_fail_metrics(
            dataframe,
            table_name=table_name,
            check_name=MODULE_BUSINESS,
            pass_column=pass_col,
        )
    ]
    for code in business_rule_codes_for(table_name):
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


def check_business_logic(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    tables: tuple[str, ...] | None = None,
    as_of_date: date | None = None,
) -> dict[str, list[CheckMetrics]]:
    """
    Apply business logic to Bronze entity tables already ingested.

    Does not write Silver tables. Does not run other quality modules. Safe
    after those modules: columns are separate and are not overwritten.
    """
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    targets = tables or BUSINESS_TABLES
    unknown = [name for name in targets if name not in RULE_CODES_BY_TABLE]
    if unknown:
        raise QualityError(f"Unknown business-logic table(s): {unknown}")

    customers = session.table(cfg.bronze_table("customers"))
    results: dict[str, list[CheckMetrics]] = {}
    for table_name in targets:
        bronze = session.table(cfg.bronze_table(table_name))
        flagged = apply_business_logic(
            bronze,
            table_name,
            customers=customers,
            as_of_date=as_of_date,
        )
        metrics = business_logic_metrics(flagged, table_name)
        results[table_name] = metrics
        for item in metrics:
            LOGGER.info(
                "business_logic table=%s check=%s total=%s passed=%s failed=%s "
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
        description="Silver business logic: flag documented policy violations; "
        "do not delete rows."
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
    check_business_logic(config=config)


if __name__ == "__main__":
    _cli()
