"""
Create Silver tables: run all five quality modules, combine flags, write
entity tables and ``silver.quality_metrics``.

Preserves every Bronze physical row, source column values, and
``_ingest_row_id``. Modules are applied independently (separate columns);
this orchestrator concatenates per-module arrays into ``failed_checks``
and sets ``quality_check_result``. Later modules never overwrite earlier
failures.

Local vs Databricks
-------------------
Databricks: run on a cluster with Bronze tables already written (Delta).
Local: the same code can write parquet Silver tables when
``MEDALLION_TABLE_FORMAT=parquet``. Absence of Databricks is not a
successful Databricks run.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Sequence

_SILVER_DIR = Path(__file__).resolve().parent
_SRC_DIR = _SILVER_DIR.parent
_BRONZE_DIR = _SRC_DIR / "bronze"
for _path in (str(_SRC_DIR), str(_BRONZE_DIR), str(_SILVER_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from config import PipelineConfig, load_config  # noqa: E402
from ingest_core import get_spark_session  # noqa: E402
from quality_common import (  # noqa: E402
    COMBINED_FAILED_CHECKS_COLUMN,
    MODULE_BUSINESS,
    MODULE_COMPLETENESS,
    MODULE_RI,
    MODULE_TYPE,
    MODULE_UNIQUENESS,
    QUALITY_CHECK_RESULT_COLUMN,
    CheckMetrics,
    QualityError,
    assert_no_row_loss,
    assert_source_columns_unchanged,
    collect_distinct_key_metrics,
    collect_table_outcome_metrics,
    combine_quality_status,
)

import importlib.util


def _load_silver(filename: str, module_name: str):
    path = _SILVER_DIR / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


completeness = _load_silver("01_quality_completeness.py", "quality_completeness")
uniqueness = _load_silver("02_quality_uniqueness.py", "quality_uniqueness")
type_validation = _load_silver("03_quality_type_validation.py", "quality_type_validation")
referential_integrity = _load_silver(
    "04_quality_referential_integrity.py", "quality_referential_integrity"
)
business_logic = _load_silver("05_quality_business_logic.py", "quality_business_logic")

LOGGER = logging.getLogger("silver.create_tables")

SILVER_ENTITY_TABLES = ("customers", "orders", "products")
SILVER_METRICS_TABLE = "quality_metrics"

# Stage 2 seed-42 contract. Optional future-signup is documented separately from
# the mandatory 460 issue instances. Type expected 0. Other business rules
# expected 0 on generated data (generator does not sabotage amounts/dates).
INTENTIONAL_FAIL_COUNTS: dict[tuple[str, str], int] = {
    ("customers", "completeness:customers.email"): 50,
    ("customers", "completeness:customers.customer_id"): 0,
    ("customers", MODULE_COMPLETENESS): 50,
    ("customers", MODULE_UNIQUENESS): 20,
    ("customers", "uniqueness:customers.customer_id"): 20,
    ("customers", "uniqueness:customers.customer_id.duplicate_keys"): 10,
    ("customers", MODULE_TYPE): 0,
    ("customers", "business:customers.signup_not_future"): 30,
    ("customers", "business:customers.lifetime_value_non_negative"): 0,
    ("customers", MODULE_BUSINESS): 30,
    ("customers", MODULE_RI): 0,
    ("orders", "completeness:orders.customer_id"): 100,
    ("orders", "completeness:orders.product_id"): 200,
    ("orders", "completeness:orders.order_id"): 0,
    ("orders", MODULE_COMPLETENESS): 300,
    ("orders", MODULE_UNIQUENESS): 40,
    ("orders", "uniqueness:orders.order_id"): 40,
    ("orders", "uniqueness:orders.order_id.duplicate_keys"): 20,
    ("orders", MODULE_TYPE): 0,
    ("orders", "ri:orders.customer_id_orphan"): 50,
    ("orders", "ri:orders.product_id_orphan"): 30,
    ("orders", MODULE_RI): 80,
    ("orders", "business:orders.quantity_positive"): 0,
    ("orders", "business:orders.unit_price_non_negative"): 0,
    ("orders", "business:orders.total_amount_non_negative"): 0,
    ("orders", "business:orders.amount_equals_qty_price"): 0,
    ("orders", "business:orders.completed_has_payment"): 0,
    ("orders", "business:orders.cancelled_without_payment"): 0,
    ("orders", "business:orders.payment_on_or_after_order"): 0,
    ("orders", "business:orders.order_not_before_signup"): 0,
    ("orders", MODULE_BUSINESS): 0,
    ("products", MODULE_COMPLETENESS): 0,
    ("products", "completeness:products.product_id"): 0,
    ("products", MODULE_UNIQUENESS): 0,
    ("products", "uniqueness:products.product_id"): 0,
    ("products", "uniqueness:products.product_id.duplicate_keys"): 0,
    ("products", MODULE_TYPE): 0,
    ("products", MODULE_RI): 0,
    ("products", MODULE_BUSINESS): 0,
    ("products", "business:products.price_non_negative"): 0,
    ("products", "business:products.cost_non_negative"): 0,
    ("products", "business:products.stock_non_negative"): 0,
    ("products", "business:products.reorder_non_negative"): 0,
}

# Mandatory listed issue instances (not uniqueness participating-row counts,
# not optional future signups).
MANDATORY_ISSUE_INSTANCES = 460

UNIQUENESS_KEY_METRIC_NAMES: dict[str, str] = {
    "customers": "uniqueness:customers.customer_id.duplicate_keys",
    "orders": "uniqueness:orders.order_id.duplicate_keys",
    "products": "uniqueness:products.product_id.duplicate_keys",
}


@dataclass(frozen=True)
class SilverTableResult:
    table_name: str
    bronze_rows: int
    silver_rows: int
    fail_rows: int
    pass_rows: int


@dataclass(frozen=True)
class SilverBuildResult:
    tables: dict[str, Any]
    metrics: list[CheckMetrics]
    table_results: dict[str, SilverTableResult]
    computed_at: datetime


def apply_all_quality_modules(
    dataframe: Any,
    table_name: str,
    *,
    customers: Any,
    products: Any,
    as_of_date: date | None = None,
) -> Any:
    """
    Run completeness → uniqueness → type → RI → business logic, then combine.

    Each module attaches its own columns. Combined ``failed_checks`` is built
    last so no module can last-writer-overwrite ``quality_check_result``.
    """
    after = completeness.apply_completeness(dataframe, table_name)
    after = uniqueness.apply_uniqueness(after, table_name)
    after = type_validation.apply_type_validation(after, table_name)
    after = referential_integrity.apply_referential_integrity(
        after,
        table_name,
        customers=customers,
        products=products,
    )
    after = business_logic.apply_business_logic(
        after,
        table_name,
        customers=customers,
        as_of_date=as_of_date,
    )
    combined = combine_quality_status(after)
    assert_no_row_loss(dataframe, combined, context=f"silver orchestrator:{table_name}")
    assert_source_columns_unchanged(
        dataframe, combined, context=f"silver orchestrator:{table_name}"
    )
    return combined


def _attach_expected(metrics: Sequence[CheckMetrics]) -> list[CheckMetrics]:
    attached: list[CheckMetrics] = []
    for item in metrics:
        expected = INTENTIONAL_FAIL_COUNTS.get((item.table_name, item.check_name))
        attached.append(item.with_expected(expected))
    return attached


def collect_silver_metrics(
    silver_tables: dict[str, Any],
) -> list[CheckMetrics]:
    """
    Rule-level, uniqueness distinct-key, and table-outcome metrics.

    Rule-level failed counts must not be summed into distinct FAIL rows.
    Table-outcome metrics use combined ``quality_check_result``.
    """
    all_metrics: list[CheckMetrics] = []
    for table_name, frame in silver_tables.items():
        all_metrics.extend(completeness.completeness_metrics(frame, table_name))
        all_metrics.extend(uniqueness.uniqueness_metrics(frame, table_name))
        all_metrics.append(
            collect_distinct_key_metrics(
                frame,
                table_name=table_name,
                check_name=UNIQUENESS_KEY_METRIC_NAMES[table_name],
                key=uniqueness.uniqueness_key_for(table_name),
                pass_column="uniqueness_pass",
                expected_fail_count=INTENTIONAL_FAIL_COUNTS.get(
                    (table_name, UNIQUENESS_KEY_METRIC_NAMES[table_name])
                ),
            )
        )
        all_metrics.extend(type_validation.type_validation_metrics(frame, table_name))
        all_metrics.extend(
            referential_integrity.referential_integrity_metrics(frame, table_name)
        )
        all_metrics.extend(business_logic.business_logic_metrics(frame, table_name))
        all_metrics.append(collect_table_outcome_metrics(frame, table_name=table_name))
    return _attach_expected(all_metrics)


def build_silver_dataframes(
    spark: Any,
    config: PipelineConfig,
    *,
    as_of_date: date | None = None,
    tables: tuple[str, ...] = SILVER_ENTITY_TABLES,
) -> SilverBuildResult:
    """Build in-memory Silver DataFrames from Bronze. Does not write."""
    customers_bronze = spark.table(config.bronze_table("customers"))
    orders_bronze = spark.table(config.bronze_table("orders"))
    products = spark.table(config.bronze_table("products"))
    bronze_by_name = {
        "customers": customers_bronze,
        "orders": orders_bronze,
        "products": products,
    }
    silver_tables: dict[str, Any] = {}
    table_results: dict[str, SilverTableResult] = {}
    for table_name in tables:
        if table_name not in bronze_by_name:
            raise QualityError(f"Unknown Silver entity {table_name!r}.")
        bronze = bronze_by_name[table_name]
        silver = apply_all_quality_modules(
            bronze,
            table_name,
            customers=customers_bronze,
            products=products,
            as_of_date=as_of_date,
        )
        bronze_rows = bronze.count()
        silver_rows = silver.count()
        if bronze_rows != silver_rows:
            raise QualityError(
                f"{table_name}: Silver row count {silver_rows} != Bronze {bronze_rows}."
            )
        fail_rows = silver.filter(
            silver[QUALITY_CHECK_RESULT_COLUMN] == "FAIL"
        ).count()
        silver_tables[table_name] = silver
        table_results[table_name] = SilverTableResult(
            table_name=table_name,
            bronze_rows=bronze_rows,
            silver_rows=silver_rows,
            fail_rows=fail_rows,
            pass_rows=silver_rows - fail_rows,
        )
        LOGGER.info(
            "silver %s bronze_rows=%s silver_rows=%s fail_rows=%s pass_rows=%s",
            table_name,
            bronze_rows,
            silver_rows,
            fail_rows,
            silver_rows - fail_rows,
        )
    metrics = collect_silver_metrics(silver_tables)
    computed_at = datetime.now(timezone.utc)
    return SilverBuildResult(
        tables=silver_tables,
        metrics=metrics,
        table_results=table_results,
        computed_at=computed_at,
    )


def _import_pyspark():
    try:
        from pyspark.sql import types as T
        from pyspark.sql.utils import AnalysisException
    except ImportError as exc:
        raise QualityError(
            "PySpark is not installed. Silver table creation requires Spark. "
            "On Databricks, run this on a cluster with an active SparkSession. "
            "For local validation install PySpark and JDK 11 or 17. "
            "Do not treat a missing Spark runtime as a successful Databricks run."
        ) from exc
    return T, AnalysisException


def quality_metrics_schema() -> Any:
    T, _AnalysisException = _import_pyspark()
    return T.StructType(
        [
            T.StructField("table_name", T.StringType(), False),
            T.StructField("check_name", T.StringType(), False),
            T.StructField("total_evaluated", T.LongType(), False),
            T.StructField("pass_count", T.LongType(), False),
            T.StructField("fail_count", T.LongType(), False),
            T.StructField("pass_pct", T.DecimalType(7, 4), False),
            T.StructField("fail_pct", T.DecimalType(7, 4), False),
            T.StructField("expected_fail_count", T.LongType(), True),
            T.StructField("population_kind", T.StringType(), False),
            T.StructField("computed_at", T.TimestampType(), False),
        ]
    )


def metrics_to_rows(
    metrics: Sequence[CheckMetrics], *, computed_at: datetime
) -> list[tuple]:
    naive = computed_at.astimezone(timezone.utc).replace(tzinfo=None)
    rows = []
    for item in metrics:
        rows.append(
            (
                item.table_name,
                item.check_name,
                int(item.total_evaluated),
                int(item.passed),
                int(item.failed),
                item.pass_pct,
                item.fail_pct,
                None if item.expected_fail_count is None else int(item.expected_fail_count),
                item.population_kind,
                naive,
            )
        )
    return rows


def ensure_silver_schema(spark: Any, config: PipelineConfig) -> None:
    qualified = config.qualified_schema(config.silver_schema)
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified}")
    except Exception as exc:
        raise QualityError(
            f"Cannot create schema {qualified}. On Databricks, set MEDALLION_CATALOG "
            "to an existing Unity Catalog catalog if you are using UC. "
            f"Original error: {exc.__class__.__name__}: {exc}"
        ) from exc


def _format_write_error(config: PipelineConfig, qualified: str, exc: BaseException) -> QualityError:
    text = str(exc).lower()
    if config.table_format == "delta" and "delta" in text:
        return QualityError(
            f"Failed to write {qualified} as delta. On Databricks use a cluster with "
            "Delta. For local Spark without Delta set MEDALLION_TABLE_FORMAT=parquet. "
            f"Original error: {exc.__class__.__name__}"
        )
    return QualityError(
        f"Failed to write {qualified} as {config.table_format}: "
        f"{exc.__class__.__name__}: {exc}"
    )


def write_silver_entity(
    spark: Any, dataframe: Any, config: PipelineConfig, table_name: str
) -> str:
    ensure_silver_schema(spark, config)
    qualified = config.silver_table(table_name)
    LOGGER.info("Overwriting Silver entity table %s (format=%s)", qualified, config.table_format)
    try:
        (
            dataframe.write.mode("overwrite")
            .format(config.table_format)
            .option("overwriteSchema", "true")
            .saveAsTable(qualified)
        )
    except Exception as exc:
        raise _format_write_error(config, qualified, exc) from exc
    return qualified


def write_quality_metrics(
    spark: Any,
    config: PipelineConfig,
    metrics: Sequence[CheckMetrics],
    *,
    computed_at: datetime,
) -> str:
    T, _AnalysisException = _import_pyspark()
    ensure_silver_schema(spark, config)
    schema = quality_metrics_schema()
    rows = metrics_to_rows(metrics, computed_at=computed_at)
    frame = spark.createDataFrame(rows, schema=schema)
    qualified = config.silver_table(SILVER_METRICS_TABLE)
    LOGGER.info(
        "Overwriting Silver quality metrics %s (%s checks)",
        qualified,
        len(metrics),
    )
    try:
        (
            frame.write.mode("overwrite")
            .format(config.table_format)
            .option("overwriteSchema", "true")
            .saveAsTable(qualified)
        )
    except Exception as exc:
        raise _format_write_error(config, qualified, exc) from exc
    return qualified


def create_silver_tables(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    as_of_date: date | None = None,
    tables: tuple[str, ...] = SILVER_ENTITY_TABLES,
    write: bool = True,
) -> SilverBuildResult:
    """
    Orchestrate all five Silver modules and optionally write tables.

    Full-refresh overwrite of Silver entity tables and quality_metrics.
    Does not mutate Bronze.
    """
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    result = build_silver_dataframes(
        session, cfg, as_of_date=as_of_date, tables=tables
    )
    if write:
        for table_name, frame in result.tables.items():
            write_silver_entity(session, frame, cfg, table_name)
        write_quality_metrics(
            session, cfg, result.metrics, computed_at=result.computed_at
        )
    for item in result.metrics:
        LOGGER.info(
            "metrics table=%s rule=%s kind=%s total=%s passed=%s failed=%s "
            "pass_pct=%s fail_pct=%s expected_failed=%s",
            item.table_name,
            item.check_name,
            item.population_kind,
            item.total_evaluated,
            item.passed,
            item.failed,
            item.pass_pct,
            item.fail_pct,
            item.expected_fail_count,
        )
    return result


def _cli() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Create Silver tables: five quality modules, combined flags, metrics."
    )
    parser.add_argument("--data-path", default=None)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--bronze-schema", default=None)
    parser.add_argument("--silver-schema", default=None)
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
        silver_schema=args.silver_schema,
        table_format=args.table_format,
    )
    create_silver_tables(config=config)


if __name__ == "__main__":
    _cli()
