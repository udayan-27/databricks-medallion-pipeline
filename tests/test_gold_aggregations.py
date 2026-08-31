"""
Gold aggregation Spark tests.

Requires a local PySpark runtime. If PySpark is not installed, the entire
module is skipped — that is BLOCKED runtime evidence, not a pass.

Uses Bronze ingest + Silver orchestration so Gold eligibility is the real
combined quality_check_result, not a hand-stamped flag.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
BRONZE_DIR = SRC_DIR / "bronze"
SILVER_DIR = SRC_DIR / "silver"
GOLD_DIR = SRC_DIR / "gold"
GOLD_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "gold"
DATA_DIR = REPO_ROOT / "data"

for _path in (str(SRC_DIR), str(BRONZE_DIR), str(SILVER_DIR), str(GOLD_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window

    PYSPARK_AVAILABLE = True
    PYSPARK_IMPORT_ERROR = ""
except ImportError as exc:  # pragma: no cover - depends on the environment
    SparkSession = None  # type: ignore[misc, assignment]
    F = None  # type: ignore[misc, assignment]
    T = None  # type: ignore[misc, assignment]
    Window = None  # type: ignore[misc, assignment]
    PYSPARK_AVAILABLE = False
    PYSPARK_IMPORT_ERROR = str(exc)

from config import load_config  # noqa: E402
from ingest_core import ingest_all  # noqa: E402
from spark_local import apply_local_spark_config  # noqa: E402

import create_gold_tables as create_gold  # noqa: E402
import create_silver_tables as create_silver  # noqa: E402

HIGH_VALUE_THRESHOLD = Decimal("1000.00")
SEGMENT_TYPES = ("Inactive", "High-Value", "Repeat", "One-Time")


def _as_decimal(value) -> Decimal:
    if value is None:
        raise AssertionError("expected a DECIMAL value, got None")
    return Decimal(str(value))


def _money(value: Decimal) -> Decimal:
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Gold Spark tests are BLOCKED in this environment.",
)
class GoldSparkTestCase(unittest.TestCase):
    spark = None
    warehouse_dir: Path

    @classmethod
    def setUpClass(cls) -> None:
        cls.warehouse_dir = Path(tempfile.mkdtemp(prefix="de_c1_gold_wh_"))
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("de-c1-gold-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.warehouse.dir", cls.warehouse_dir.as_posix())
        )
        builder = apply_local_spark_config(builder)
        existing = SparkSession.getActiveSession()
        if existing is not None:
            existing.stop()
        cls.spark = builder.getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls) -> None:
        if cls.spark is not None:
            cls.spark.stop()
            cls.spark = None

    @staticmethod
    def _config(data_path: Path, prefix: str):
        return load_config(
            data_path=str(data_path),
            catalog=None,
            bronze_schema=f"{prefix}_bronze",
            silver_schema=f"{prefix}_silver",
            gold_schema=f"{prefix}_gold",
            table_format="parquet",
        )

    def _eligible(self, config):
        silver = config.qualified_schema(config.silver_schema)
        return self.spark.sql(
            f"""
            SELECT *
            FROM {silver}.orders
            WHERE order_status = 'Completed'
              AND quality_check_result = 'PASS'
            """
        )

    def _gold(self, table_name: str):
        return self.spark.table(self.config.gold_table(table_name))

    def _eligible_totals(self, config):
        silver = config.qualified_schema(config.silver_schema)
        row = self.spark.sql(
            f"""
            SELECT
              CAST(COUNT(*) AS BIGINT) AS total_orders,
              CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue
            FROM {silver}.orders
            WHERE order_status = 'Completed'
              AND quality_check_result = 'PASS'
            """
        ).collect()[0]
        return int(row["total_orders"] or 0), row["total_revenue"]


def _segment_of(total_orders: int, total_revenue: Decimal) -> str:
    if total_orders == 0:
        return "Inactive"
    if total_revenue >= HIGH_VALUE_THRESHOLD:
        return "High-Value"
    if total_orders >= 2:
        return "Repeat"
    if total_orders == 1:
        return "One-Time"
    raise AssertionError(f"unlabeled customer orders={total_orders} revenue={total_revenue}")


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Gold Spark tests are BLOCKED in this environment.",
)
class TestGoldFixturePipeline(GoldSparkTestCase):
    """Adversarial fixture through Bronze → Silver → Gold."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.config = cls._config(GOLD_FIXTURE_DIR, "gfix")
        ingest_all(cls.spark, cls.config)
        create_silver.create_silver_tables(cls.spark, cls.config, write=True)
        cls.result = create_gold.create_gold_tables(cls.spark, cls.config, write=True)

    def test_output_types_are_decimal_not_double(self) -> None:
        frames = (
            (self._gold("sales_by_product"), ("total_revenue", "avg_order_value")),
            (
                self._gold("revenue_by_customer"),
                ("total_revenue", "avg_order_value", "lifetime_value_actual"),
            ),
            (self._gold("daily_trends"), ("total_revenue", "avg_order_value")),
            (self._gold("weekly_trends"), ("total_revenue", "avg_order_value")),
            (self._gold("customer_segmentation"), ("avg_revenue", "total_revenue")),
        )
        for frame, money_cols in frames:
            fields = {field.name: field.dataType for field in frame.schema.fields}
            for col in money_cols:
                self.assertEqual(
                    str(fields[col]),
                    "DecimalType(18,2)",
                    f"{col} was {fields[col]}",
                )
                self.assertNotIsInstance(fields[col], T.DoubleType)
                self.assertNotIsInstance(fields[col], T.FloatType)

    def test_revenue_and_order_reconciliation(self) -> None:
        expected_orders, expected_revenue = self._eligible_totals(self.config)
        self.assertEqual(expected_orders, 17)
        self.assertEqual(_as_decimal(expected_revenue), Decimal("3330.00"))
        product_row = self._gold("sales_by_product").agg(
            F.sum("total_orders").alias("orders"),
            F.sum("total_revenue").alias("revenue"),
        ).collect()[0]
        customer_row = self._gold("revenue_by_customer").agg(
            F.sum("total_orders").alias("orders"),
            F.sum("total_revenue").alias("revenue"),
        ).collect()[0]
        daily_row = self._gold("daily_trends").agg(
            F.sum("total_orders").alias("orders"),
            F.sum("total_revenue").alias("revenue"),
        ).collect()[0]
        weekly_row = self._gold("weekly_trends").agg(
            F.sum("total_orders").alias("orders"),
            F.sum("total_revenue").alias("revenue"),
        ).collect()[0]
        segment_rev = self._gold("customer_segmentation").agg(
            F.sum("total_revenue").alias("revenue")
        ).collect()[0]
        self.assertEqual(int(product_row["orders"]), expected_orders)
        self.assertEqual(int(customer_row["orders"]), expected_orders)
        self.assertEqual(int(daily_row["orders"]), expected_orders)
        self.assertEqual(int(weekly_row["orders"]), expected_orders)
        self.assertEqual(_as_decimal(product_row["revenue"]), _as_decimal(expected_revenue))
        self.assertEqual(_as_decimal(customer_row["revenue"]), _as_decimal(expected_revenue))
        self.assertEqual(_as_decimal(daily_row["revenue"]), _as_decimal(expected_revenue))
        self.assertEqual(_as_decimal(weekly_row["revenue"]), _as_decimal(expected_revenue))
        self.assertEqual(_as_decimal(segment_rev["revenue"]), _as_decimal(expected_revenue))

    def test_duplicate_orders_do_not_amplify_revenue(self) -> None:
        silver_orders = self.spark.table(self.config.silver_table("orders"))
        dup = silver_orders.filter(F.col("order_id") == 99)
        self.assertEqual(dup.count(), 2)
        self.assertEqual(dup.filter(F.col("quality_check_result") == "FAIL").count(), 2)
        self.assertEqual(self._eligible(self.config).filter(F.col("order_id") == 99).count(), 0)
        alice = self._gold("revenue_by_customer").filter(F.col("customer_id") == 1).collect()[0]
        self.assertEqual(int(alice["total_orders"]), 2)
        self.assertEqual(_as_decimal(alice["total_revenue"]), Decimal("1000.00"))
        widget = self._gold("sales_by_product").filter(F.col("product_id") == 100).collect()[0]
        self.assertEqual(_as_decimal(widget["total_revenue"]), Decimal("1260.01"))

    def test_failed_null_orphan_and_status_rows_are_excluded(self) -> None:
        eligible = self._eligible(self.config)
        self.assertEqual(eligible.filter(F.col("customer_id").isNull()).count(), 0)
        self.assertEqual(eligible.filter(F.col("product_id").isNull()).count(), 0)
        self.assertEqual(eligible.filter(F.col("customer_id") == 90001).count(), 0)
        self.assertEqual(eligible.filter(F.col("product_id") == 9001).count(), 0)
        self.assertEqual(eligible.filter(F.col("order_status") != "Completed").count(), 0)
        self.assertEqual(eligible.filter(F.col("quality_check_result") != "PASS").count(), 0)
        self.assertEqual(eligible.filter(F.col("order_id") == 20).count(), 0)
        eve = self._gold("revenue_by_customer").filter(F.col("customer_id") == 5).collect()[0]
        self.assertEqual(int(eve["total_orders"]), 0)
        self.assertEqual(_as_decimal(eve["total_revenue"]), Decimal("0.00"))
        self.assertIsNone(eve["avg_order_value"])
        self.assertEqual(_as_decimal(eve["lifetime_value_actual"]), Decimal("0.00"))

    def test_zero_order_and_null_email_customers_are_kept(self) -> None:
        customers = self._gold("revenue_by_customer")
        self.assertEqual(customers.count(), 12)
        dan = customers.filter(F.col("customer_id") == 4).collect()[0]
        self.assertEqual(int(dan["total_orders"]), 0)
        self.assertIsNone(dan["avg_order_value"])
        grace = customers.filter(F.col("customer_id") == 7).collect()[0]
        self.assertEqual(_as_decimal(grace["total_revenue"]), Decimal("100.00"))
        frank_rows = customers.filter(F.col("customer_id") == 6)
        self.assertEqual(frank_rows.count(), 1)
        self.assertEqual(_as_decimal(frank_rows.collect()[0]["total_revenue"]), Decimal("50.00"))

    def test_lifetime_value_actual_is_order_revenue_not_source(self) -> None:
        iris = self._gold("revenue_by_customer").filter(F.col("customer_id") == 8).collect()[0]
        self.assertEqual(_as_decimal(iris["lifetime_value_actual"]), Decimal("30.00"))
        self.assertEqual(_as_decimal(iris["total_revenue"]), Decimal("30.00"))
        self.assertNotEqual(_as_decimal(iris["lifetime_value_actual"]), Decimal("99999.00"))
        silver_customers = self.spark.table(self.config.silver_table("customers"))
        source = silver_customers.filter(F.col("customer_id") == 8).select("lifetime_value").collect()[0]
        self.assertEqual(_as_decimal(source["lifetime_value"]), Decimal("99999.00"))

    def test_product_customer_consistency_and_unused_product(self) -> None:
        sales = self._gold("sales_by_product")
        customers = self._gold("revenue_by_customer")
        self.assertEqual(sales.filter(F.col("product_id") == 102).count(), 0)
        self.assertEqual(sales.select("product_id").distinct().count(), sales.count())
        eligible = self._eligible(self.config)
        by_product = {
            row["product_id"]: (int(row["orders"]), _as_decimal(row["revenue"]))
            for row in eligible.groupBy("product_id")
            .agg(F.count("*").alias("orders"), F.sum("total_amount").alias("revenue"))
            .collect()
        }
        for row in sales.collect():
            expected = by_product[row["product_id"]]
            self.assertEqual(int(row["total_orders"]), expected[0])
            self.assertEqual(_as_decimal(row["total_revenue"]), expected[1])
        by_customer = {
            row["customer_id"]: (int(row["orders"]), _as_decimal(row["revenue"]))
            for row in eligible.groupBy("customer_id")
            .agg(F.count("*").alias("orders"), F.sum("total_amount").alias("revenue"))
            .collect()
        }
        for row in customers.collect():
            expected = by_customer.get(row["customer_id"], (0, Decimal("0.00")))
            self.assertEqual(int(row["total_orders"]), expected[0])
            self.assertEqual(_as_decimal(row["total_revenue"]), expected[1])

    def test_avg_order_value_formula_and_rounding(self) -> None:
        customers = self._gold("revenue_by_customer")
        nora = customers.filter(F.col("customer_id") == 11).collect()[0]
        self.assertEqual(_as_decimal(nora["total_revenue"]), Decimal("1000.00"))
        self.assertEqual(int(nora["total_orders"]), 1)
        self.assertEqual(_as_decimal(nora["avg_order_value"]), Decimal("1000.00"))
        pat = customers.filter(F.col("customer_id") == 12).collect()[0]
        self.assertEqual(_as_decimal(pat["total_revenue"]), Decimal("10.01"))
        self.assertEqual(int(pat["total_orders"]), 2)
        self.assertEqual(_as_decimal(pat["avg_order_value"]), _money(Decimal("10.01") / 2))
        gadget = self._gold("sales_by_product").filter(F.col("product_id") == 101).collect()[0]
        self.assertEqual(_as_decimal(gadget["total_revenue"]), Decimal("2069.99"))
        self.assertEqual(int(gadget["total_orders"]), 5)
        self.assertEqual(
            _as_decimal(gadget["avg_order_value"]),
            _money(Decimal("2069.99") / 5),
        )

    def test_segmentation_exclusive_exhaustive_and_boundaries(self) -> None:
        customers = self._gold("revenue_by_customer")
        segments = self._gold("customer_segmentation")
        by_id = {row["customer_id"]: row for row in customers.collect()}
        labels = {
            cid: _segment_of(int(row["total_orders"]), _as_decimal(row["total_revenue"]))
            for cid, row in by_id.items()
        }
        self.assertEqual(labels[1], "High-Value")
        self.assertEqual(labels[11], "High-Value")
        self.assertEqual(labels[2], "One-Time")
        self.assertEqual(_as_decimal(by_id[2]["total_revenue"]), Decimal("999.99"))
        self.assertEqual(labels[3], "Repeat")
        self.assertEqual(labels[4], "Inactive")
        self.assertEqual(labels[5], "Inactive")
        self.assertEqual(labels[12], "Repeat")
        self.assertEqual(len(labels), 12)
        self.assertEqual(len(set(labels.values()) - set(SEGMENT_TYPES)), 0)
        counts = {name: 0 for name in SEGMENT_TYPES}
        revenues = {name: Decimal("0.00") for name in SEGMENT_TYPES}
        for cid, name in labels.items():
            counts[name] += 1
            revenues[name] += _as_decimal(by_id[cid]["total_revenue"])
        gold_rows = {row["segment_type"]: row for row in segments.collect()}
        self.assertEqual(set(gold_rows), set(SEGMENT_TYPES))
        self.assertEqual(segments.count(), 4)
        self.assertEqual(sum(int(row["customer_count"]) for row in gold_rows.values()), 12)
        for name in SEGMENT_TYPES:
            self.assertEqual(int(gold_rows[name]["customer_count"]), counts[name])
            self.assertEqual(_as_decimal(gold_rows[name]["total_revenue"]), revenues[name])
        self.assertEqual(counts["High-Value"], 2)
        self.assertEqual(counts["Repeat"], 4)
        self.assertEqual(counts["One-Time"], 4)
        self.assertEqual(counts["Inactive"], 2)
        intersections = (
            ("High-Value", "Repeat"),
            ("High-Value", "One-Time"),
            ("Repeat", "One-Time"),
            ("Inactive", "High-Value"),
            ("Inactive", "Repeat"),
            ("Inactive", "One-Time"),
        )
        for left, right in intersections:
            self.assertEqual(
                set(cid for cid, name in labels.items() if name == left)
                & set(cid for cid, name in labels.items() if name == right),
                set(),
            )

    def test_weekly_monday_boundaries(self) -> None:
        sunday_week = self.spark.sql("SELECT CAST(date_trunc('WEEK', DATE '2026-08-30') AS DATE) AS w").collect()[0]["w"]
        monday_week = self.spark.sql("SELECT CAST(date_trunc('WEEK', DATE '2026-08-31') AS DATE) AS w").collect()[0]["w"]
        tuesday_week = self.spark.sql("SELECT CAST(date_trunc('WEEK', DATE '2026-09-01') AS DATE) AS w").collect()[0]["w"]
        self.assertEqual(sunday_week, date(2026, 8, 24))
        self.assertEqual(monday_week, date(2026, 8, 31))
        self.assertEqual(tuesday_week, date(2026, 8, 31))
        weeks = {row["week_start_date"]: row for row in self._gold("weekly_trends").collect()}
        self.assertIn(date(2026, 8, 24), weeks)
        self.assertIn(date(2026, 8, 31), weeks)
        self.assertEqual(int(weeks[date(2026, 8, 24)]["total_orders"]), 1)
        self.assertEqual(_as_decimal(weeks[date(2026, 8, 24)]["total_revenue"]), Decimal("20.00"))
        self.assertEqual(int(weeks[date(2026, 8, 31)]["total_orders"]), 16)
        self.assertEqual(_as_decimal(weeks[date(2026, 8, 31)]["total_revenue"]), Decimal("3310.00"))
        days = {row["trend_date"]: row for row in self._gold("daily_trends").collect()}
        self.assertEqual(int(days[date(2026, 8, 30)]["total_orders"]), 1)
        self.assertEqual(int(days[date(2026, 8, 31)]["total_orders"]), 15)
        self.assertEqual(int(days[date(2026, 9, 1)]["total_orders"]), 1)

    def test_repeated_gold_execution_is_stable(self) -> None:
        before = (
            self._gold("revenue_by_customer")
            .select("customer_id", "total_revenue", "lifetime_value_actual")
            .collect()
        )
        before_counts = {
            name: self._gold(name).count() for name in create_gold.GOLD_TABLES
        }
        again = create_gold.create_gold_tables(self.spark, self.config, write=True)
        for table_name in create_gold.GOLD_TABLES:
            self.assertEqual(
                again.table_results[table_name].row_count,
                before_counts[table_name],
            )
            self.assertEqual(
                again.table_results[table_name].row_count,
                self.result.table_results[table_name].row_count,
            )
        after = (
            self._gold("revenue_by_customer")
            .select("customer_id", "total_revenue", "lifetime_value_actual")
            .collect()
        )
        self.assertEqual(sorted(before), sorted(after))


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Gold Spark tests are BLOCKED in this environment.",
)
class TestGoldZeroEligibleOrders(GoldSparkTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.config = cls._config(GOLD_FIXTURE_DIR, "gzero")
        ingest_all(cls.spark, cls.config)
        create_silver.create_silver_tables(cls.spark, cls.config, write=True)
        orders_schema = cls.spark.table(cls.config.silver_table("orders")).schema
        empty_orders = cls.spark.createDataFrame([], schema=orders_schema)
        (
            empty_orders.write.mode("overwrite")
            .format("parquet")
            .option("overwriteSchema", "true")
            .saveAsTable(cls.config.silver_table("orders"))
        )
        cls.result = create_gold.create_gold_tables(cls.spark, cls.config, write=True)

    def test_zero_eligible_orders_keep_customers_and_empty_facts(self) -> None:
        sales = self.spark.table(self.config.gold_table("sales_by_product"))
        customers = self.spark.table(self.config.gold_table("revenue_by_customer"))
        daily = self.spark.table(self.config.gold_table("daily_trends"))
        weekly = self.spark.table(self.config.gold_table("weekly_trends"))
        segments = self.spark.table(self.config.gold_table("customer_segmentation"))
        self.assertEqual(sales.count(), 0)
        self.assertEqual(daily.count(), 0)
        self.assertEqual(weekly.count(), 0)
        self.assertEqual(customers.count(), 12)
        self.assertEqual(customers.filter(F.col("total_orders") == 0).count(), 12)
        self.assertEqual(customers.filter(F.col("avg_order_value").isNull()).count(), 12)
        self.assertEqual(
            customers.filter(F.col("lifetime_value_actual") == F.lit(Decimal("0.00"))).count(),
            12,
        )
        by_type = {row["segment_type"]: row for row in segments.collect()}
        self.assertEqual(int(by_type["Inactive"]["customer_count"]), 12)
        self.assertEqual(int(by_type["High-Value"]["customer_count"]), 0)
        self.assertEqual(int(by_type["Repeat"]["customer_count"]), 0)
        self.assertEqual(int(by_type["One-Time"]["customer_count"]), 0)
        self.assertIsNone(by_type["High-Value"]["avg_revenue"])
        self.assertEqual(_as_decimal(by_type["Inactive"]["total_revenue"]), Decimal("0.00"))
        self.assertEqual(_as_decimal(by_type["Inactive"]["avg_revenue"]), Decimal("0.00"))


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Gold Spark tests are BLOCKED in this environment.",
)
class TestGoldSeed42Reconciliation(GoldSparkTestCase):
    """Full seed-42 Bronze → Silver → Gold. Local parquet, not Databricks."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.config = cls._config(DATA_DIR, "gfull")
        ingest_all(cls.spark, cls.config)
        create_silver.create_silver_tables(cls.spark, cls.config, write=True)
        cls.result = create_gold.create_gold_tables(cls.spark, cls.config, write=True)
        cls.sales = cls.spark.table(cls.config.gold_table("sales_by_product"))
        cls.customers = cls.spark.table(cls.config.gold_table("revenue_by_customer"))
        cls.daily = cls.spark.table(cls.config.gold_table("daily_trends"))
        cls.weekly = cls.spark.table(cls.config.gold_table("weekly_trends"))
        cls.segments = cls.spark.table(cls.config.gold_table("customer_segmentation"))

    def test_seed42_revenue_order_and_customer_population(self) -> None:
        expected_orders, expected_revenue = self._eligible_totals(self.config)
        self.assertGreater(expected_orders, 0)
        self.assertIsNotNone(expected_revenue)
        product = self.sales.agg(F.sum("total_orders"), F.sum("total_revenue")).collect()[0]
        customer = self.customers.agg(
            F.sum("total_orders"), F.sum("total_revenue")
        ).collect()[0]
        daily = self.daily.agg(F.sum("total_orders"), F.sum("total_revenue")).collect()[0]
        weekly = self.weekly.agg(F.sum("total_orders"), F.sum("total_revenue")).collect()[0]
        segment_rev = self.segments.agg(F.sum("total_revenue")).collect()[0][0]
        self.assertEqual(int(product[0]), expected_orders)
        self.assertEqual(int(customer[0]), expected_orders)
        self.assertEqual(int(daily[0]), expected_orders)
        self.assertEqual(int(weekly[0]), expected_orders)
        self.assertEqual(_as_decimal(product[1]), _as_decimal(expected_revenue))
        self.assertEqual(_as_decimal(customer[1]), _as_decimal(expected_revenue))
        self.assertEqual(_as_decimal(daily[1]), _as_decimal(expected_revenue))
        self.assertEqual(_as_decimal(weekly[1]), _as_decimal(expected_revenue))
        self.assertEqual(_as_decimal(segment_rev), _as_decimal(expected_revenue))
        self.assertEqual(self.customers.count(), 10000)
        self.assertEqual(
            sum(int(row["customer_count"]) for row in self.segments.collect()),
            10000,
        )
        silver_customers = self.spark.table(self.config.silver_table("customers"))
        distinct_ids = silver_customers.filter(F.col("customer_id").isNotNull()).select(
            "customer_id"
        ).distinct().count()
        self.assertEqual(distinct_ids, 10000)

    def test_seed42_failed_and_duplicate_orders_excluded(self) -> None:
        silver_orders = self.spark.table(self.config.silver_table("orders"))
        eligible = self._eligible(self.config)
        fail_completed = silver_orders.filter(
            (F.col("order_status") == "Completed")
            & (F.col("quality_check_result") == "FAIL")
        )
        self.assertGreater(fail_completed.count(), 0)
        self.assertEqual(eligible.filter(F.col("quality_check_result") == "FAIL").count(), 0)
        uniq_fail = silver_orders.filter(F.col("uniqueness_pass") == F.lit(False))
        self.assertEqual(uniq_fail.count(), 40)
        self.assertEqual(eligible.filter(F.col("uniqueness_pass") == F.lit(False)).count(), 0)
        self.assertEqual(eligible.filter(F.col("customer_id").isNull()).count(), 0)
        self.assertEqual(eligible.filter(F.col("product_id").isNull()).count(), 0)

    def test_seed42_segmentation_exclusive_and_zero_order_customers(self) -> None:
        labels = self.customers.select(
            "customer_id",
            F.when(F.col("total_orders") == 0, F.lit("Inactive"))
            .when(F.col("total_revenue") >= F.lit(Decimal("1000.00")), F.lit("High-Value"))
            .when(F.col("total_orders") >= 2, F.lit("Repeat"))
            .when(F.col("total_orders") == 1, F.lit("One-Time"))
            .alias("segment_type"),
        )
        self.assertEqual(labels.filter(F.col("segment_type").isNull()).count(), 0)
        counted = {
            row["segment_type"]: int(row["n"])
            for row in labels.groupBy("segment_type").count().withColumnRenamed("count", "n").collect()
        }
        gold_counts = {
            row["segment_type"]: int(row["customer_count"])
            for row in self.segments.collect()
        }
        self.assertEqual(set(gold_counts), set(SEGMENT_TYPES))
        for name in SEGMENT_TYPES:
            self.assertEqual(gold_counts[name], counted.get(name, 0), name)
        inactive = self.customers.filter(F.col("total_orders") == 0)
        self.assertEqual(inactive.count(), gold_counts["Inactive"])
        self.assertGreaterEqual(inactive.count(), 30)
        self.assertEqual(inactive.filter(F.col("avg_order_value").isNotNull()).count(), 0)
        boundary = self.customers.filter(F.col("total_revenue") == F.lit(Decimal("1000.00")))
        if boundary.count() > 0:
            self.assertEqual(
                labels.join(boundary, "customer_id")
                .filter(F.col("segment_type") != "High-Value")
                .count(),
                0,
            )
        just_below = self.customers.filter(
            (F.col("total_revenue") == F.lit(Decimal("999.99")))
            & (F.col("total_orders") > 0)
        )
        if just_below.count() > 0:
            self.assertEqual(
                labels.join(just_below, "customer_id")
                .filter(F.col("segment_type") == "High-Value")
                .count(),
                0,
            )

    def test_seed42_lifetime_value_actual_not_source_column(self) -> None:
        silver_customers = self.spark.table(self.config.silver_table("customers"))
        canonical = (
            silver_customers.filter(F.col("customer_id").isNotNull())
            .withColumn(
                "_rn",
                F.row_number().over(
                    Window.partitionBy("customer_id").orderBy("_ingest_row_id")
                ),
            )
            .filter(F.col("_rn") == 1)
            .select(
                F.col("customer_id"),
                F.col("lifetime_value").alias("source_ltv"),
            )
        )
        compared = self.customers.join(canonical, "customer_id")
        mismatches = compared.filter(F.col("lifetime_value_actual") != F.col("source_ltv"))
        self.assertGreater(mismatches.count(), 0)
        self.assertEqual(
            self.customers.filter(
                F.col("lifetime_value_actual") != F.col("total_revenue")
            ).count(),
            0,
        )


if __name__ == "__main__":
    unittest.main()
