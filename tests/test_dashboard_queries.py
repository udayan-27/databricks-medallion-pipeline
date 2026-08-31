"""
Dashboard query Spark tests.

Requires a local PySpark runtime. If PySpark is not installed, the entire
module is skipped — that is BLOCKED runtime evidence, not a pass.

Tile queries are executed with spark.sql against Gold tables. One class
rebuilds Gold through Bronze → Silver → Gold using the Gold fixture so
dashboard numbers reconcile with Gold. A second class writes Gold-shaped
tables directly for Top-N, tie, null, zero, and empty-segment edges.

Run Spark suites sequentially (one process). Concurrent full suites can
collide on Windows temp files during JVM gateway launch.
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
BRONZE_DIR = SRC_DIR / "bronze"
SILVER_DIR = SRC_DIR / "silver"
GOLD_DIR = SRC_DIR / "gold"
GOLD_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "gold"

for _path in (str(SRC_DIR), str(BRONZE_DIR), str(SILVER_DIR), str(GOLD_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

try:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    PYSPARK_AVAILABLE = True
    PYSPARK_IMPORT_ERROR = ""
except ImportError as exc:  # pragma: no cover - depends on the environment
    F = None  # type: ignore[misc, assignment]
    T = None  # type: ignore[misc, assignment]
    PYSPARK_AVAILABLE = False
    PYSPARK_IMPORT_ERROR = str(exc)

from config import load_config  # noqa: E402
from ingest_core import ingest_all  # noqa: E402
from spark_local import start_local_test_spark, stop_local_test_spark  # noqa: E402

import create_gold_tables as create_gold  # noqa: E402
import create_silver_tables as create_silver  # noqa: E402

DASHBOARD_SQL_PATH = REPO_ROOT / "src" / "dashboard" / "dashboard_queries.sql"
DASHBOARD_QUERY_PREFIX = "-- DASHBOARD_QUERY:"
SEGMENT_TYPES = ("Inactive", "High-Value", "Repeat", "One-Time")


def _split_dashboard_queries(sql_text: str) -> dict[str, str]:
    parts: dict[str, str] = {}
    current_name: str | None = None
    current_lines: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith(DASHBOARD_QUERY_PREFIX):
            if current_name is not None:
                parts[current_name] = "\n".join(current_lines).strip().rstrip(";").strip()
            current_name = stripped[len(DASHBOARD_QUERY_PREFIX) :].strip()
            current_lines = []
        elif current_name is not None:
            current_lines.append(line)
    if current_name is not None:
        parts[current_name] = "\n".join(current_lines).strip().rstrip(";").strip()
    return parts


def _render_dashboard_sql(sql_text: str, gold_schema: str) -> str:
    return sql_text.replace("{gold_schema}", gold_schema)


def _as_decimal(value) -> Decimal:
    if value is None:
        raise AssertionError("expected a DECIMAL value, got None")
    return Decimal(str(value))


def _money_schema():
    return T.DecimalType(18, 2)


def _sales_schema():
    return T.StructType(
        [
            T.StructField("product_id", T.IntegerType(), False),
            T.StructField("product_name", T.StringType(), True),
            T.StructField("category", T.StringType(), True),
            T.StructField("total_orders", T.LongType(), False),
            T.StructField("total_revenue", _money_schema(), False),
            T.StructField("avg_order_value", T.DecimalType(18, 2), True),
        ]
    )


def _customer_schema():
    return T.StructType(
        [
            T.StructField("customer_id", T.IntegerType(), False),
            T.StructField("customer_name", T.StringType(), True),
            T.StructField("customer_segment", T.StringType(), True),
            T.StructField("total_orders", T.LongType(), False),
            T.StructField("total_revenue", _money_schema(), False),
            T.StructField("avg_order_value", T.DecimalType(18, 2), True),
            T.StructField("lifetime_value_actual", _money_schema(), False),
        ]
    )


def _segment_schema():
    return T.StructType(
        [
            T.StructField("segment_type", T.StringType(), False),
            T.StructField("customer_count", T.LongType(), False),
            T.StructField("avg_revenue", T.DecimalType(18, 2), True),
            T.StructField("total_revenue", _money_schema(), False),
        ]
    )


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Dashboard Spark tests are BLOCKED in this environment.",
)
class DashboardSparkTestCase(unittest.TestCase):
    spark = None
    warehouse_dir: Path

    @classmethod
    def setUpClass(cls) -> None:
        cls.warehouse_dir = Path(tempfile.mkdtemp(prefix="de_c1_dash_wh_"))
        cls.spark = start_local_test_spark("de-c1-dashboard-tests", cls.warehouse_dir)

    @classmethod
    def tearDownClass(cls) -> None:
        stop_local_test_spark(cls.spark, getattr(cls, "warehouse_dir", None))
        cls.spark = None

    def _run(self, query_name: str, gold_schema: str):
        sql = _render_dashboard_sql(
            _split_dashboard_queries(DASHBOARD_SQL_PATH.read_text(encoding="utf-8"))[query_name],
            gold_schema,
        )
        return self.spark.sql(sql)

    def _write_table(self, qualified: str, frame) -> None:
        schema_name = qualified.rsplit(".", 1)[0]
        self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        (
            frame.write.mode("overwrite")
            .format("parquet")
            .option("overwriteSchema", "true")
            .saveAsTable(qualified)
        )


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Dashboard Spark tests are BLOCKED in this environment.",
)
class TestDashboardGoldFixturePipeline(DashboardSparkTestCase):
    """Dashboard SQL against Gold produced from the Gold adversarial fixture."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.config = load_config(
            data_path=str(GOLD_FIXTURE_DIR),
            catalog=None,
            bronze_schema="ddash_bronze",
            silver_schema="ddash_silver",
            gold_schema="ddash_gold",
            table_format="parquet",
        )
        ingest_all(cls.spark, cls.config)
        create_silver.create_silver_tables(cls.spark, cls.config, write=True)
        create_gold.create_gold_tables(cls.spark, cls.config, write=True)
        cls.gold = cls.config.gold_schema

    def test_top_10_row_count_order_and_gold_match(self) -> None:
        result = self._run("top_10_products", self.gold)
        gold_sales = self.spark.table(self.config.gold_table("sales_by_product"))
        expected = gold_sales.orderBy(
            F.col("total_revenue").desc(), F.col("product_id").asc()
        ).limit(10)
        self.assertLessEqual(result.count(), 10)
        self.assertEqual(result.count(), min(10, gold_sales.count()))
        self.assertEqual(result.count(), 2)
        result_ids = [row["product_id"] for row in result.collect()]
        expected_ids = [row["product_id"] for row in expected.collect()]
        self.assertEqual(result_ids, expected_ids)
        self.assertEqual(result_ids, [101, 100])
        revenues = [_as_decimal(row["total_revenue"]) for row in result.collect()]
        self.assertEqual(revenues, sorted(revenues, reverse=True))
        self.assertEqual(result.select("product_id").distinct().count(), result.count())

    def test_histogram_uses_gold_customer_population(self) -> None:
        result = self._run("customer_revenue_distribution", self.gold)
        gold_customers = self.spark.table(self.config.gold_table("revenue_by_customer"))
        self.assertEqual(result.count(), gold_customers.count())
        self.assertEqual(result.count(), 12)
        self.assertEqual(result.select("customer_id").distinct().count(), result.count())
        zeros = result.filter(F.col("lifetime_value_actual") == F.lit(Decimal("0.00")))
        gold_zeros = gold_customers.filter(F.col("total_orders") == 0)
        self.assertEqual(zeros.count(), gold_zeros.count())
        self.assertGreater(zeros.count(), 0)
        dash = result.select(
            "customer_id",
            F.col("lifetime_value_actual").alias("dash_ltv"),
        )
        gold = gold_customers.select(
            "customer_id",
            F.col("total_revenue").alias("gold_rev"),
            F.col("lifetime_value_actual").alias("gold_ltv"),
        )
        joined = dash.join(gold, "customer_id")
        self.assertEqual(joined.filter(F.col("dash_ltv") != F.col("gold_rev")).count(), 0)
        self.assertEqual(joined.filter(F.col("dash_ltv") != F.col("gold_ltv")).count(), 0)
        self.assertEqual(
            result.filter(F.col("lifetime_value_actual").isNull()).count(),
            0,
        )

    def test_segmentation_counts_reconcile_with_gold(self) -> None:
        result = self._run("customer_segmentation", self.gold)
        gold_segments = self.spark.table(self.config.gold_table("customer_segmentation"))
        gold_customers = self.spark.table(self.config.gold_table("revenue_by_customer"))
        self.assertEqual(result.count(), 4)
        self.assertEqual(result.select("segment_type").distinct().count(), 4)
        by_type = {row["segment_type"]: int(row["customer_count"]) for row in result.collect()}
        gold_counts = {
            row["segment_type"]: int(row["customer_count"]) for row in gold_segments.collect()
        }
        self.assertEqual(by_type, gold_counts)
        self.assertEqual(set(by_type), set(SEGMENT_TYPES))
        self.assertEqual(sum(by_type.values()), gold_customers.count())
        self.assertEqual(sum(by_type.values()), 12)
        self.assertEqual(by_type["High-Value"], 2)
        self.assertEqual(by_type["Repeat"], 4)
        self.assertEqual(by_type["One-Time"], 4)
        self.assertEqual(by_type["Inactive"], 2)

    def test_category_filter_applies_before_limit(self) -> None:
        gold_sales = self.spark.table(self.config.gold_table("sales_by_product"))
        filtered_sql = f"""
        SELECT product_id, product_name, category, total_revenue, total_orders
        FROM {self.gold}.sales_by_product
        WHERE category = 'Home'
        ORDER BY total_revenue DESC, product_id ASC
        LIMIT 10
        """
        filtered = self.spark.sql(filtered_sql)
        after_limit = self._run("top_10_products", self.gold).filter(
            F.col("category") == "Home"
        )
        self.assertEqual(filtered.count(), 1)
        self.assertEqual(filtered.collect()[0]["product_id"], 101)
        self.assertEqual(
            [row["product_id"] for row in filtered.collect()],
            [
                row["product_id"]
                for row in gold_sales.filter(F.col("category") == "Home")
                .orderBy(F.col("total_revenue").desc(), F.col("product_id").asc())
                .limit(10)
                .collect()
            ],
        )
        self.assertEqual(after_limit.count(), 1)

    def test_filter_value_queries_use_gold_fields(self) -> None:
        categories = self._run("filter_values_category", self.gold)
        segments = self._run("filter_values_customer_segment", self.gold)
        gold_sales = self.spark.table(self.config.gold_table("sales_by_product"))
        gold_customers = self.spark.table(self.config.gold_table("revenue_by_customer"))
        self.assertEqual(
            categories.count(),
            gold_sales.select("category").distinct().count(),
        )
        self.assertEqual(
            segments.count(),
            gold_customers.select("customer_segment").distinct().count(),
        )
        self.assertIn("Electronics", [row["category"] for row in categories.collect()])
        self.assertIn("Home", [row["category"] for row in categories.collect()])
        self.assertNotIn("Clothing", [row["category"] for row in categories.collect()])


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Dashboard Spark tests are BLOCKED in this environment.",
)
class TestDashboardQueryEdges(DashboardSparkTestCase):
    """Deterministic Gold-shaped tables; dashboard SQL only."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.schema = "dash_edge_gold"
        cls.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cls.schema}")

    def _sales_rows(self, rows):
        return self.spark.createDataFrame(rows, schema=_sales_schema())

    def _customer_rows(self, rows):
        return self.spark.createDataFrame(rows, schema=_customer_schema())

    def _segment_rows(self, rows):
        return self.spark.createDataFrame(rows, schema=_segment_schema())

    def _install_sales(self, rows) -> None:
        self._write_table(f"{self.schema}.sales_by_product", self._sales_rows(rows))

    def _install_customers(self, rows) -> None:
        self._write_table(f"{self.schema}.revenue_by_customer", self._customer_rows(rows))

    def _install_segments(self, rows) -> None:
        self._write_table(f"{self.schema}.customer_segmentation", self._segment_rows(rows))

    def test_fewer_than_ten_products(self) -> None:
        self._install_sales(
            [
                (1, "A", "Cat", 1, Decimal("30.00"), Decimal("30.00")),
                (2, "B", "Cat", 1, Decimal("20.00"), Decimal("20.00")),
                (3, "C", None, 1, Decimal("10.00"), Decimal("10.00")),
            ]
        )
        result = self._run("top_10_products", self.schema)
        self.assertEqual(result.count(), 3)
        self.assertEqual([row["product_id"] for row in result.collect()], [1, 2, 3])
        self.assertIsNone(result.collect()[2]["category"])

    def test_revenue_ties_use_product_id(self) -> None:
        self._install_sales(
            [
                (20, "Late", "X", 1, Decimal("100.00"), Decimal("100.00")),
                (5, "Early", "X", 1, Decimal("100.00"), Decimal("100.00")),
                (9, "Mid", "Y", 1, Decimal("100.00"), Decimal("100.00")),
            ]
        )
        result = self._run("top_10_products", self.schema)
        self.assertEqual([row["product_id"] for row in result.collect()], [5, 9, 20])

    def test_tie_at_tenth_place_is_deterministic(self) -> None:
        rows = []
        for pid in range(1, 9):
            rows.append(
                (
                    pid,
                    f"P{pid}",
                    "A",
                    1,
                    Decimal("200.00") - Decimal(pid),
                    Decimal("10.00"),
                )
            )
        for pid in (9, 10, 11):
            rows.append((pid, f"P{pid}", "B", 1, Decimal("50.00"), Decimal("50.00")))
        self._install_sales(rows)
        result = self._run("top_10_products", self.schema)
        self.assertEqual(result.count(), 10)
        ids = [row["product_id"] for row in result.collect()]
        self.assertEqual(ids[-3:], [8, 9, 10])
        self.assertNotIn(11, ids)

    def test_empty_products_return_zero_rows(self) -> None:
        self._install_sales([])
        result = self._run("top_10_products", self.schema)
        self.assertEqual(result.count(), 0)
        values = self._run("filter_values_category", self.schema)
        self.assertEqual(values.count(), 0)

    def test_category_filter_before_limit_not_after(self) -> None:
        rows = []
        for pid in range(1, 12):
            category = "Keep" if pid >= 6 else "Drop"
            rows.append(
                (
                    pid,
                    f"P{pid}",
                    category,
                    1,
                    Decimal("100.00") - Decimal(pid),
                    Decimal("10.00"),
                )
            )
        self._install_sales(rows)
        global_top = self._run("top_10_products", self.schema)
        after = global_top.filter(F.col("category") == "Keep")
        before = self.spark.sql(
            f"""
            SELECT product_id, product_name, category, total_revenue, total_orders
            FROM {self.schema}.sales_by_product
            WHERE category = 'Keep'
            ORDER BY total_revenue DESC, product_id ASC
            LIMIT 10
            """
        )
        self.assertLess(after.count(), before.count())
        self.assertEqual(before.count(), 6)
        self.assertEqual(after.count(), 5)
        self.assertTrue(all(row["category"] == "Keep" for row in before.collect()))

    def test_histogram_keeps_zero_revenue_and_unique_ids(self) -> None:
        self._install_customers(
            [
                (1, "A", "Premium", 2, Decimal("80.00"), Decimal("40.00"), Decimal("80.00")),
                (2, "B", "Basic", 0, Decimal("0.00"), None, Decimal("0.00")),
                (3, "C", "Standard", 1, Decimal("5.00"), Decimal("5.00"), Decimal("5.00")),
            ]
        )
        result = self._run("customer_revenue_distribution", self.schema)
        self.assertEqual(result.count(), 3)
        self.assertEqual(result.select("customer_id").distinct().count(), 3)
        zeros = result.filter(F.col("lifetime_value_actual") == F.lit(Decimal("0.00")))
        self.assertEqual(zeros.count(), 1)
        self.assertEqual(zeros.collect()[0]["customer_id"], 2)
        ids = [row["customer_id"] for row in result.collect()]
        self.assertEqual(ids, sorted(ids))

    def test_empty_and_one_segment_populations(self) -> None:
        self._install_segments(
            [
                ("Inactive", 3, Decimal("0.00"), Decimal("0.00")),
                ("High-Value", 0, None, Decimal("0.00")),
                ("Repeat", 0, None, Decimal("0.00")),
                ("One-Time", 0, None, Decimal("0.00")),
            ]
        )
        result = self._run("customer_segmentation", self.schema)
        self.assertEqual(result.count(), 4)
        by_type = {row["segment_type"]: int(row["customer_count"]) for row in result.collect()}
        self.assertEqual(set(by_type), set(SEGMENT_TYPES))
        self.assertEqual(by_type["Inactive"], 3)
        self.assertEqual(by_type["High-Value"], 0)
        self.assertEqual(sum(by_type.values()), 3)
        self._install_customers([])
        dist = self._run("customer_revenue_distribution", self.schema)
        self.assertEqual(dist.count(), 0)
        self._install_segments(
            [
                ("Inactive", 0, None, Decimal("0.00")),
                ("High-Value", 0, None, Decimal("0.00")),
                ("Repeat", 0, None, Decimal("0.00")),
                ("One-Time", 0, None, Decimal("0.00")),
            ]
        )
        empty_seg = self._run("customer_segmentation", self.schema)
        self.assertEqual(sum(int(row["customer_count"]) for row in empty_seg.collect()), 0)
        self.assertEqual(empty_seg.count(), 4)

    def test_null_customer_segment_is_a_valid_filter_value(self) -> None:
        self._install_customers(
            [
                (1, "A", "Premium", 1, Decimal("1.00"), Decimal("1.00"), Decimal("1.00")),
                (2, "B", None, 0, Decimal("0.00"), None, Decimal("0.00")),
            ]
        )
        values = self._run("filter_values_customer_segment", self.schema)
        labels = [row["customer_segment"] for row in values.collect()]
        self.assertIn("Premium", labels)
        self.assertIn(None, labels)


if __name__ == "__main__":
    unittest.main()
