"""
Silver completeness and uniqueness Spark tests.

Requires a local PySpark runtime. If PySpark is not installed, the entire
module is skipped — that is BLOCKED runtime evidence, not a pass.

Uses Bronze ingest (fixtures and committed CSVs) so NULL semantics match the
PERMISSIVE empty-field contract. Quality modules are applied in memory; this
increment does not write Silver tables.
"""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
BRONZE_DIR = SRC_DIR / "bronze"
SILVER_DIR = SRC_DIR / "silver"
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "bronze"
DATA_DIR = REPO_ROOT / "data"

for _path in (str(SRC_DIR), str(BRONZE_DIR), str(SILVER_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    PYSPARK_AVAILABLE = True
    PYSPARK_IMPORT_ERROR = ""
except ImportError as exc:  # pragma: no cover - depends on the environment
    SparkSession = None  # type: ignore[misc, assignment]
    F = None  # type: ignore[misc, assignment]
    T = None  # type: ignore[misc, assignment]
    PYSPARK_AVAILABLE = False
    PYSPARK_IMPORT_ERROR = str(exc)

from config import load_config  # noqa: E402
from contracts import INGEST_ROW_ID_COLUMN  # noqa: E402
from ingest_core import ingest_all  # noqa: E402
from spark_local import apply_local_spark_config  # noqa: E402
from quality_common import (  # noqa: E402
    MODULE_COMPLETENESS,
    MODULE_UNIQUENESS,
    QualityError,
    lineage_and_source_columns,
)


def _load_silver(filename: str, module_name: str):
    path = SILVER_DIR / filename
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


completeness = _load_silver("01_quality_completeness.py", "quality_completeness")
uniqueness = _load_silver("02_quality_uniqueness.py", "quality_uniqueness")

EMAIL_CODE = "completeness:customers.email"
ORDER_CUSTOMER_CODE = "completeness:orders.customer_id"
ORDER_PRODUCT_CODE = "completeness:orders.product_id"
CUSTOMER_UNIQ_CODE = "uniqueness:customers.customer_id"
ORDER_UNIQ_CODE = "uniqueness:orders.order_id"


def _code_count(dataframe, column: str, code: str) -> int:
    return dataframe.filter(F.array_contains(F.col(column), F.lit(code))).count()


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Silver Spark tests are BLOCKED in this environment.",
)
class SilverSparkTestCase(unittest.TestCase):
    spark = None
    warehouse_dir: Path

    @classmethod
    def setUpClass(cls) -> None:
        cls.warehouse_dir = Path(tempfile.mkdtemp(prefix="de_c1_silver_wh_"))
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("de-c1-silver-tests")
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
    def _fixture_config(data_path: Path, schema: str):
        return load_config(
            data_path=str(data_path),
            catalog=None,
            bronze_schema=schema,
            table_format="parquet",
        )


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Silver Spark tests are BLOCKED in this environment.",
)
class TestSilverSyntheticEdges(SilverSparkTestCase):
    def test_empty_string_email_is_not_null(self) -> None:
        schema = T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("email", T.StringType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        df = self.spark.createDataFrame(
            [(1, "", 1), (2, None, 2), (3, "a@example.com", 3)],
            schema=schema,
        )
        flagged = completeness.apply_completeness(df, "customers")
        self.assertEqual(flagged.count(), 3)
        self.assertEqual(_code_count(flagged, "completeness_failed_checks", EMAIL_CODE), 1)
        empty = flagged.filter(F.col("email") == "").collect()
        self.assertEqual(len(empty), 1)
        self.assertTrue(empty[0]["completeness_pass"])
        self.assertNotIn(EMAIL_CODE, list(empty[0]["completeness_failed_checks"] or []))

    def test_null_keys_are_not_uniqueness_duplicates(self) -> None:
        schema = T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("email", T.StringType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        df = self.spark.createDataFrame(
            [(None, "a@example.com", 1), (None, "b@example.com", 2), (1, "c@example.com", 3)],
            schema=schema,
        )
        flagged = uniqueness.apply_uniqueness(df, "customers")
        self.assertEqual(flagged.count(), 3)
        self.assertEqual(flagged.filter(~F.col("uniqueness_pass")).count(), 0)
        nulls = flagged.filter(F.col("customer_id").isNull())
        self.assertEqual(nulls.count(), 2)
        self.assertEqual(nulls.filter(F.col("uniqueness_pass")).count(), 2)

    def test_all_duplicate_copies_fail_and_keys_are_not_rewritten(self) -> None:
        schema = T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("email", T.StringType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        df = self.spark.createDataFrame(
            [(7, "a@example.com", 10), (7, "a@example.com", 11), (8, "b@example.com", 12)],
            schema=schema,
        )
        flagged = uniqueness.apply_uniqueness(df, "customers")
        self.assertEqual(flagged.count(), 3)
        self.assertEqual(flagged.filter(~F.col("uniqueness_pass")).count(), 2)
        self.assertEqual(_code_count(flagged, "uniqueness_failed_checks", CUSTOMER_UNIQ_CODE), 2)
        keys = [row.customer_id for row in flagged.orderBy(INGEST_ROW_ID_COLUMN).collect()]
        self.assertEqual(keys, [7, 7, 8])
        ids = [row[INGEST_ROW_ID_COLUMN] for row in flagged.collect()]
        self.assertEqual(sorted(ids), [10, 11, 12])
        self.assertNotEqual(set(ids), set(keys))

    def test_multiple_checks_accumulate_on_the_same_physical_row(self) -> None:
        schema = T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("email", T.StringType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        df = self.spark.createDataFrame(
            [(9, None, 21), (9, None, 22)],
            schema=schema,
        )
        after_c = completeness.apply_completeness(df, "customers")
        after_both = uniqueness.apply_uniqueness(after_c, "customers")
        self.assertEqual(after_both.count(), 2)
        for row in after_both.collect():
            completeness_codes = list(row["completeness_failed_checks"] or [])
            uniqueness_codes = list(row["uniqueness_failed_checks"] or [])
            self.assertIn(EMAIL_CODE, completeness_codes)
            self.assertIn(CUSTOMER_UNIQ_CODE, uniqueness_codes)
            self.assertFalse(row["completeness_pass"])
            self.assertFalse(row["uniqueness_pass"])
            self.assertNotIn(CUSTOMER_UNIQ_CODE, completeness_codes)
            self.assertNotIn(EMAIL_CODE, uniqueness_codes)

    def test_ingest_row_id_required(self) -> None:
        schema = T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("email", T.StringType(), True),
            ]
        )
        df = self.spark.createDataFrame([(1, "a@example.com")], schema=schema)
        with self.assertRaises(QualityError):
            completeness.apply_completeness(df, "customers")


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Silver Spark tests are BLOCKED in this environment.",
)
class TestSilverFixtureQuality(SilverSparkTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.config = cls._fixture_config(FIXTURE_DIR, schema="bronze_silver_fx")
        ingest_all(cls.spark, cls.config)
        customers = cls.spark.table(cls.config.bronze_table("customers"))
        orders = cls.spark.table(cls.config.bronze_table("orders"))
        products = cls.spark.table(cls.config.bronze_table("products"))
        cls.bronze_customers = customers
        cls.bronze_orders = orders
        cls.bronze_products = products
        cls.customers = uniqueness.apply_uniqueness(
            completeness.apply_completeness(customers, "customers"),
            "customers",
        )
        cls.orders = uniqueness.apply_uniqueness(
            completeness.apply_completeness(orders, "orders"),
            "orders",
        )
        cls.products = uniqueness.apply_uniqueness(
            completeness.apply_completeness(products, "products"),
            "products",
        )

    def test_fixture_rows_remain_after_validation(self) -> None:
        self.assertEqual(self.customers.count(), 4)
        self.assertEqual(self.orders.count(), 6)
        self.assertEqual(self.products.count(), 2)
        self.assertEqual(self.customers.count(), self.bronze_customers.count())
        self.assertEqual(self.orders.count(), self.bronze_orders.count())
        self.assertEqual(self.products.count(), self.bronze_products.count())

    def test_fixture_source_columns_and_lineage_unchanged(self) -> None:
        for bronze, flagged in (
            (self.bronze_customers, self.customers),
            (self.bronze_orders, self.orders),
            (self.bronze_products, self.products),
        ):
            cols = lineage_and_source_columns(bronze)
            self.assertIn(INGEST_ROW_ID_COLUMN, cols)
            self.assertEqual(bronze.select(*cols).exceptAll(flagged.select(*cols)).count(), 0)
            self.assertEqual(flagged.select(*cols).exceptAll(bronze.select(*cols)).count(), 0)
            self.assertEqual(
                flagged.select(INGEST_ROW_ID_COLUMN).distinct().count(),
                flagged.count(),
            )

    def test_fixture_completeness_and_uniqueness_counts(self) -> None:
        self.assertEqual(_code_count(self.customers, "completeness_failed_checks", EMAIL_CODE), 1)
        self.assertEqual(
            _code_count(self.orders, "completeness_failed_checks", ORDER_CUSTOMER_CODE),
            1,
        )
        self.assertEqual(
            _code_count(self.orders, "completeness_failed_checks", ORDER_PRODUCT_CODE),
            1,
        )
        self.assertEqual(self.customers.filter(~F.col("uniqueness_pass")).count(), 2)
        self.assertEqual(self.orders.filter(~F.col("uniqueness_pass")).count(), 2)
        self.assertEqual(self.products.filter(~F.col("completeness_pass")).count(), 0)
        self.assertEqual(self.products.filter(~F.col("uniqueness_pass")).count(), 0)

    def test_null_fk_is_completeness_not_orphan(self) -> None:
        null_customer = self.orders.filter(F.col("customer_id").isNull())
        self.assertEqual(null_customer.count(), 1)
        self.assertEqual(
            _code_count(null_customer, "completeness_failed_checks", ORDER_CUSTOMER_CODE),
            1,
        )
        self.assertTrue(null_customer.collect()[0]["uniqueness_pass"])
        self.assertNotIn("referential_integrity_pass", self.orders.columns)
        exploded = null_customer.select(
            F.explode(F.col("completeness_failed_checks")).alias("code")
        )
        self.assertEqual(exploded.filter(F.col("code").startswith("ri:")).count(), 0)
        self.assertEqual(exploded.filter(F.col("code").contains("orphan")).count(), 0)

    def test_orphan_fk_is_not_a_completeness_null(self) -> None:
        orphan = self.orders.filter(F.col("customer_id") == 90001)
        self.assertEqual(orphan.count(), 1)
        self.assertEqual(
            _code_count(orphan, "completeness_failed_checks", ORDER_CUSTOMER_CODE),
            0,
        )
        self.assertTrue(orphan.collect()[0]["completeness_pass"])
        self.assertNotIn("referential_integrity_pass", self.orders.columns)


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Silver Spark tests are BLOCKED in this environment.",
)
class TestSilverCommittedSource(SilverSparkTestCase):
    """Full committed CSVs via Bronze ingest. Local Spark, not Databricks."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.config = cls._fixture_config(DATA_DIR, schema="bronze_silver_full")
        ingest_all(cls.spark, cls.config)
        customers = cls.spark.table(cls.config.bronze_table("customers"))
        orders = cls.spark.table(cls.config.bronze_table("orders"))
        products = cls.spark.table(cls.config.bronze_table("products"))
        cls.bronze_customers = customers
        cls.bronze_orders = orders
        cls.bronze_products = products
        cls.customers = uniqueness.apply_uniqueness(
            completeness.apply_completeness(customers, "customers"),
            "customers",
        )
        cls.orders = uniqueness.apply_uniqueness(
            completeness.apply_completeness(orders, "orders"),
            "orders",
        )
        cls.products = uniqueness.apply_uniqueness(
            completeness.apply_completeness(products, "products"),
            "products",
        )
        cls.customer_completeness_metrics = completeness.completeness_metrics(
            cls.customers, "customers"
        )
        cls.order_completeness_metrics = completeness.completeness_metrics(
            cls.orders, "orders"
        )
        cls.customer_uniqueness_metrics = uniqueness.uniqueness_metrics(
            cls.customers, "customers"
        )
        cls.order_uniqueness_metrics = uniqueness.uniqueness_metrics(
            cls.orders, "orders"
        )

    def _metric(self, metrics, check_name: str):
        matches = [item for item in metrics if item.check_name == check_name]
        self.assertEqual(len(matches), 1, check_name)
        return matches[0]

    def test_physical_rows_remain(self) -> None:
        self.assertEqual(self.customers.count(), 10010)
        self.assertEqual(self.orders.count(), 100020)
        self.assertEqual(self.products.count(), 500)
        self.assertEqual(self.customers.count(), self.bronze_customers.count())
        self.assertEqual(self.orders.count(), self.bronze_orders.count())
        self.assertEqual(self.products.count(), self.bronze_products.count())

    def test_source_values_and_ingest_row_id_preserved(self) -> None:
        for bronze, flagged in (
            (self.bronze_customers, self.customers),
            (self.bronze_orders, self.orders),
            (self.bronze_products, self.products),
        ):
            cols = lineage_and_source_columns(bronze)
            self.assertEqual(bronze.select(*cols).exceptAll(flagged.select(*cols)).count(), 0)
            self.assertEqual(flagged.select(*cols).exceptAll(bronze.select(*cols)).count(), 0)
            self.assertEqual(
                flagged.select(INGEST_ROW_ID_COLUMN).distinct().count(),
                flagged.count(),
            )
            self.assertNotIn("quality_check_result", flagged.columns)

    def test_completeness_injected_null_counts(self) -> None:
        self.assertEqual(
            _code_count(self.customers, "completeness_failed_checks", EMAIL_CODE),
            50,
        )
        self.assertEqual(
            _code_count(self.orders, "completeness_failed_checks", ORDER_CUSTOMER_CODE),
            100,
        )
        self.assertEqual(
            _code_count(self.orders, "completeness_failed_checks", ORDER_PRODUCT_CODE),
            200,
        )
        email = self._metric(self.customer_completeness_metrics, EMAIL_CODE)
        self.assertEqual(email.total_evaluated, 10010)
        self.assertEqual(email.failed, 50)
        self.assertEqual(email.passed, 9960)
        order_customer = self._metric(self.order_completeness_metrics, ORDER_CUSTOMER_CODE)
        self.assertEqual(order_customer.total_evaluated, 100020)
        self.assertEqual(order_customer.failed, 100)
        order_product = self._metric(self.order_completeness_metrics, ORDER_PRODUCT_CODE)
        self.assertEqual(order_product.failed, 200)
        order_module = self._metric(self.order_completeness_metrics, MODULE_COMPLETENESS)
        self.assertEqual(order_module.failed, 300)

    def test_uniqueness_flags_all_participating_physical_rows(self) -> None:
        self.assertEqual(self.customers.filter(~F.col("uniqueness_pass")).count(), 20)
        self.assertEqual(self.orders.filter(~F.col("uniqueness_pass")).count(), 40)
        self.assertEqual(_code_count(self.customers, "uniqueness_failed_checks", CUSTOMER_UNIQ_CODE), 20)
        self.assertEqual(_code_count(self.orders, "uniqueness_failed_checks", ORDER_UNIQ_CODE), 40)
        self.assertEqual(uniqueness.uniqueness_duplicate_key_count(self.customers, "customers"), 10)
        self.assertEqual(uniqueness.uniqueness_duplicate_key_count(self.orders, "orders"), 20)
        self.assertEqual(self.products.filter(~F.col("uniqueness_pass")).count(), 0)
        customer_u = self._metric(self.customer_uniqueness_metrics, MODULE_UNIQUENESS)
        self.assertEqual(customer_u.total_evaluated, 10010)
        self.assertEqual(customer_u.failed, 20)
        self.assertEqual(customer_u.passed, 9990)
        order_u = self._metric(self.order_uniqueness_metrics, MODULE_UNIQUENESS)
        self.assertEqual(order_u.total_evaluated, 100020)
        self.assertEqual(order_u.failed, 40)
        self.assertEqual(order_u.passed, 99980)

    def test_null_fk_rows_are_not_classified_as_orphans(self) -> None:
        null_fk = self.orders.filter(F.col("customer_id").isNull())
        self.assertEqual(null_fk.count(), 100)
        self.assertEqual(
            _code_count(null_fk, "completeness_failed_checks", ORDER_CUSTOMER_CODE),
            100,
        )
        self.assertEqual(null_fk.filter(~F.col("uniqueness_pass")).count(), 0)
        self.assertNotIn("referential_integrity_pass", self.orders.columns)
        completeness_codes = null_fk.select(
            F.explode(F.col("completeness_failed_checks")).alias("code")
        )
        uniqueness_codes = null_fk.select(
            F.explode(F.col("uniqueness_failed_checks")).alias("code")
        )
        self.assertEqual(completeness_codes.filter(F.col("code").startswith("ri:")).count(), 0)
        self.assertEqual(completeness_codes.filter(F.col("code").contains("orphan")).count(), 0)
        self.assertEqual(uniqueness_codes.count(), 0)

    def test_later_check_does_not_overwrite_completeness(self) -> None:
        self.assertIn("completeness_pass", self.customers.columns)
        self.assertIn("uniqueness_pass", self.customers.columns)
        self.assertEqual(
            _code_count(self.customers, "completeness_failed_checks", EMAIL_CODE),
            50,
        )
        email_fail_rows = self.customers.filter(
            F.array_contains(F.col("completeness_failed_checks"), F.lit(EMAIL_CODE))
        )
        self.assertEqual(email_fail_rows.filter(F.col("completeness_pass")).count(), 0)
        self.assertEqual(
            email_fail_rows.filter(
                F.array_contains(F.col("uniqueness_failed_checks"), F.lit(CUSTOMER_UNIQ_CODE))
            ).count(),
            0,
        )


if __name__ == "__main__":
    unittest.main()
