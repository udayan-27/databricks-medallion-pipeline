"""
Silver quality Spark tests (completeness, uniqueness, type, RI).

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
TYPE_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "silver" / "type_validation"
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
from contracts import ENTITY_CONTRACTS, INGEST_ROW_ID_COLUMN  # noqa: E402
from ingest_core import add_ingest_row_id, ingest_all, read_source_csv  # noqa: E402
from spark_local import apply_local_spark_config  # noqa: E402
from quality_common import (  # noqa: E402
    MODULE_COMPLETENESS,
    MODULE_RI,
    MODULE_TYPE,
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
type_validation = _load_silver("03_quality_type_validation.py", "quality_type_validation")
referential_integrity = _load_silver(
    "04_quality_referential_integrity.py", "quality_referential_integrity"
)

EMAIL_CODE = "completeness:customers.email"
ORDER_CUSTOMER_CODE = "completeness:orders.customer_id"
ORDER_PRODUCT_CODE = "completeness:orders.product_id"
CUSTOMER_UNIQ_CODE = "uniqueness:customers.customer_id"
ORDER_UNIQ_CODE = "uniqueness:orders.order_id"
TYPE_QUANTITY_CODE = "type:orders.quantity"
TYPE_ORDER_DATE_CODE = "type:orders.order_date"
TYPE_UNIT_PRICE_CODE = "type:orders.unit_price"
TYPE_ORDER_STATUS_CODE = "type:orders.order_status_domain"
TYPE_SEGMENT_CODE = "type:customers.customer_segment_domain"
TYPE_SIGNUP_DATE_CODE = "type:customers.signup_date"
TYPE_LIFETIME_VALUE_CODE = "type:customers.lifetime_value"
TYPE_PRICE_CODE = "type:products.price"
TYPE_STOCK_CODE = "type:products.stock_quantity"
CUSTOMER_ORPHAN_CODE = "ri:orders.customer_id_orphan"
PRODUCT_ORPHAN_CODE = "ri:orders.product_id_orphan"


def _code_count(dataframe, column: str, code: str) -> int:
    return dataframe.filter(F.array_contains(F.col(column), F.lit(code))).count()


def _apply_implemented_quality(
    dataframe,
    table_name: str,
    *,
    customers=None,
    products=None,
):
    after = completeness.apply_completeness(dataframe, table_name)
    after = uniqueness.apply_uniqueness(after, table_name)
    after = type_validation.apply_type_validation(after, table_name)
    return referential_integrity.apply_referential_integrity(
        after,
        table_name,
        customers=customers,
        products=products,
    )


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
        cls.customers = _apply_implemented_quality(
            customers, "customers", customers=customers, products=products
        )
        cls.orders = _apply_implemented_quality(
            orders, "orders", customers=customers, products=products
        )
        cls.products = _apply_implemented_quality(
            products, "products", customers=customers, products=products
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
        self.assertEqual(self.products.filter(~F.col("type_validation_pass")).count(), 0)
        self.assertEqual(
            self.products.filter(~F.col("referential_integrity_pass")).count(), 0
        )

    def test_null_fk_is_completeness_not_orphan(self) -> None:
        null_customer = self.orders.filter(F.col("customer_id").isNull())
        self.assertEqual(null_customer.count(), 1)
        self.assertEqual(
            _code_count(null_customer, "completeness_failed_checks", ORDER_CUSTOMER_CODE),
            1,
        )
        self.assertTrue(null_customer.collect()[0]["uniqueness_pass"])
        self.assertTrue(null_customer.collect()[0]["type_validation_pass"])
        self.assertTrue(null_customer.collect()[0]["referential_integrity_pass"])
        self.assertEqual(
            _code_count(
                null_customer, "referential_integrity_failed_checks", CUSTOMER_ORPHAN_CODE
            ),
            0,
        )
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
        self.assertFalse(orphan.collect()[0]["referential_integrity_pass"])
        self.assertEqual(
            _code_count(orphan, "referential_integrity_failed_checks", CUSTOMER_ORPHAN_CODE),
            1,
        )

    def test_fixture_ri_counts_and_no_fan_out(self) -> None:
        self.assertEqual(
            _code_count(self.orders, "referential_integrity_failed_checks", CUSTOMER_ORPHAN_CODE),
            1,
        )
        self.assertEqual(
            _code_count(self.orders, "referential_integrity_failed_checks", PRODUCT_ORPHAN_CODE),
            1,
        )
        self.assertEqual(self.orders.count(), 6)
        self.assertEqual(
            self.orders.select(INGEST_ROW_ID_COLUMN).distinct().count(),
            6,
        )
        # Duplicate parent customer_id=1 must not multiply child rows.
        child_of_dup = self.orders.filter(F.col("customer_id") == 1)
        self.assertEqual(child_of_dup.count(), 4)


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
        cls.customers = _apply_implemented_quality(
            customers, "customers", customers=customers, products=products
        )
        cls.orders = _apply_implemented_quality(
            orders, "orders", customers=customers, products=products
        )
        cls.products = _apply_implemented_quality(
            products, "products", customers=customers, products=products
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
        cls.order_type_metrics = type_validation.type_validation_metrics(
            cls.orders, "orders"
        )
        cls.customer_type_metrics = type_validation.type_validation_metrics(
            cls.customers, "customers"
        )
        cls.product_type_metrics = type_validation.type_validation_metrics(
            cls.products, "products"
        )
        cls.order_ri_metrics = referential_integrity.referential_integrity_metrics(
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
        self.assertEqual(null_fk.filter(~F.col("referential_integrity_pass")).count(), 0)
        self.assertEqual(
            _code_count(null_fk, "referential_integrity_failed_checks", CUSTOMER_ORPHAN_CODE),
            0,
        )
        completeness_codes = null_fk.select(
            F.explode(F.col("completeness_failed_checks")).alias("code")
        )
        uniqueness_codes = null_fk.select(
            F.explode(F.col("uniqueness_failed_checks")).alias("code")
        )
        self.assertEqual(completeness_codes.filter(F.col("code").startswith("ri:")).count(), 0)
        self.assertEqual(completeness_codes.filter(F.col("code").contains("orphan")).count(), 0)
        self.assertEqual(uniqueness_codes.count(), 0)

        null_product = self.orders.filter(F.col("product_id").isNull())
        self.assertEqual(null_product.count(), 200)
        self.assertEqual(
            _code_count(null_product, "completeness_failed_checks", ORDER_PRODUCT_CODE),
            200,
        )
        self.assertEqual(
            _code_count(
                null_product, "referential_integrity_failed_checks", PRODUCT_ORPHAN_CODE
            ),
            0,
        )

    def test_intentional_orphans_are_ri_failures_not_completeness(self) -> None:
        self.assertEqual(
            _code_count(self.orders, "referential_integrity_failed_checks", CUSTOMER_ORPHAN_CODE),
            50,
        )
        self.assertEqual(
            _code_count(self.orders, "referential_integrity_failed_checks", PRODUCT_ORPHAN_CODE),
            30,
        )
        customer_ri = self._metric(self.order_ri_metrics, CUSTOMER_ORPHAN_CODE)
        self.assertEqual(customer_ri.total_evaluated, 100020)
        self.assertEqual(customer_ri.failed, 50)
        self.assertNotEqual(customer_ri.failed, 150)
        product_ri = self._metric(self.order_ri_metrics, PRODUCT_ORPHAN_CODE)
        self.assertEqual(product_ri.failed, 30)
        self.assertNotEqual(product_ri.failed, 230)
        module_ri = self._metric(self.order_ri_metrics, MODULE_RI)
        self.assertEqual(module_ri.failed, 80)
        orphan_customers = self.orders.filter(
            F.array_contains(
                F.col("referential_integrity_failed_checks"), F.lit(CUSTOMER_ORPHAN_CODE)
            )
        )
        self.assertEqual(
            orphan_customers.filter(F.col("customer_id").isNull()).count(), 0
        )
        self.assertEqual(
            _code_count(orphan_customers, "completeness_failed_checks", ORDER_CUSTOMER_CODE),
            0,
        )
        orphan_products = self.orders.filter(
            F.array_contains(
                F.col("referential_integrity_failed_checks"), F.lit(PRODUCT_ORPHAN_CODE)
            )
        )
        self.assertEqual(orphan_products.filter(F.col("product_id").isNull()).count(), 0)
        valid_fk = self.orders.filter(
            F.col("customer_id").isNotNull()
            & F.col("product_id").isNotNull()
            & (F.col("customer_id") < 90000)
            & (F.col("product_id") < 9000)
        )
        self.assertGreater(valid_fk.count(), 0)
        self.assertEqual(valid_fk.filter(~F.col("referential_integrity_pass")).count(), 0)

    def test_committed_source_has_no_type_failures(self) -> None:
        self.assertEqual(self.customers.filter(~F.col("type_validation_pass")).count(), 0)
        self.assertEqual(self.orders.filter(~F.col("type_validation_pass")).count(), 0)
        self.assertEqual(self.products.filter(~F.col("type_validation_pass")).count(), 0)
        order_type = self._metric(self.order_type_metrics, MODULE_TYPE)
        self.assertEqual(order_type.total_evaluated, 100020)
        self.assertEqual(order_type.failed, 0)
        customer_type = self._metric(self.customer_type_metrics, MODULE_TYPE)
        self.assertEqual(customer_type.total_evaluated, 10010)
        self.assertEqual(customer_type.failed, 0)
        product_type = self._metric(self.product_type_metrics, MODULE_TYPE)
        self.assertEqual(product_type.failed, 0)

    def test_later_check_does_not_overwrite_completeness(self) -> None:
        self.assertIn("completeness_pass", self.customers.columns)
        self.assertIn("uniqueness_pass", self.customers.columns)
        self.assertIn("type_validation_pass", self.customers.columns)
        self.assertIn("referential_integrity_pass", self.customers.columns)
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
        self.assertEqual(email_fail_rows.filter(~F.col("type_validation_pass")).count(), 0)


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Silver Spark tests are BLOCKED in this environment.",
)
class TestSilverTypeValidationFixture(SilverSparkTestCase):
    """
    Focused malformed-type CSV fixture (not Stage 2 data).

    Scenarios:
    - orders 1: valid Completed row with payment_date
    - orders 2: quantity=xyz (malformed INT → PERMISSIVE NULL); NULL payment_date
    - orders 3: order_date=13/01/2024 (malformed DATE vs yyyy-MM-dd)
    - orders 4: unit_price=12.34.56 (malformed DECIMAL)
    - orders 5: order_status=Shipped (invalid domain)
    - orders 6: valid Pending with NULL payment_date
    - orders 7: NULL customer_id (completeness-owned; not a type failure)
    - customers 2: customer_segment=premium (wrong case)
    - customers 3: signup_date malformed
    - customers 4: lifetime_value malformed
    - products 101: price malformed
    - products 102: stock_quantity malformed
    """

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.customers = add_ingest_row_id(
            read_source_csv(
                cls.spark,
                str(TYPE_FIXTURE_DIR / "customers.csv"),
                ENTITY_CONTRACTS["customers"],
            )
        )
        cls.orders = add_ingest_row_id(
            read_source_csv(
                cls.spark,
                str(TYPE_FIXTURE_DIR / "orders.csv"),
                ENTITY_CONTRACTS["orders"],
            )
        )
        cls.products = add_ingest_row_id(
            read_source_csv(
                cls.spark,
                str(TYPE_FIXTURE_DIR / "products.csv"),
                ENTITY_CONTRACTS["products"],
            )
        )
        cls.typed_customers = type_validation.apply_type_validation(
            cls.customers, "customers"
        )
        cls.typed_orders = type_validation.apply_type_validation(cls.orders, "orders")
        cls.typed_products = type_validation.apply_type_validation(
            cls.products, "products"
        )
        cls.complete_orders = completeness.apply_completeness(cls.typed_orders, "orders")

    def test_fixture_rows_are_not_deleted(self) -> None:
        self.assertEqual(self.typed_customers.count(), 4)
        self.assertEqual(self.typed_orders.count(), 7)
        self.assertEqual(self.typed_products.count(), 3)
        self.assertEqual(self.typed_customers.count(), self.customers.count())
        self.assertEqual(self.typed_orders.count(), self.orders.count())
        self.assertEqual(self.typed_products.count(), self.products.count())

    def test_valid_rows_pass(self) -> None:
        valid_order = self.typed_orders.filter(F.col("order_id") == 1)
        self.assertEqual(valid_order.count(), 1)
        self.assertTrue(valid_order.collect()[0]["type_validation_pass"])
        valid_customer = self.typed_customers.filter(F.col("customer_id") == 1)
        self.assertTrue(valid_customer.collect()[0]["type_validation_pass"])
        valid_product = self.typed_products.filter(F.col("product_id") == 100)
        self.assertTrue(valid_product.collect()[0]["type_validation_pass"])

    def test_malformed_integer_quantity(self) -> None:
        row = self.typed_orders.filter(F.col("order_id") == 2)
        self.assertEqual(row.count(), 1)
        collected = row.collect()[0]
        self.assertIsNone(collected["quantity"])
        self.assertFalse(collected["type_validation_pass"])
        self.assertIn(TYPE_QUANTITY_CODE, list(collected["type_failed_checks"] or []))

    def test_malformed_date_order_date(self) -> None:
        row = self.typed_orders.filter(F.col("order_id") == 3)
        collected = row.collect()[0]
        self.assertIsNone(collected["order_date"])
        self.assertFalse(collected["type_validation_pass"])
        self.assertIn(TYPE_ORDER_DATE_CODE, list(collected["type_failed_checks"] or []))

    def test_malformed_decimal_unit_price(self) -> None:
        row = self.typed_orders.filter(F.col("order_id") == 4)
        collected = row.collect()[0]
        self.assertIsNone(collected["unit_price"])
        self.assertFalse(collected["type_validation_pass"])
        self.assertIn(TYPE_UNIT_PRICE_CODE, list(collected["type_failed_checks"] or []))

    def test_invalid_domain_order_status(self) -> None:
        row = self.typed_orders.filter(F.col("order_id") == 5)
        collected = row.collect()[0]
        self.assertEqual(collected["order_status"], "Shipped")
        self.assertFalse(collected["type_validation_pass"])
        self.assertIn(TYPE_ORDER_STATUS_CODE, list(collected["type_failed_checks"] or []))

    def test_nullable_payment_date_null_is_valid(self) -> None:
        pending_null_pay = self.typed_orders.filter(F.col("order_id") == 6)
        collected = pending_null_pay.collect()[0]
        self.assertIsNone(collected["payment_date"])
        self.assertTrue(collected["type_validation_pass"])
        malformed_qty_also_null_pay = self.typed_orders.filter(F.col("order_id") == 2)
        pay_codes = list(malformed_qty_also_null_pay.collect()[0]["type_failed_checks"] or [])
        self.assertNotIn("type:orders.payment_date", pay_codes)

    def test_completeness_owned_null_fk_is_not_a_type_failure(self) -> None:
        row = self.complete_orders.filter(F.col("order_id") == 7)
        collected = row.collect()[0]
        self.assertIsNone(collected["customer_id"])
        self.assertTrue(collected["type_validation_pass"])
        self.assertNotIn(
            "type:orders.customer_id", list(collected["type_failed_checks"] or [])
        )
        self.assertFalse(collected["completeness_pass"])
        self.assertIn(ORDER_CUSTOMER_CODE, list(collected["completeness_failed_checks"] or []))

    def test_wrong_case_segment_and_malformed_customer_fields(self) -> None:
        wrong_case = self.typed_customers.filter(F.col("customer_id") == 2).collect()[0]
        self.assertEqual(wrong_case["customer_segment"], "premium")
        self.assertFalse(wrong_case["type_validation_pass"])
        self.assertIn(TYPE_SEGMENT_CODE, list(wrong_case["type_failed_checks"] or []))
        bad_date = self.typed_customers.filter(F.col("customer_id") == 3).collect()[0]
        self.assertIsNone(bad_date["signup_date"])
        self.assertIn(TYPE_SIGNUP_DATE_CODE, list(bad_date["type_failed_checks"] or []))
        bad_dec = self.typed_customers.filter(F.col("customer_id") == 4).collect()[0]
        self.assertIsNone(bad_dec["lifetime_value"])
        self.assertIn(TYPE_LIFETIME_VALUE_CODE, list(bad_dec["type_failed_checks"] or []))

    def test_malformed_product_price_and_stock(self) -> None:
        bad_price = self.typed_products.filter(F.col("product_id") == 101).collect()[0]
        self.assertIsNone(bad_price["price"])
        self.assertIn(TYPE_PRICE_CODE, list(bad_price["type_failed_checks"] or []))
        bad_stock = self.typed_products.filter(F.col("product_id") == 102).collect()[0]
        self.assertIsNone(bad_stock["stock_quantity"])
        self.assertIn(TYPE_STOCK_CODE, list(bad_stock["type_failed_checks"] or []))

    def test_type_does_not_rewrite_business_keys_or_lineage(self) -> None:
        cols = lineage_and_source_columns(self.orders)
        self.assertEqual(
            self.orders.select(*cols).exceptAll(self.typed_orders.select(*cols)).count(),
            0,
        )
        ids = [row[INGEST_ROW_ID_COLUMN] for row in self.typed_orders.collect()]
        self.assertEqual(len(ids), len(set(ids)))


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Silver Spark tests are BLOCKED in this environment.",
)
class TestSilverTypeAndRISynthetic(SilverSparkTestCase):
    def test_trailing_space_status_fails_domain_and_is_not_trimmed(self) -> None:
        schema = T.StructType(
            [
                T.StructField("order_id", T.IntegerType(), True),
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("order_date", T.DateType(), True),
                T.StructField("product_id", T.IntegerType(), True),
                T.StructField("quantity", T.IntegerType(), True),
                T.StructField("unit_price", T.DecimalType(18, 2), True),
                T.StructField("total_amount", T.DecimalType(18, 2), True),
                T.StructField("order_status", T.StringType(), True),
                T.StructField("payment_date", T.DateType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        df = self.spark.createDataFrame(
            [(1, 1, None, 100, 1, None, None, "Completed ", None, 1)],
            schema=schema,
        )
        # order_date/unit_price/total_amount NULL are type failures; status space is domain.
        flagged = type_validation.apply_type_validation(df, "orders")
        row = flagged.collect()[0]
        self.assertEqual(row["order_status"], "Completed ")
        codes = list(row["type_failed_checks"] or [])
        self.assertIn(TYPE_ORDER_STATUS_CODE, codes)

    def test_duplicate_parent_ids_do_not_multiply_child_rows(self) -> None:
        customer_schema = T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        product_schema = T.StructType(
            [
                T.StructField("product_id", T.IntegerType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        order_schema = T.StructType(
            [
                T.StructField("order_id", T.IntegerType(), True),
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("product_id", T.IntegerType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        customers = self.spark.createDataFrame(
            [(1, 101), (1, 102), (1, 103), (2, 104)],
            schema=customer_schema,
        )
        products = self.spark.createDataFrame([(100, 201)], schema=product_schema)
        orders = self.spark.createDataFrame(
            [
                (10, 1, 100, 1),
                (11, 1, 100, 2),
                (12, None, 100, 3),
                (13, 999, 100, 4),
            ],
            schema=order_schema,
        )
        flagged = referential_integrity.apply_referential_integrity(
            orders, "orders", customers=customers, products=products
        )
        self.assertEqual(flagged.count(), 4)
        self.assertEqual(flagged.count(), orders.count())
        per_id = flagged.groupBy(INGEST_ROW_ID_COLUMN).count()
        self.assertEqual(per_id.filter(F.col("count") > 1).count(), 0)
        self.assertEqual(
            flagged.filter(F.col("order_id") == 10).count(),
            1,
        )
        self.assertEqual(
            _code_count(flagged, "referential_integrity_failed_checks", CUSTOMER_ORPHAN_CODE),
            1,
        )
        null_row = flagged.filter(F.col("order_id") == 12).collect()[0]
        self.assertTrue(null_row["referential_integrity_pass"])
        orphan = flagged.filter(F.col("order_id") == 13).collect()[0]
        self.assertEqual(orphan["customer_id"], 999)
        self.assertFalse(orphan["referential_integrity_pass"])
        keys = [row.customer_id for row in flagged.orderBy(INGEST_ROW_ID_COLUMN).collect()]
        self.assertEqual(keys, [1, 1, None, 999])

    def test_empty_orders_remain_empty(self) -> None:
        order_schema = T.StructType(
            [
                T.StructField("order_id", T.IntegerType(), True),
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("product_id", T.IntegerType(), True),
                T.StructField("order_date", T.DateType(), True),
                T.StructField("quantity", T.IntegerType(), True),
                T.StructField("unit_price", T.DecimalType(18, 2), True),
                T.StructField("total_amount", T.DecimalType(18, 2), True),
                T.StructField("order_status", T.StringType(), True),
                T.StructField("payment_date", T.DateType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        customer_schema = T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        product_schema = T.StructType(
            [
                T.StructField("product_id", T.IntegerType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        empty_orders = self.spark.createDataFrame([], schema=order_schema)
        customers = self.spark.createDataFrame([(1, 1)], schema=customer_schema)
        products = self.spark.createDataFrame([(100, 1)], schema=product_schema)
        typed = type_validation.apply_type_validation(empty_orders, "orders")
        ri = referential_integrity.apply_referential_integrity(
            typed, "orders", customers=customers, products=products
        )
        self.assertEqual(typed.count(), 0)
        self.assertEqual(ri.count(), 0)

    def test_all_invalid_orphans_and_repeated_ri_execution(self) -> None:
        customer_schema = T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        product_schema = T.StructType(
            [
                T.StructField("product_id", T.IntegerType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        order_schema = T.StructType(
            [
                T.StructField("order_id", T.IntegerType(), True),
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("product_id", T.IntegerType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        customers = self.spark.createDataFrame([(1, 1)], schema=customer_schema)
        products = self.spark.createDataFrame([(100, 1)], schema=product_schema)
        orders = self.spark.createDataFrame(
            [(10, 999, 888, 1), (11, 998, 887, 2)],
            schema=order_schema,
        )
        first = referential_integrity.apply_referential_integrity(
            orders, "orders", customers=customers, products=products
        )
        second = referential_integrity.apply_referential_integrity(
            first, "orders", customers=customers, products=products
        )
        self.assertEqual(first.count(), 2)
        self.assertEqual(second.count(), 2)
        self.assertEqual(first.filter(~F.col("referential_integrity_pass")).count(), 2)
        self.assertEqual(second.filter(~F.col("referential_integrity_pass")).count(), 2)
        for row in second.collect():
            codes = list(row["referential_integrity_failed_checks"] or [])
            self.assertEqual(codes.count(CUSTOMER_ORPHAN_CODE), 1)
            self.assertEqual(codes.count(PRODUCT_ORPHAN_CODE), 1)

    def test_multiple_quality_failures_can_coexist(self) -> None:
        customer_schema = T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("email", T.StringType(), True),
                T.StructField("signup_date", T.DateType(), True),
                T.StructField("customer_segment", T.StringType(), True),
                T.StructField("lifetime_value", T.DecimalType(18, 2), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        product_schema = T.StructType(
            [
                T.StructField("product_id", T.IntegerType(), True),
                T.StructField("price", T.DecimalType(18, 2), True),
                T.StructField("cost", T.DecimalType(18, 2), True),
                T.StructField("stock_quantity", T.IntegerType(), True),
                T.StructField("reorder_level", T.IntegerType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        order_schema = T.StructType(
            [
                T.StructField("order_id", T.IntegerType(), True),
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("order_date", T.DateType(), True),
                T.StructField("product_id", T.IntegerType(), True),
                T.StructField("quantity", T.IntegerType(), True),
                T.StructField("unit_price", T.DecimalType(18, 2), True),
                T.StructField("total_amount", T.DecimalType(18, 2), True),
                T.StructField("order_status", T.StringType(), True),
                T.StructField("payment_date", T.DateType(), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )
        customers = self.spark.createDataFrame(
            [(1, "a@example.com", None, "Premium", None, 1)],
            schema=customer_schema,
        )
        products = self.spark.createDataFrame(
            [(100, None, None, 1, 1, 1)],
            schema=product_schema,
        )
        orders = self.spark.createDataFrame(
            [
                (10, None, None, 999, None, None, None, "Shipped", None, 1),
                (10, None, None, 999, None, None, None, "Shipped", None, 2),
            ],
            schema=order_schema,
        )
        flagged = _apply_implemented_quality(
            orders, "orders", customers=customers, products=products
        )
        self.assertEqual(flagged.count(), 2)
        for row in flagged.collect():
            self.assertFalse(row["completeness_pass"])
            self.assertFalse(row["uniqueness_pass"])
            self.assertFalse(row["type_validation_pass"])
            self.assertFalse(row["referential_integrity_pass"])
            completeness_codes = list(row["completeness_failed_checks"] or [])
            uniqueness_codes = list(row["uniqueness_failed_checks"] or [])
            type_codes = list(row["type_failed_checks"] or [])
            ri_codes = list(row["referential_integrity_failed_checks"] or [])
            self.assertIn(ORDER_CUSTOMER_CODE, completeness_codes)
            self.assertIn(ORDER_UNIQ_CODE, uniqueness_codes)
            self.assertIn(TYPE_ORDER_STATUS_CODE, type_codes)
            self.assertIn(TYPE_QUANTITY_CODE, type_codes)
            self.assertIn(PRODUCT_ORPHAN_CODE, ri_codes)
            self.assertNotIn(CUSTOMER_ORPHAN_CODE, ri_codes)
            self.assertNotIn(ORDER_CUSTOMER_CODE, type_codes)
            self.assertEqual(row["order_id"], 10)
            self.assertIn(row[INGEST_ROW_ID_COLUMN], (1, 2))


if __name__ == "__main__":
    unittest.main()
