"""
Silver quality Spark tests (completeness, uniqueness, type, RI, business
logic, and Silver table orchestration).

Requires a local PySpark runtime. If PySpark is not installed, the entire
module is skipped — that is BLOCKED runtime evidence, not a pass.

Uses Bronze ingest (fixtures and committed CSVs) so NULL semantics match the
PERMISSIVE empty-field contract. Combined Silver tables are written only in
orchestration tests (parquet, local warehouse).
"""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
BRONZE_DIR = SRC_DIR / "bronze"
SILVER_DIR = SRC_DIR / "silver"
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "bronze"
TYPE_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "silver" / "type_validation"
BL_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "silver" / "business_logic"
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
    COMBINED_FAILED_CHECKS_COLUMN,
    MODULE_BUSINESS,
    MODULE_COMPLETENESS,
    MODULE_RI,
    MODULE_TYPE,
    MODULE_UNIQUENESS,
    POPULATION_DISTINCT_KEY,
    POPULATION_TABLE_OUTCOME,
    QUALITY_CHECK_RESULT_COLUMN,
    QualityError,
    combine_quality_status,
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
business_logic = _load_silver("05_quality_business_logic.py", "quality_business_logic")
create_silver = _load_silver("create_silver_tables.py", "create_silver_tables")

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
QTY_POSITIVE_CODE = "business:orders.quantity_positive"
UNIT_PRICE_NN_CODE = "business:orders.unit_price_non_negative"
TOTAL_AMOUNT_NN_CODE = "business:orders.total_amount_non_negative"
AMOUNT_EQ_CODE = "business:orders.amount_equals_qty_price"
COMPLETED_PAY_CODE = "business:orders.completed_has_payment"
CANCELLED_PAY_CODE = "business:orders.cancelled_without_payment"
PAYMENT_AFTER_CODE = "business:orders.payment_on_or_after_order"
ORDER_SIGNUP_CODE = "business:orders.order_not_before_signup"
SIGNUP_FUTURE_CODE = "business:customers.signup_not_future"
LTV_NN_CODE = "business:customers.lifetime_value_non_negative"
PRICE_NN_CODE = "business:products.price_non_negative"
COST_NN_CODE = "business:products.cost_non_negative"
STOCK_NN_CODE = "business:products.stock_non_negative"
REORDER_NN_CODE = "business:products.reorder_non_negative"


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
    after = referential_integrity.apply_referential_integrity(
        after,
        table_name,
        customers=customers,
        products=products,
    )
    return business_logic.apply_business_logic(
        after,
        table_name,
        customers=customers,
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
        cls.customer_bl_metrics = business_logic.business_logic_metrics(
            cls.customers, "customers"
        )
        cls.order_bl_metrics = business_logic.business_logic_metrics(
            cls.orders, "orders"
        )
        cls.product_bl_metrics = business_logic.business_logic_metrics(
            cls.products, "products"
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
        self.assertIn("business_logic_pass", self.customers.columns)
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


    def test_optional_future_signup_is_separate_from_mandatory_460(self) -> None:
        self.assertEqual(
            _code_count(self.customers, "business_logic_failed_checks", SIGNUP_FUTURE_CODE),
            30,
        )
        signup = self._metric(self.customer_bl_metrics, SIGNUP_FUTURE_CODE)
        self.assertEqual(signup.total_evaluated, 10010)
        self.assertEqual(signup.failed, 30)
        self.assertEqual(signup.passed, 9980)
        ltv = self._metric(self.customer_bl_metrics, LTV_NN_CODE)
        self.assertEqual(ltv.failed, 0)
        customer_bl = self._metric(self.customer_bl_metrics, MODULE_BUSINESS)
        self.assertEqual(customer_bl.failed, 30)
        # Future-signup customers were excluded from orders in Stage 2.
        self.assertEqual(
            _code_count(self.orders, "business_logic_failed_checks", ORDER_SIGNUP_CODE),
            0,
        )
        for code in (
            QTY_POSITIVE_CODE,
            UNIT_PRICE_NN_CODE,
            TOTAL_AMOUNT_NN_CODE,
            AMOUNT_EQ_CODE,
            COMPLETED_PAY_CODE,
            CANCELLED_PAY_CODE,
            PAYMENT_AFTER_CODE,
        ):
            self.assertEqual(
                _code_count(self.orders, "business_logic_failed_checks", code),
                0,
                code,
            )
        order_bl = self._metric(self.order_bl_metrics, MODULE_BUSINESS)
        self.assertEqual(order_bl.failed, 0)
        self.assertEqual(order_bl.total_evaluated, 100020)
        product_bl = self._metric(self.product_bl_metrics, MODULE_BUSINESS)
        self.assertEqual(product_bl.failed, 0)
        self.assertEqual(self.products.filter(~F.col("business_logic_pass")).count(), 0)


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
            self.assertTrue(row["business_logic_pass"])
            self.assertNotIn(ORDER_SIGNUP_CODE, list(row["business_logic_failed_checks"] or []))
            self.assertEqual(row["order_id"], 10)
            self.assertIn(row[INGEST_ROW_ID_COLUMN], (1, 2))


def _bl_codes(row) -> list[str]:
    return list(row["business_logic_failed_checks"] or [])


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Silver Spark tests are BLOCKED in this environment.",
)
class TestSilverBusinessLogicFixture(SilverSparkTestCase):
    """
    Focused business-logic CSV fixture (not Stage 2 data).

    Order ids:
    1 valid completed; 2 qty=0; 3 qty=1; 4 unit_price=0; 5 negative price/amount;
    6 amount +0.01 (pass); 7 amount +0.02 (fail); 8 completed no payment;
    9 cancelled with payment; 10 cancelled no payment; 11 pending null pay;
    12 pending with payment; 13 payment before order; 14 payment = order date;
    15 order before signup; 16 order = signup; 17 null quantity; 18 null customer_id;
    19 orphan customer; 20 multiple BL failures; 21 negative amount mismatch;
    22 as-of signup/order same day.
    Customers: 2 as-of boundary; 3 future no order; 4 no order valid; 5 negative LTV;
    6 null signup.
    """

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.customers = add_ingest_row_id(
            read_source_csv(
                cls.spark,
                str(BL_FIXTURE_DIR / "customers.csv"),
                ENTITY_CONTRACTS["customers"],
            )
        )
        cls.orders = add_ingest_row_id(
            read_source_csv(
                cls.spark,
                str(BL_FIXTURE_DIR / "orders.csv"),
                ENTITY_CONTRACTS["orders"],
            )
        )
        cls.products = add_ingest_row_id(
            read_source_csv(
                cls.spark,
                str(BL_FIXTURE_DIR / "products.csv"),
                ENTITY_CONTRACTS["products"],
            )
        )
        cls.bl_customers = business_logic.apply_business_logic(cls.customers, "customers")
        cls.bl_orders = business_logic.apply_business_logic(
            cls.orders, "orders", customers=cls.customers
        )
        cls.bl_products = business_logic.apply_business_logic(cls.products, "products")

    def _order(self, order_id: int):
        rows = self.bl_orders.filter(F.col("order_id") == order_id).collect()
        self.assertEqual(len(rows), 1, order_id)
        return rows[0]

    def _customer(self, customer_id: int):
        rows = self.bl_customers.filter(F.col("customer_id") == customer_id).collect()
        self.assertEqual(len(rows), 1, customer_id)
        return rows[0]

    def test_fixture_rows_and_lineage_preserved(self) -> None:
        self.assertEqual(self.bl_customers.count(), 7)
        self.assertEqual(self.bl_orders.count(), 22)
        self.assertEqual(self.bl_products.count(), 7)
        self.assertEqual(self.bl_customers.count(), self.customers.count())
        self.assertEqual(self.bl_orders.count(), self.orders.count())
        self.assertEqual(self.bl_products.count(), self.products.count())
        cols = lineage_and_source_columns(self.orders)
        self.assertEqual(
            self.orders.select(*cols).exceptAll(self.bl_orders.select(*cols)).count(),
            0,
        )
        ids = [row[INGEST_ROW_ID_COLUMN] for row in self.bl_orders.collect()]
        self.assertEqual(len(ids), len(set(ids)))

    def test_valid_and_boundary_examples(self) -> None:
        self.assertTrue(self._order(1)["business_logic_pass"])
        self.assertEqual(_bl_codes(self._order(1)), [])
        self.assertTrue(self._order(3)["business_logic_pass"])
        self.assertTrue(self._order(4)["business_logic_pass"])
        self.assertTrue(self._order(6)["business_logic_pass"])
        self.assertTrue(self._order(10)["business_logic_pass"])
        self.assertTrue(self._order(11)["business_logic_pass"])
        self.assertTrue(self._order(12)["business_logic_pass"])
        self.assertTrue(self._order(14)["business_logic_pass"])
        self.assertTrue(self._order(16)["business_logic_pass"])
        self.assertTrue(self._order(22)["business_logic_pass"])
        self.assertTrue(self._customer(1)["business_logic_pass"])
        self.assertTrue(self._customer(2)["business_logic_pass"])
        self.assertTrue(self._customer(4)["business_logic_pass"])

    def test_quantity_and_money_rules(self) -> None:
        self.assertIn(QTY_POSITIVE_CODE, _bl_codes(self._order(2)))
        self.assertNotIn(AMOUNT_EQ_CODE, _bl_codes(self._order(2)))
        self.assertIn(UNIT_PRICE_NN_CODE, _bl_codes(self._order(5)))
        self.assertIn(TOTAL_AMOUNT_NN_CODE, _bl_codes(self._order(5)))
        self.assertNotIn(AMOUNT_EQ_CODE, _bl_codes(self._order(6)))
        self.assertIn(AMOUNT_EQ_CODE, _bl_codes(self._order(7)))
        self.assertIn(TOTAL_AMOUNT_NN_CODE, _bl_codes(self._order(21)))
        self.assertIn(AMOUNT_EQ_CODE, _bl_codes(self._order(21)))

    def test_status_and_payment_rules(self) -> None:
        self.assertIn(COMPLETED_PAY_CODE, _bl_codes(self._order(8)))
        self.assertIn(CANCELLED_PAY_CODE, _bl_codes(self._order(9)))
        self.assertNotIn(COMPLETED_PAY_CODE, _bl_codes(self._order(11)))
        self.assertNotIn(CANCELLED_PAY_CODE, _bl_codes(self._order(12)))
        self.assertIn(PAYMENT_AFTER_CODE, _bl_codes(self._order(13)))
        self.assertNotIn(PAYMENT_AFTER_CODE, _bl_codes(self._order(14)))

    def test_signup_timing_and_null_deferral(self) -> None:
        self.assertIn(SIGNUP_FUTURE_CODE, _bl_codes(self._customer(3)))
        self.assertEqual(
            self.bl_orders.filter(F.col("customer_id") == 3).count(),
            0,
        )
        self.assertTrue(self._customer(4)["business_logic_pass"])
        self.assertIn(LTV_NN_CODE, _bl_codes(self._customer(5)))
        self.assertNotIn(SIGNUP_FUTURE_CODE, _bl_codes(self._customer(6)))
        self.assertTrue(self._customer(6)["business_logic_pass"])
        self.assertIn(ORDER_SIGNUP_CODE, _bl_codes(self._order(15)))
        null_qty = self._order(17)
        self.assertIsNone(null_qty["quantity"])
        self.assertNotIn(QTY_POSITIVE_CODE, _bl_codes(null_qty))
        null_fk = self._order(18)
        self.assertIsNone(null_fk["customer_id"])
        self.assertNotIn(ORDER_SIGNUP_CODE, _bl_codes(null_fk))
        orphan = self._order(19)
        self.assertEqual(orphan["customer_id"], 90001)
        self.assertNotIn(ORDER_SIGNUP_CODE, _bl_codes(orphan))

    def test_multiple_simultaneous_business_failures(self) -> None:
        codes = _bl_codes(self._order(20))
        self.assertIn(QTY_POSITIVE_CODE, codes)
        self.assertIn(AMOUNT_EQ_CODE, codes)
        self.assertIn(COMPLETED_PAY_CODE, codes)
        self.assertGreaterEqual(len(codes), 3)

    def test_product_non_negative_rules(self) -> None:
        by_id = {
            row.product_id: row
            for row in self.bl_products.collect()
        }
        self.assertTrue(by_id[100]["business_logic_pass"])
        self.assertIn(PRICE_NN_CODE, _bl_codes(by_id[101]))
        self.assertIn(COST_NN_CODE, _bl_codes(by_id[102]))
        self.assertTrue(by_id[103]["business_logic_pass"])
        self.assertIn(STOCK_NN_CODE, _bl_codes(by_id[104]))
        self.assertIn(REORDER_NN_CODE, _bl_codes(by_id[105]))
        self.assertTrue(by_id[106]["business_logic_pass"])


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Silver Spark tests are BLOCKED in this environment.",
)
class TestSilverBusinessLogicSynthetic(SilverSparkTestCase):
    def _orders_schema(self):
        return T.StructType(
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

    def _customers_schema(self):
        return T.StructType(
            [
                T.StructField("customer_id", T.IntegerType(), True),
                T.StructField("signup_date", T.DateType(), True),
                T.StructField("lifetime_value", T.DecimalType(18, 2), True),
                T.StructField(INGEST_ROW_ID_COLUMN, T.LongType(), False),
            ]
        )

    def test_decimal_rounding_stays_decimal(self) -> None:
        customers = self.spark.createDataFrame(
            [(1, date(2024, 1, 1), Decimal("1.00"), 1)],
            schema=self._customers_schema(),
        )
        orders = self.spark.createDataFrame(
            [
                (
                    1,
                    1,
                    date(2024, 6, 1),
                    100,
                    3,
                    Decimal("6.67"),
                    Decimal("20.01"),
                    "Pending",
                    None,
                    1,
                ),
                (
                    2,
                    1,
                    date(2024, 6, 1),
                    100,
                    3,
                    Decimal("6.67"),
                    Decimal("20.03"),
                    "Pending",
                    None,
                    2,
                ),
            ],
            schema=self._orders_schema(),
        )
        flagged = business_logic.apply_business_logic(
            orders, "orders", customers=customers
        )
        exact = flagged.filter(F.col("order_id") == 1).collect()[0]
        off = flagged.filter(F.col("order_id") == 2).collect()[0]
        # 3 * 6.67 = 20.01 (DECIMAL). 20.03 is 0.02 outside the 0.01 tolerance.
        self.assertNotIn(AMOUNT_EQ_CODE, _bl_codes(exact))
        self.assertIn(AMOUNT_EQ_CODE, _bl_codes(off))
        self.assertEqual(exact["total_amount"], Decimal("20.01"))

    def test_duplicate_parent_signup_uses_min_ingest_row(self) -> None:
        customers = self.spark.createDataFrame(
            [
                (1, date(2024, 3, 1), Decimal("1.00"), 10),
                (1, date(2025, 12, 1), Decimal("1.00"), 20),
            ],
            schema=self._customers_schema(),
        )
        orders = self.spark.createDataFrame(
            [
                (1, 1, date(2024, 6, 1), 100, 1, Decimal("10.00"), Decimal("10.00"), "Pending", None, 1),
                (2, 1, date(2024, 2, 1), 100, 1, Decimal("10.00"), Decimal("10.00"), "Pending", None, 2),
            ],
            schema=self._orders_schema(),
        )
        flagged = business_logic.apply_business_logic(
            orders, "orders", customers=customers
        )
        self.assertEqual(flagged.count(), 2)
        later_ok = flagged.filter(F.col("order_id") == 1).collect()[0]
        earlier_fail = flagged.filter(F.col("order_id") == 2).collect()[0]
        self.assertNotIn(ORDER_SIGNUP_CODE, _bl_codes(later_ok))
        self.assertIn(ORDER_SIGNUP_CODE, _bl_codes(earlier_fail))

    def test_empty_and_repeated_execution(self) -> None:
        customers = self.spark.createDataFrame(
            [(1, date(2024, 1, 1), Decimal("1.00"), 1)],
            schema=self._customers_schema(),
        )
        empty = self.spark.createDataFrame([], schema=self._orders_schema())
        first = business_logic.apply_business_logic(
            empty, "orders", customers=customers
        )
        second = business_logic.apply_business_logic(
            first, "orders", customers=customers
        )
        self.assertEqual(first.count(), 0)
        self.assertEqual(second.count(), 0)
        invalid = self.spark.createDataFrame(
            [
                (1, 1, date(2024, 6, 1), 100, 0, Decimal("-1.00"), Decimal("-1.00"), "Completed", None, 1),
            ],
            schema=self._orders_schema(),
        )
        once = business_logic.apply_business_logic(
            invalid, "orders", customers=customers
        )
        twice = business_logic.apply_business_logic(
            once, "orders", customers=customers
        )
        self.assertEqual(once.count(), 1)
        self.assertEqual(twice.count(), 1)
        codes = _bl_codes(twice.collect()[0])
        self.assertEqual(codes.count(QTY_POSITIVE_CODE), 1)
        self.assertEqual(codes.count(UNIT_PRICE_NN_CODE), 1)
        self.assertEqual(codes.count(COMPLETED_PAY_CODE), 1)

    def test_all_valid_and_all_invalid_customers(self) -> None:
        valid = self.spark.createDataFrame(
            [(1, date(2026, 8, 31), Decimal("0.00"), 1)],
            schema=self._customers_schema(),
        )
        invalid = self.spark.createDataFrame(
            [(2, date(2026, 9, 1), Decimal("-0.01"), 2)],
            schema=self._customers_schema(),
        )
        valid_f = business_logic.apply_business_logic(valid, "customers")
        invalid_f = business_logic.apply_business_logic(invalid, "customers")
        self.assertTrue(valid_f.collect()[0]["business_logic_pass"])
        bad = invalid_f.collect()[0]
        self.assertIn(SIGNUP_FUTURE_CODE, _bl_codes(bad))
        self.assertIn(LTV_NN_CODE, _bl_codes(bad))


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Silver Spark tests are BLOCKED in this environment.",
)
class TestSilverOrchestrationAndReconciliation(SilverSparkTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.config = cls._fixture_config(BL_FIXTURE_DIR, schema="silver_bl_orch")
        ingest_all(cls.spark, cls.config)
        cls.result = create_silver.create_silver_tables(
            cls.spark, cls.config, write=True
        )

    def _metric(self, check_name: str, table_name: str):
        matches = [
            item
            for item in self.result.metrics
            if item.table_name == table_name and item.check_name == check_name
        ]
        self.assertEqual(len(matches), 1, check_name)
        return matches[0]

    def test_written_silver_row_counts_match_bronze(self) -> None:
        for table_name in ("customers", "orders", "products"):
            bronze = self.spark.table(self.config.bronze_table(table_name))
            silver = self.spark.table(self.config.silver_table(table_name))
            self.assertEqual(silver.count(), bronze.count())
            self.assertEqual(
                self.result.table_results[table_name].bronze_rows,
                self.result.table_results[table_name].silver_rows,
            )
            cols = lineage_and_source_columns(bronze)
            self.assertEqual(bronze.select(*cols).exceptAll(silver.select(*cols)).count(), 0)
            self.assertEqual(silver.select(*cols).exceptAll(bronze.select(*cols)).count(), 0)
            self.assertEqual(
                silver.select(INGEST_ROW_ID_COLUMN).distinct().count(),
                silver.count(),
            )
            self.assertIn(QUALITY_CHECK_RESULT_COLUMN, silver.columns)
            self.assertIn(COMBINED_FAILED_CHECKS_COLUMN, silver.columns)
            self.assertIn("completeness_pass", silver.columns)
            self.assertIn("business_logic_pass", silver.columns)

    def test_combined_status_accumulates_without_overwrite(self) -> None:
        silver_orders = self.spark.table(self.config.silver_table("orders"))
        multi = silver_orders.filter(F.col("order_id") == 20).collect()[0]
        combined = list(multi[COMBINED_FAILED_CHECKS_COLUMN] or [])
        self.assertIn(QTY_POSITIVE_CODE, combined)
        self.assertIn(AMOUNT_EQ_CODE, combined)
        self.assertIn(COMPLETED_PAY_CODE, combined)
        self.assertFalse(multi["business_logic_pass"])
        self.assertEqual(multi[QUALITY_CHECK_RESULT_COLUMN], "FAIL")
        self.assertTrue(multi["completeness_pass"])
        null_fk = silver_orders.filter(F.col("order_id") == 18).collect()[0]
        combined_null = list(null_fk[COMBINED_FAILED_CHECKS_COLUMN] or [])
        self.assertIn(ORDER_CUSTOMER_CODE, combined_null)
        self.assertNotIn(ORDER_SIGNUP_CODE, combined_null)
        self.assertNotIn(CUSTOMER_ORPHAN_CODE, combined_null)
        self.assertEqual(null_fk[QUALITY_CHECK_RESULT_COLUMN], "FAIL")
        self.assertFalse(null_fk["completeness_pass"])
        self.assertTrue(null_fk["referential_integrity_pass"])
        self.assertTrue(null_fk["business_logic_pass"])

    def test_ri_does_not_fan_out_and_metrics_reconcile(self) -> None:
        silver_orders = self.result.tables["orders"]
        self.assertEqual(silver_orders.count(), 22)
        per_id = silver_orders.groupBy(INGEST_ROW_ID_COLUMN).count()
        self.assertEqual(per_id.filter(F.col("count") > 1).count(), 0)
        outcome = self._metric(QUALITY_CHECK_RESULT_COLUMN, "orders")
        self.assertEqual(outcome.population_kind, POPULATION_TABLE_OUTCOME)
        self.assertEqual(
            outcome.failed + outcome.passed,
            outcome.total_evaluated,
        )
        self.assertEqual(outcome.failed, self.result.table_results["orders"].fail_rows)
        qty = self._metric(QTY_POSITIVE_CODE, "orders")
        amount = self._metric(AMOUNT_EQ_CODE, "orders")
        completed = self._metric(COMPLETED_PAY_CODE, "orders")
        # Order 20 fails all three of these rules. Rule-level sums are issue
        # instances, not distinct FAIL rows (outcome also includes completeness/RI).
        self.assertNotEqual(
            qty.failed + amount.failed + completed.failed,
            outcome.failed,
        )
        self.assertEqual(outcome.failed + outcome.passed, 22)
        metrics_table = self.spark.table(self.config.silver_table("quality_metrics"))
        self.assertEqual(metrics_table.count(), len(self.result.metrics))
        self.assertIn("total_evaluated", metrics_table.columns)
        self.assertIn("expected_fail_count", metrics_table.columns)
        self.assertIn("population_kind", metrics_table.columns)

    def test_repeated_orchestration_does_not_change_counts(self) -> None:
        again = create_silver.create_silver_tables(self.spark, self.config, write=True)
        for table_name in ("customers", "orders", "products"):
            self.assertEqual(
                again.table_results[table_name].silver_rows,
                self.result.table_results[table_name].silver_rows,
            )
            self.assertEqual(
                again.table_results[table_name].fail_rows,
                self.result.table_results[table_name].fail_rows,
            )


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Silver Spark tests are BLOCKED in this environment.",
)
class TestSilverCommittedOrchestration(SilverSparkTestCase):
    """Full seed-42 Bronze → combined Silver in memory (local parquet warehouse)."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.config = cls._fixture_config(DATA_DIR, schema="silver_full_orch")
        ingest_all(cls.spark, cls.config)
        cls.result = create_silver.build_silver_dataframes(cls.spark, cls.config)

    def _metric(self, check_name: str, table_name: str):
        matches = [
            item
            for item in self.result.metrics
            if item.table_name == table_name and item.check_name == check_name
        ]
        self.assertEqual(len(matches), 1, check_name)
        return matches[0]

    def test_physical_reconciliation_and_mandatory_counts(self) -> None:
        self.assertEqual(self.result.table_results["customers"].bronze_rows, 10010)
        self.assertEqual(self.result.table_results["customers"].silver_rows, 10010)
        self.assertEqual(self.result.table_results["orders"].bronze_rows, 100020)
        self.assertEqual(self.result.table_results["orders"].silver_rows, 100020)
        self.assertEqual(self.result.table_results["products"].bronze_rows, 500)
        self.assertEqual(self.result.table_results["products"].silver_rows, 500)
        self.assertEqual(self._metric(EMAIL_CODE, "customers").failed, 50)
        self.assertEqual(self._metric(ORDER_CUSTOMER_CODE, "orders").failed, 100)
        self.assertEqual(self._metric(ORDER_PRODUCT_CODE, "orders").failed, 200)
        self.assertEqual(self._metric(CUSTOMER_UNIQ_CODE, "customers").failed, 20)
        self.assertEqual(self._metric(ORDER_UNIQ_CODE, "orders").failed, 40)
        self.assertEqual(self._metric(CUSTOMER_ORPHAN_CODE, "orders").failed, 50)
        self.assertEqual(self._metric(PRODUCT_ORPHAN_CODE, "orders").failed, 30)
        self.assertEqual(self._metric(MODULE_TYPE, "customers").failed, 0)
        self.assertEqual(self._metric(MODULE_TYPE, "orders").failed, 0)
        self.assertEqual(self._metric(MODULE_TYPE, "products").failed, 0)
        self.assertEqual(self._metric(SIGNUP_FUTURE_CODE, "customers").failed, 30)
        self.assertEqual(self._metric(ORDER_SIGNUP_CODE, "orders").failed, 0)
        self.assertEqual(self._metric(MODULE_BUSINESS, "orders").failed, 0)
        dup_keys = self._metric(
            "uniqueness:customers.customer_id.duplicate_keys", "customers"
        )
        self.assertEqual(dup_keys.failed, 10)
        self.assertEqual(dup_keys.total_evaluated, 10000)
        self.assertEqual(dup_keys.population_kind, POPULATION_DISTINCT_KEY)
        order_keys = self._metric(
            "uniqueness:orders.order_id.duplicate_keys", "orders"
        )
        self.assertEqual(order_keys.failed, 20)
        self.assertEqual(order_keys.population_kind, POPULATION_DISTINCT_KEY)

    def test_table_outcome_is_not_the_sum_of_rule_failures(self) -> None:
        customer_fail = self.result.table_results["customers"].fail_rows
        email = self._metric(EMAIL_CODE, "customers").failed
        uniq = self._metric(CUSTOMER_UNIQ_CODE, "customers").failed
        future = self._metric(SIGNUP_FUTURE_CODE, "customers").failed
        rule_sum = email + uniq + future
        self.assertEqual(email, 50)
        self.assertEqual(uniq, 20)
        self.assertEqual(future, 30)
        self.assertEqual(rule_sum, 100)
        self.assertEqual(customer_fail, 100)
        outcome = self._metric(QUALITY_CHECK_RESULT_COLUMN, "customers")
        self.assertEqual(outcome.failed, customer_fail)
        self.assertEqual(outcome.passed, 10010 - customer_fail)
        order_fail = self.result.table_results["orders"].fail_rows
        order_rule_sum = (
            self._metric(ORDER_CUSTOMER_CODE, "orders").failed
            + self._metric(ORDER_PRODUCT_CODE, "orders").failed
            + self._metric(ORDER_UNIQ_CODE, "orders").failed
            + self._metric(CUSTOMER_ORPHAN_CODE, "orders").failed
            + self._metric(PRODUCT_ORPHAN_CODE, "orders").failed
        )
        self.assertEqual(order_rule_sum, 420)
        self.assertEqual(order_fail, 420)
        self.assertEqual(self._metric(QUALITY_CHECK_RESULT_COLUMN, "orders").failed, 420)
        # Documented: summing rule failures is not the definition of FAIL rows.
        # On this seed they happen to match because mandatory classes are disjoint.
        self.assertEqual(
            self.result.table_results["customers"].pass_rows
            + self.result.table_results["customers"].fail_rows,
            10010,
        )

    def test_expected_vs_observed_on_seed_42(self) -> None:
        for key, expected in create_silver.INTENTIONAL_FAIL_COUNTS.items():
            table_name, check_name = key
            matches = [
                item
                for item in self.result.metrics
                if item.table_name == table_name and item.check_name == check_name
            ]
            if not matches:
                continue
            item = matches[0]
            self.assertEqual(item.expected_fail_count, expected, check_name)
            self.assertEqual(item.failed, expected, check_name)

    def test_bronze_columns_and_keys_preserved_after_combine(self) -> None:
        bronze_customers = self.spark.table(self.config.bronze_table("customers"))
        silver_customers = self.result.tables["customers"]
        self.assertIn("customer_id", silver_customers.columns)
        self.assertIn("email", silver_customers.columns)
        self.assertNotIn("quality_check_result", bronze_customers.columns)
        valid = silver_customers.filter(
            (F.col("customer_id") == 1) | (F.col(QUALITY_CHECK_RESULT_COLUMN) == "PASS")
        )
        self.assertGreater(valid.count(), 0)
        future_rows = silver_customers.filter(
            F.array_contains(F.col("business_logic_failed_checks"), F.lit(SIGNUP_FUTURE_CODE))
        )
        self.assertEqual(future_rows.count(), 30)
        self.assertEqual(future_rows.filter(F.col("completeness_pass")).count(), 30)
        self.assertEqual(future_rows.filter(F.col("uniqueness_pass")).count(), 30)
        self.assertEqual(
            future_rows.filter(F.col(QUALITY_CHECK_RESULT_COLUMN) == "FAIL").count(),
            30,
        )


if __name__ == "__main__":
    unittest.main()
