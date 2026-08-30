"""
Bronze Spark ingest tests.

Requires a local PySpark runtime. If PySpark is not installed, the entire
module is skipped — that is BLOCKED runtime evidence, not a pass.

These tests use tiny fixtures under tests/fixtures/bronze/ plus, when Spark
is available, the committed data/ CSVs. They never collect the full orders
dataset to the driver.

Databricks-specific checks (Unity Catalog, Delta commit, DBFS/S3 read, SQL
warehouse) are not executed here.
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
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "bronze"
DATA_DIR = REPO_ROOT / "data"

sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(BRONZE_DIR))

try:
    from pyspark.sql import SparkSession

    PYSPARK_AVAILABLE = True
    PYSPARK_IMPORT_ERROR = ""
except ImportError as exc:  # pragma: no cover - depends on the environment
    SparkSession = None  # type: ignore[misc, assignment]
    PYSPARK_AVAILABLE = False
    PYSPARK_IMPORT_ERROR = str(exc)

from config import load_config  # noqa: E402
from contracts import (  # noqa: E402
    BRONZE_ENTITY_TABLES,
    BRONZE_METADATA_TABLE,
    CUSTOMER_SOURCE_FIELDS,
    INGEST_ROW_ID_COLUMN,
)
from ingest_core import (  # noqa: E402
    BronzeIngestError,
    ingest_all,
    ingest_customers,
)
from spark_local import apply_local_spark_config  # noqa: E402


def _simple_type(spark_dtype) -> str:
    return spark_dtype.simpleString().replace(" ", "")


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Spark ingest tests are BLOCKED in this environment.",
)
class BronzeSparkTestCase(unittest.TestCase):
    spark = None
    warehouse_dir: Path

    @classmethod
    def setUpClass(cls) -> None:
        cls.warehouse_dir = Path(tempfile.mkdtemp(prefix="de_c1_bronze_wh_"))
        builder = (
            SparkSession.builder.master("local[2]")
            .appName("de-c1-bronze-tests")
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
    def _fixture_config(data_path: Path, schema: str = "bronze"):
        return load_config(
            data_path=str(data_path),
            catalog=None,
            bronze_schema=schema,
            table_format="parquet",
        )


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Spark ingest tests are BLOCKED in this environment.",
)
class TestFixtureIngest(BronzeSparkTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.config = cls._fixture_config(FIXTURE_DIR, schema="bronze_fx")
        cls.results = ingest_all(cls.spark, cls.config)

    def _table(self, name: str):
        return self.spark.table(self.config.bronze_table(name))

    def test_orchestration_covers_all_three_datasets(self) -> None:
        self.assertEqual([item.table_name for item in self.results], list(BRONZE_ENTITY_TABLES))
        ingest_ids = {item.ingest_id for item in self.results}
        self.assertEqual(len(ingest_ids), 1)

    def test_physical_row_counts_match_source(self) -> None:
        self.assertEqual(self._table("customers").count(), 4)
        self.assertEqual(self._table("orders").count(), 6)
        self.assertEqual(self._table("products").count(), 2)
        counts = {item.table_name: item.row_count for item in self.results}
        self.assertEqual(counts, {"customers": 4, "orders": 6, "products": 2})

    def test_source_columns_preserved(self) -> None:
        customers = self._table("customers")
        for name, _type in CUSTOMER_SOURCE_FIELDS:
            self.assertIn(name, customers.columns)
        self.assertIn(INGEST_ROW_ID_COLUMN, customers.columns)
        self.assertNotIn("quality_check_result", customers.columns)
        self.assertNotIn("failed_checks", customers.columns)

    def test_explicit_schema_types(self) -> None:
        customers = {f.name: _simple_type(f.dataType) for f in self._table("customers").schema.fields}
        orders = {f.name: _simple_type(f.dataType) for f in self._table("orders").schema.fields}
        products = {f.name: _simple_type(f.dataType) for f in self._table("products").schema.fields}
        self.assertEqual(customers["customer_id"], "int")
        self.assertEqual(customers["signup_date"], "date")
        self.assertEqual(customers["lifetime_value"], "decimal(18,2)")
        self.assertEqual(customers[INGEST_ROW_ID_COLUMN], "bigint")
        self.assertEqual(orders["unit_price"], "decimal(18,2)")
        self.assertEqual(orders["total_amount"], "decimal(18,2)")
        self.assertEqual(orders["payment_date"], "date")
        self.assertEqual(products["price"], "decimal(18,2)")
        self.assertEqual(products["stock_quantity"], "int")

    def test_nulls_retained(self) -> None:
        self.assertEqual(self._table("customers").filter("email IS NULL").count(), 1)
        self.assertEqual(self._table("orders").filter("customer_id IS NULL").count(), 1)
        self.assertEqual(self._table("orders").filter("product_id IS NULL").count(), 1)
        self.assertEqual(self._table("orders").filter("payment_date IS NULL").count(), 2)

    def test_duplicate_business_keys_retained(self) -> None:
        self.assertEqual(self._table("customers").filter("customer_id = 1").count(), 2)
        self.assertEqual(self._table("orders").filter("order_id = 10").count(), 2)
        self.assertEqual(self._table("customers").select("customer_id").distinct().count(), 3)
        self.assertEqual(self._table("orders").select("order_id").distinct().count(), 5)

    def test_orphan_foreign_keys_retained(self) -> None:
        self.assertEqual(self._table("orders").filter("customer_id = 90001").count(), 1)
        self.assertEqual(self._table("orders").filter("product_id = 9001").count(), 1)
        product_ids = {row.product_id for row in self._table("products").select("product_id").collect()}
        self.assertNotIn(9001, product_ids)
        customer_ids = {row.customer_id for row in self._table("customers").select("customer_id").collect()}
        self.assertNotIn(90001, customer_ids)

    def test_business_keys_not_rewritten(self) -> None:
        names = [
            row.customer_name
            for row in self._table("customers").filter("customer_id = 1").select("customer_name").collect()
        ]
        self.assertEqual(names, ["Alice Example", "Alice Example"])
        ltv = (
            self._table("customers")
            .filter("customer_id = 2")
            .select("lifetime_value")
            .collect()[0][0]
        )
        self.assertEqual(ltv, Decimal("5.50"))

    def test_ingest_row_id_unique_per_physical_row(self) -> None:
        for table in BRONZE_ENTITY_TABLES:
            df = self._table(table)
            self.assertEqual(
                df.select(INGEST_ROW_ID_COLUMN).distinct().count(),
                df.count(),
                f"{table} lineage ids are not unique",
            )

    def test_no_silver_filtering(self) -> None:
        orders = self._table("orders")
        self.assertEqual(orders.filter("order_status = 'Cancelled'").count(), 1)
        self.assertEqual(orders.filter("order_status = 'Pending'").count(), 1)
        self.assertEqual(orders.count(), 6)

    def test_ingestion_metadata(self) -> None:
        meta = self._table(BRONZE_METADATA_TABLE)
        self.assertGreaterEqual(meta.count(), 3)
        success = meta.filter("status = 'SUCCESS'")
        latest_id = self.results[0].ingest_id
        run_rows = success.filter(f"ingest_id = '{latest_id}'")
        self.assertEqual(run_rows.count(), 3)
        for table in BRONZE_ENTITY_TABLES:
            row = run_rows.filter(f"table_name = '{table}'").collect()[0]
            self.assertIsNotNone(row.ingested_at)
            self.assertGreater(row.row_count, 0)
            self.assertTrue(str(row.source_file).endswith(f"{table}.csv"))
            self.assertIsNone(row.error_message)

    def test_rerun_overwrites_entities_and_appends_metadata(self) -> None:
        meta_before = self._table(BRONZE_METADATA_TABLE).count()
        second = ingest_all(self.spark, self.config)
        self.assertEqual(self._table("customers").count(), 4)
        self.assertEqual(self._table("orders").count(), 6)
        self.assertEqual(self._table("products").count(), 2)
        self.assertNotEqual(second[0].ingest_id, self.results[0].ingest_id)
        self.assertEqual(self._table(BRONZE_METADATA_TABLE).count(), meta_before + 3)


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Spark ingest tests are BLOCKED in this environment.",
)
class TestIngestErrors(BronzeSparkTestCase):
    def test_missing_file_fails(self) -> None:
        missing = Path(tempfile.mkdtemp(prefix="de_c1_missing_"))
        cfg = self._fixture_config(missing, schema="bronze_missing")
        with self.assertRaises(BronzeIngestError) as ctx:
            ingest_customers(self.spark, cfg)
        self.assertIn("not found", str(ctx.exception).lower())

    def test_empty_file_fails(self) -> None:
        cfg = self._fixture_config(FIXTURE_DIR / "empty", schema="bronze_empty")
        with self.assertRaises(BronzeIngestError) as ctx:
            ingest_customers(self.spark, cfg)
        self.assertIn("empty", str(ctx.exception).lower())

    def test_header_only_fails(self) -> None:
        cfg = self._fixture_config(FIXTURE_DIR / "header_only", schema="bronze_hdr")
        with self.assertRaises(BronzeIngestError) as ctx:
            ingest_customers(self.spark, cfg)
        self.assertIn("0 data rows", str(ctx.exception).lower())

    def test_header_mismatch_fails_with_diff(self) -> None:
        cfg = self._fixture_config(FIXTURE_DIR / "bad_header", schema="bronze_bad")
        with self.assertRaises(BronzeIngestError) as ctx:
            ingest_customers(self.spark, cfg)
        message = str(ctx.exception)
        self.assertIn("Header mismatch", message)
        self.assertIn("Expected", message)

    def test_preflight_blocks_partial_overwrite(self) -> None:
        tmp = Path(tempfile.mkdtemp(prefix="de_c1_partial_"))
        (tmp / "customers.csv").write_text(
            (FIXTURE_DIR / "customers.csv").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        cfg = self._fixture_config(tmp, schema="bronze_partial")
        with self.assertRaises(BronzeIngestError) as ctx:
            ingest_all(self.spark, cfg)
        self.assertIn("Preflight failed", str(ctx.exception))
        try:
            listed = self.spark.catalog.listTables(cfg.bronze_schema)
            tables = [getattr(row, "name", getattr(row, "tableName", None)) for row in listed]
        except Exception:
            tables = []
        self.assertNotIn("customers", tables)


@unittest.skipUnless(
    PYSPARK_AVAILABLE,
    f"PySpark is not installed ({PYSPARK_IMPORT_ERROR or 'no module'}). "
    "Spark ingest tests are BLOCKED in this environment.",
)
class TestCommittedSourceIngest(BronzeSparkTestCase):
    """Full committed CSVs. Still local Spark, not Databricks."""

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.config = cls._fixture_config(DATA_DIR, schema="bronze_full")
        cls.results = ingest_all(cls.spark, cls.config)

    def _table(self, name: str):
        return self.spark.table(self.config.bronze_table(name))

    def test_full_source_versus_bronze_counts(self) -> None:
        self.assertEqual(self._table("customers").count(), 10010)
        self.assertEqual(self._table("orders").count(), 100020)
        self.assertEqual(self._table("products").count(), 500)

    def test_full_intentional_defects_observable(self) -> None:
        customers = self._table("customers")
        orders = self._table("orders")
        self.assertEqual(customers.filter("email IS NULL").count(), 50)
        dup_customers = customers.groupBy("customer_id").count().filter("count > 1")
        self.assertEqual(dup_customers.count(), 10)
        self.assertEqual(orders.filter("customer_id IS NULL").count(), 100)
        self.assertEqual(orders.filter("product_id IS NULL").count(), 200)
        customer_keys = customers.select("customer_id").distinct()
        product_keys = self._table("products").select("product_id").distinct()
        orphan_c = (
            orders.filter("customer_id IS NOT NULL")
            .join(customer_keys, on="customer_id", how="left_anti")
        )
        orphan_p = (
            orders.filter("product_id IS NOT NULL")
            .join(product_keys, on="product_id", how="left_anti")
        )
        self.assertEqual(orphan_c.count(), 50)
        self.assertEqual(orphan_p.count(), 30)
        dup_orders = orders.groupBy("order_id").count().filter("count > 1")
        self.assertEqual(dup_orders.count(), 20)

    def test_full_lineage_unique(self) -> None:
        for table in BRONZE_ENTITY_TABLES:
            df = self._table(table)
            self.assertEqual(df.select(INGEST_ROW_ID_COLUMN).distinct().count(), df.count())

    def test_full_metadata_row_counts(self) -> None:
        meta = self._table(BRONZE_METADATA_TABLE).filter(
            f"ingest_id = '{self.results[0].ingest_id}' AND status = 'SUCCESS'"
        )
        rows = {row.table_name: row.row_count for row in meta.collect()}
        self.assertEqual(rows["customers"], 10010)
        self.assertEqual(rows["orders"], 100020)
        self.assertEqual(rows["products"], 500)


if __name__ == "__main__":
    unittest.main()
