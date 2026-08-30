"""
Spark-free Bronze contract tests.

These always run. They assert the frozen schema, CSV options, table names,
configuration defaults, header validation, and that ingest source does not
invoke row-dropping APIs.

They do not execute Spark and therefore cannot prove a Databricks write.
See tests/test_bronze_ingest.py for runtime tests (skipped without PySpark).
"""

from __future__ import annotations

import ast
import os
import sys
import unittest
from pathlib import Path

_MEDALLION_ENV = (
    "MEDALLION_DATA_PATH",
    "MEDALLION_CATALOG",
    "MEDALLION_BRONZE_SCHEMA",
    "MEDALLION_SILVER_SCHEMA",
    "MEDALLION_GOLD_SCHEMA",
    "MEDALLION_TABLE_FORMAT",
    "MEDALLION_SPARK_APP_NAME",
)

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
BRONZE_DIR = SRC_DIR / "bronze"
sys.path.insert(0, str(SRC_DIR))
sys.path.insert(0, str(BRONZE_DIR))

from config import (  # noqa: E402
    DEFAULT_BRONZE_SCHEMA,
    DEFAULT_TABLE_FORMAT,
    ConfigError,
    default_data_path,
    load_config,
    repo_root,
)
from contracts import (  # noqa: E402
    BRONZE_CUSTOMERS_TABLE,
    BRONZE_ENTITY_TABLES,
    BRONZE_METADATA_TABLE,
    BRONZE_ORDERS_TABLE,
    BRONZE_PRODUCTS_TABLE,
    CSV_READ_OPTIONS,
    CUSTOMER_SOURCE_FIELDS,
    ENTITY_CONTRACTS,
    INGEST_ROW_ID_COLUMN,
    METADATA_FIELDS,
    ORDER_SOURCE_FIELDS,
    PRODUCT_SOURCE_FIELDS,
    has_uri_scheme,
    join_source_path,
    spark_input_path,
)
from ingest_core import (  # noqa: E402
    BronzeIngestError,
    ingest_all,
    ingest_customers,
    ingest_orders,
    ingest_products,
    new_ingest_id,
    validate_header,
)


class TestBronzeTableNames(unittest.TestCase):
    def test_entity_table_names_are_unambiguous(self) -> None:
        self.assertEqual(BRONZE_ENTITY_TABLES, ("customers", "orders", "products"))
        self.assertEqual(BRONZE_METADATA_TABLE, "ingest_metadata")
        self.assertEqual(
            {BRONZE_CUSTOMERS_TABLE, BRONZE_ORDERS_TABLE, BRONZE_PRODUCTS_TABLE},
            set(BRONZE_ENTITY_TABLES),
        )

    def test_not_using_flattened_default_schema_names(self) -> None:
        """Layer is the schema; entity is the table. Not bronze_customers in default."""
        for name in BRONZE_ENTITY_TABLES:
            self.assertFalse(name.startswith("bronze_"))
            self.assertNotIn("silver", name)


class TestBronzeSchemaContract(unittest.TestCase):
    def test_customer_schema(self) -> None:
        self.assertEqual(
            CUSTOMER_SOURCE_FIELDS,
            (
                ("customer_id", "INT"),
                ("customer_name", "STRING"),
                ("email", "STRING"),
                ("country", "STRING"),
                ("signup_date", "DATE"),
                ("customer_segment", "STRING"),
                ("lifetime_value", "DECIMAL(18,2)"),
            ),
        )

    def test_order_schema(self) -> None:
        self.assertEqual(
            ORDER_SOURCE_FIELDS,
            (
                ("order_id", "INT"),
                ("customer_id", "INT"),
                ("order_date", "DATE"),
                ("product_id", "INT"),
                ("quantity", "INT"),
                ("unit_price", "DECIMAL(18,2)"),
                ("total_amount", "DECIMAL(18,2)"),
                ("order_status", "STRING"),
                ("payment_date", "DATE"),
            ),
        )

    def test_product_schema(self) -> None:
        self.assertEqual(
            PRODUCT_SOURCE_FIELDS,
            (
                ("product_id", "INT"),
                ("product_name", "STRING"),
                ("category", "STRING"),
                ("price", "DECIMAL(18,2)"),
                ("cost", "DECIMAL(18,2)"),
                ("stock_quantity", "INT"),
                ("reorder_level", "INT"),
            ),
        )

    def test_metadata_schema(self) -> None:
        names = [name for name, _type in METADATA_FIELDS]
        self.assertEqual(
            names,
            [
                "ingest_id",
                "source_file",
                "table_name",
                "row_count",
                "ingested_at",
                "status",
                "error_message",
            ],
        )

    def test_lineage_column_is_technical(self) -> None:
        self.assertEqual(INGEST_ROW_ID_COLUMN, "_ingest_row_id")
        for contract in ENTITY_CONTRACTS.values():
            self.assertNotIn(INGEST_ROW_ID_COLUMN, contract.source_columns)

    def test_source_filenames(self) -> None:
        self.assertEqual(ENTITY_CONTRACTS["customers"].filename, "customers.csv")
        self.assertEqual(ENTITY_CONTRACTS["orders"].filename, "orders.csv")
        self.assertEqual(ENTITY_CONTRACTS["products"].filename, "products.csv")


class TestCsvReadOptions(unittest.TestCase):
    def test_permissive_mode(self) -> None:
        self.assertEqual(CSV_READ_OPTIONS["mode"], "PERMISSIVE")

    def test_explicit_schema_flags(self) -> None:
        self.assertEqual(CSV_READ_OPTIONS["inferSchema"], "false")
        self.assertEqual(CSV_READ_OPTIONS["header"], "true")
        self.assertEqual(CSV_READ_OPTIONS["dateFormat"], "yyyy-MM-dd")
        self.assertEqual(CSV_READ_OPTIONS["nullValue"], "")
        self.assertEqual(CSV_READ_OPTIONS["encoding"], "UTF-8")

    def test_no_trim_cleanse(self) -> None:
        self.assertEqual(CSV_READ_OPTIONS["ignoreLeadingWhiteSpace"], "false")
        self.assertEqual(CSV_READ_OPTIONS["ignoreTrailingWhiteSpace"], "false")


class IsolatedMedallionEnvMixin:
    def setUp(self) -> None:
        self._saved_env = {key: os.environ.get(key) for key in _MEDALLION_ENV}
        for key in _MEDALLION_ENV:
            os.environ.pop(key, None)

    def tearDown(self) -> None:
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


class TestConfig(IsolatedMedallionEnvMixin, unittest.TestCase):
    def test_default_data_path_is_repo_relative(self) -> None:
        path = default_data_path()
        self.assertEqual(Path(path).resolve(), (repo_root() / "data").resolve())
        lowered = path.replace("\\", "/").lower()
        self.assertNotIn("/workspace/users/", lowered)
        self.assertNotIn("databricks.com", lowered)

    def test_load_config_defaults(self) -> None:
        cfg = load_config()
        self.assertEqual(cfg.bronze_schema, DEFAULT_BRONZE_SCHEMA)
        self.assertEqual(cfg.table_format, DEFAULT_TABLE_FORMAT)
        self.assertIsNone(cfg.catalog)
        self.assertEqual(cfg.bronze_table("customers"), "bronze.customers")

    def test_catalog_qualification(self) -> None:
        cfg = load_config(catalog="dev", bronze_schema="bronze")
        self.assertEqual(cfg.bronze_table("orders"), "dev.bronze.orders")

    def test_rejects_unsafe_identifiers(self) -> None:
        with self.assertRaises(ConfigError):
            load_config(bronze_schema="bronze; DROP SCHEMA gold")
        with self.assertRaises(ConfigError):
            load_config(catalog="https://example.cloud.databricks.com")

    def test_rejects_unknown_format(self) -> None:
        with self.assertRaises(ConfigError):
            load_config(table_format="csv")

    def test_explicit_data_path_not_windows_user_profile(self) -> None:
        cfg = load_config(data_path="/Volumes/main/bronze/landing")
        self.assertEqual(cfg.data_path, "/Volumes/main/bronze/landing")
        self.assertTrue(has_uri_scheme(cfg.data_path))


class TestPathHelpers(unittest.TestCase):
    def test_join_local_and_remote(self) -> None:
        self.assertTrue(join_source_path("data", "customers.csv").endswith("customers.csv"))
        self.assertEqual(
            join_source_path("dbfs:/mnt/raw", "orders.csv"),
            "dbfs:/mnt/raw/orders.csv",
        )
        self.assertEqual(
            join_source_path("s3://bucket/prefix/", "products.csv"),
            "s3://bucket/prefix/products.csv",
        )

    def test_windows_drive_is_not_a_uri_scheme(self) -> None:
        self.assertFalse(has_uri_scheme(r"D:\data\customers.csv"))
        self.assertTrue(has_uri_scheme("dbfs:/data/customers.csv"))
        self.assertTrue(has_uri_scheme("/Volumes/main/landing"))

    def test_spark_input_path_passthrough_remote(self) -> None:
        self.assertEqual(spark_input_path("s3://bucket/raw/customers.csv"), "s3://bucket/raw/customers.csv")
        self.assertEqual(spark_input_path("dbfs:/mnt/raw/orders.csv"), "dbfs:/mnt/raw/orders.csv")
        self.assertEqual(
            spark_input_path("/Volumes/main/landing/products.csv"),
            "/Volumes/main/landing/products.csv",
        )
        self.assertEqual(
            spark_input_path("abfss://container@account.dfs.core.windows.net/raw/x.csv"),
            "abfss://container@account.dfs.core.windows.net/raw/x.csv",
        )

    def test_spark_input_path_local_not_percent_encoded(self) -> None:
        local = spark_input_path(str(Path.cwd() / "file with spaces.csv"))
        self.assertNotIn("%20", local)
        self.assertFalse(local.lower().startswith("file:"))
        self.assertIn("file with spaces.csv", local.replace("\\", "/"))

    def test_spark_input_path_does_not_call_as_uri(self) -> None:
        source = (BRONZE_DIR / "contracts.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr == "as_uri":
                self.fail("spark_input_path must not call Path.as_uri()")
        self.assertNotIn("Udayan", source)
        self.assertIn("as_posix()", source)

    def test_local_windows_fs_adapter_is_present_and_gated(self) -> None:
        java = SRC_DIR / "local_runtime" / "NoWinutilsRawLocalFileSystem.java"
        helper = SRC_DIR / "spark_local.py"
        self.assertTrue(java.is_file(), "local Windows FileSystem source is missing")
        helper_text = helper.read_text(encoding="utf-8")
        self.assertIn('os.name != "nt"', helper_text)
        self.assertIn("fs.file.impl", helper_text)
        java_text = java.read_text(encoding="utf-8")
        self.assertIn("listStatus", java_text)
        self.assertIn("setPermission", java_text)


class TestHeaderValidation(unittest.TestCase):
    def test_accepts_exact_header(self) -> None:
        contract = ENTITY_CONTRACTS["customers"]
        validate_header(list(contract.source_columns), contract, "customers.csv")

    def test_rejects_mismatch_with_diff(self) -> None:
        contract = ENTITY_CONTRACTS["customers"]
        with self.assertRaises(BronzeIngestError) as ctx:
            validate_header(["id", "name"], contract, "customers.csv")
        message = str(ctx.exception)
        self.assertIn("Expected", message)
        self.assertIn("Missing", message)
        self.assertIn("customer_id", message)

    def test_rejects_bom(self) -> None:
        contract = ENTITY_CONTRACTS["products"]
        header = ["\ufeffproduct_id"] + list(contract.source_columns)[1:]
        with self.assertRaises(BronzeIngestError) as ctx:
            validate_header(header, contract, "products.csv")
        self.assertIn("BOM", str(ctx.exception))


class TestOrchestrationSurface(unittest.TestCase):
    def test_ingest_all_is_the_orchestrator(self) -> None:
        self.assertTrue(callable(ingest_all))
        self.assertTrue(callable(ingest_customers))
        self.assertTrue(callable(ingest_orders))
        self.assertTrue(callable(ingest_products))

    def test_ingest_all_source_calls_all_three_entities(self) -> None:
        source = (BRONZE_DIR / "ingest_core.py").read_text(encoding="utf-8")
        self.assertIn("preflight_all_sources", source)
        self.assertIn("BRONZE_ENTITY_TABLES", source)
        self.assertIn('"customers"', source)
        self.assertIn('"orders"', source)
        self.assertIn('"products"', source)

    def test_numbered_scripts_delegate_to_core(self) -> None:
        customers = (BRONZE_DIR / "01_ingest_customers.py").read_text(encoding="utf-8")
        orders = (BRONZE_DIR / "02_ingest_orders.py").read_text(encoding="utf-8")
        products = (BRONZE_DIR / "03_ingest_products.py").read_text(encoding="utf-8")
        all_script = (BRONZE_DIR / "ingest_all.py").read_text(encoding="utf-8")
        self.assertIn("ingest_customers", customers)
        self.assertIn("ingest_orders", orders)
        self.assertIn("ingest_products", products)
        self.assertIn("ingest_all", all_script)
        self.assertIn("cli_main", all_script)

    def test_ingest_id_is_unique_per_call(self) -> None:
        self.assertNotEqual(new_ingest_id(), new_ingest_id())


class TestNoRowDroppingInSource(unittest.TestCase):
    """Parse Bronze Python with AST so comments cannot hide a real drop call."""

    FORBIDDEN_CALLS = {
        "dropDuplicates",
        "drop_duplicates",
        "dropna",
        "drop",
    }

    def _call_names(self, path: Path) -> set[str]:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    names.add(func.id)
                elif isinstance(func, ast.Attribute):
                    names.add(func.attr)
        return names

    def test_ingest_modules_do_not_drop_rows(self) -> None:
        for filename in (
            "ingest_core.py",
            "01_ingest_customers.py",
            "02_ingest_orders.py",
            "03_ingest_products.py",
            "ingest_all.py",
            "contracts.py",
        ):
            path = BRONZE_DIR / filename
            calls = self._call_names(path)
            forbidden = calls & self.FORBIDDEN_CALLS
            self.assertFalse(
                forbidden,
                f"{filename} calls forbidden row-drop APIs: {forbidden}",
            )

    def test_csv_mode_literal_is_permissive(self) -> None:
        source = (BRONZE_DIR / "contracts.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        modes: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Dict):
                keys = [k.value for k in node.keys if isinstance(k, ast.Constant)]
                values = [v.value for v in node.values if isinstance(v, ast.Constant)]
                mapping = dict(zip(keys, values))
                if "mode" in mapping:
                    modes.append(str(mapping["mode"]))
        self.assertIn("PERMISSIVE", modes)
        self.assertNotIn("DROPMALFORMED", modes)
        self.assertNotIn("FAILFAST", modes)

    def test_no_silver_quality_filter_in_bronze(self) -> None:
        source = (BRONZE_DIR / "ingest_core.py").read_text(encoding="utf-8")
        self.assertNotIn("quality_check_result", source)
        self.assertNotIn("failed_checks", source)
        self.assertNotIn("from silver", source)


class TestCommittedSourceRowCounts(unittest.TestCase):
    """Source physical counts (not Bronze). Used as the expected ingest targets."""

    def _count_data_rows(self, path: Path) -> int:
        lines = path.read_text(encoding="utf-8").splitlines()
        return max(len(lines) - 1, 0)

    def test_committed_csv_counts(self) -> None:
        data = REPO_ROOT / "data"
        self.assertEqual(self._count_data_rows(data / "customers.csv"), 10010)
        self.assertEqual(self._count_data_rows(data / "orders.csv"), 100020)
        self.assertEqual(self._count_data_rows(data / "products.csv"), 500)


class TestBronzeFixtures(unittest.TestCase):
    """Executable checks on the Spark-test fixtures (csv module, no Spark)."""

    def _rows(self, name: str) -> list[dict[str, str]]:
        import csv

        path = REPO_ROOT / "tests" / "fixtures" / "bronze" / name
        with path.open(encoding="utf-8", newline="") as handle:
            return list(csv.DictReader(handle))

    def test_fixture_counts_and_defects(self) -> None:
        customers = self._rows("customers.csv")
        orders = self._rows("orders.csv")
        products = self._rows("products.csv")
        self.assertEqual(len(customers), 4)
        self.assertEqual(len(orders), 6)
        self.assertEqual(len(products), 2)
        self.assertEqual(sum(1 for row in customers if row["email"] == ""), 1)
        self.assertEqual(sum(1 for row in customers if row["customer_id"] == "1"), 2)
        self.assertEqual(sum(1 for row in orders if row["order_id"] == "10"), 2)
        self.assertEqual(sum(1 for row in orders if row["customer_id"] == ""), 1)
        self.assertEqual(sum(1 for row in orders if row["product_id"] == ""), 1)
        self.assertEqual(sum(1 for row in orders if row["customer_id"] == "90001"), 1)
        self.assertEqual(sum(1 for row in orders if row["product_id"] == "9001"), 1)
        product_ids = {row["product_id"] for row in products}
        customer_ids = {row["customer_id"] for row in customers}
        self.assertNotIn("9001", product_ids)
        self.assertNotIn("90001", customer_ids)

    def test_stubs_replaced(self) -> None:
        for filename in (
            "01_ingest_customers.py",
            "02_ingest_orders.py",
            "03_ingest_products.py",
            "ingest_all.py",
            "ingest_core.py",
        ):
            text = (BRONZE_DIR / filename).read_text(encoding="utf-8")
            self.assertNotIn("NotImplementedError", text)


class TestDefaultConfigDoesNotReadEnvSecrets(unittest.TestCase):
    def test_no_token_keys_in_config_module(self) -> None:
        text = (SRC_DIR / "config.py").read_text(encoding="utf-8")
        lowered = text.lower()
        for needle in ("password", "secret", "token", "akid", "akia"):
            self.assertNotIn(needle, lowered)


if __name__ == "__main__":
    unittest.main()
