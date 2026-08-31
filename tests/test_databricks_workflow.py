"""
Spark-free contract tests for the Databricks bootstrap/validation workflow.

These tests do not require a Databricks workspace. They query real CSVs,
temp copies, and module source. They do not prove a Databricks run.
"""

from __future__ import annotations

import ast
import importlib.util
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
DATABRICKS_DIR = SRC_DIR / "databricks"
BRONZE_DIR = SRC_DIR / "bronze"
DATA_DIR = REPO_ROOT / "data"
GENERATOR_DIR = SRC_DIR / "data_generation"

for _path in (str(SRC_DIR), str(BRONZE_DIR), str(DATABRICKS_DIR), str(GENERATOR_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from config import DEFAULT_TABLE_FORMAT, repo_root  # noqa: E402
from generate_sample_data import (  # noqa: E402
    AS_OF_DATE,
    N_CUSTOMER_DUPLICATE_ROWS,
    N_CUSTOMER_NULL_EMAIL,
    N_CUSTOMERS,
    N_FUTURE_SIGNUPS,
    N_ORDER_DUPLICATE_ROWS,
    N_ORDER_NULL_CUSTOMER_ID,
    N_ORDER_NULL_PRODUCT_ID,
    N_ORDER_ORPHAN_CUSTOMER_ID,
    N_ORDER_ORPHAN_PRODUCT_ID,
    N_ORDERS,
    N_PRODUCTS,
)

import bootstrap as dbx_bootstrap  # noqa: E402
import validate as dbx_validate  # noqa: E402


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class TestExpectationAlignment(unittest.TestCase):
    def test_counts_match_generator_contract(self) -> None:
        expected = dbx_validate.expected_source_contract()
        self.assertEqual(expected["customers_rows"], N_CUSTOMERS + N_CUSTOMER_DUPLICATE_ROWS)
        self.assertEqual(expected["orders_rows"], N_ORDERS + N_ORDER_DUPLICATE_ROWS)
        self.assertEqual(expected["products_rows"], N_PRODUCTS)
        self.assertEqual(expected["null_emails"], N_CUSTOMER_NULL_EMAIL)
        self.assertEqual(expected["null_order_customer_id"], N_ORDER_NULL_CUSTOMER_ID)
        self.assertEqual(expected["null_order_product_id"], N_ORDER_NULL_PRODUCT_ID)
        self.assertEqual(expected["duplicate_customer_keys"], N_CUSTOMER_DUPLICATE_ROWS)
        self.assertEqual(expected["duplicate_customer_rows"], N_CUSTOMER_DUPLICATE_ROWS * 2)
        self.assertEqual(expected["duplicate_order_keys"], N_ORDER_DUPLICATE_ROWS)
        self.assertEqual(expected["duplicate_order_rows"], N_ORDER_DUPLICATE_ROWS * 2)
        self.assertEqual(expected["orphan_customer_id"], N_ORDER_ORPHAN_CUSTOMER_ID)
        self.assertEqual(expected["orphan_product_id"], N_ORDER_ORPHAN_PRODUCT_ID)
        self.assertEqual(expected["future_signups"], N_FUTURE_SIGNUPS)
        self.assertEqual(expected["mandatory_issue_instances"], 460)
        self.assertEqual(dbx_validate.AS_OF_DATE, AS_OF_DATE)

    def test_pipeline_config_uses_volume_path_and_delta(self) -> None:
        runtime = dbx_bootstrap.load_databricks_runtime_config(
            catalog="workspace",
            source_schema="de_c1",
            source_volume="source_data",
            spark_app_name="DE_C1_Databricks",
        )
        pipeline = runtime.pipeline_config()
        self.assertEqual(pipeline.data_path, "/Volumes/workspace/de_c1/source_data")
        self.assertEqual(pipeline.catalog, "workspace")
        self.assertEqual(pipeline.bronze_schema, "bronze")
        self.assertEqual(pipeline.silver_schema, "silver")
        self.assertEqual(pipeline.gold_schema, "gold")
        self.assertEqual(pipeline.table_format, DEFAULT_TABLE_FORMAT)
        self.assertEqual(pipeline.table_format, "delta")
        self.assertEqual(pipeline.spark_app_name, "DE_C1_Databricks")
        self.assertEqual(pipeline.bronze_table("customers"), "workspace.bronze.customers")


class TestValidationReport(unittest.TestCase):
    def test_pass_and_fail_formatting(self) -> None:
        report = dbx_validate.ValidationReport()
        report.compare("rows", 3, 3, notes="ok")
        report.compare("nulls", 50, 49, notes="mismatch")
        text = report.format_table()
        self.assertIn("CHECK", text)
        self.assertIn("EXPECTED", text)
        self.assertIn("ACTUAL", text)
        self.assertIn("STATUS", text)
        self.assertIn("NOTES", text)
        self.assertIn("FINAL RESULT: FAIL", text)
        self.assertEqual(report.final_result(), "FAIL")
        self.assertEqual(len(report.critical_failures), 1)

    def test_all_pass(self) -> None:
        report = dbx_validate.ValidationReport()
        report.compare("rows", 1, 1)
        self.assertTrue(report.passed)
        self.assertIn("FINAL RESULT: PASS", report.format_table())


class TestSourceValidationQueriesFiles(unittest.TestCase):
    def test_committed_csvs_match_contract(self) -> None:
        report = dbx_validate.validate_source_files(DATA_DIR)
        self.assertTrue(report.passed, report.format_table())
        self.assertGreaterEqual(len(report.checks), 14)

    def test_wrong_row_count_fails(self) -> None:
        temp = Path(tempfile.mkdtemp(prefix="de_c1_dbx_bad_"))
        try:
            for name in ("customers.csv", "orders.csv", "products.csv"):
                shutil.copyfile(DATA_DIR / name, temp / name)
            customers_path = temp / "customers.csv"
            rows = customers_path.read_text(encoding="utf-8").splitlines()
            customers_path.write_text("\n".join(rows[:20]) + "\n", encoding="utf-8")
            report = dbx_validate.validate_source_files(temp)
            self.assertFalse(report.passed)
            failed = {item.check: item for item in report.critical_failures}
            self.assertIn("source.customers.rows", failed)
            self.assertEqual(failed["source.customers.rows"].expected, "10010")
            self.assertNotEqual(failed["source.customers.rows"].actual, "10010")
        finally:
            shutil.rmtree(temp, ignore_errors=True)

    def test_missing_file_raises(self) -> None:
        temp = Path(tempfile.mkdtemp(prefix="de_c1_dbx_missing_"))
        try:
            with self.assertRaises(dbx_validate.ValidationError):
                dbx_validate.validate_source_files(temp)
        finally:
            shutil.rmtree(temp, ignore_errors=True)


class TestSourceCopy(unittest.TestCase):
    def test_copy_preserves_bytes_and_hash(self) -> None:
        temp = Path(tempfile.mkdtemp(prefix="de_c1_dbx_copy_"))
        git_data = temp / "git_data"
        volume = temp / "volume"
        git_data.mkdir()
        volume.mkdir()
        try:
            for name in ("customers.csv", "orders.csv", "products.csv"):
                shutil.copyfile(DATA_DIR / name, git_data / name)
            copied = dbx_bootstrap.copy_git_sources_to_volume(git_data, str(volume))
            self.assertEqual(len(copied), 3)
            for item in copied:
                source = git_data / item.filename
                dest = volume / item.filename
                self.assertTrue(dest.is_file())
                self.assertEqual(source.read_bytes(), dest.read_bytes())
                self.assertEqual(item.sha256, dbx_bootstrap.sha256_file(source))
                self.assertEqual(item.mechanism, "posix_direct")
        finally:
            shutil.rmtree(temp, ignore_errors=True)

    def test_missing_git_sources_fail(self) -> None:
        temp = Path(tempfile.mkdtemp(prefix="de_c1_dbx_gitmiss_"))
        try:
            with self.assertRaises(dbx_bootstrap.DatabricksBootstrapError):
                dbx_bootstrap.verify_git_source_files(temp)
        finally:
            shutil.rmtree(temp, ignore_errors=True)

    def test_default_git_data_path_is_repo_data(self) -> None:
        self.assertEqual(dbx_bootstrap.default_git_data_path(), repo_root() / "data")
        source = (DATABRICKS_DIR / "bootstrap.py").read_text(encoding="utf-8")
        self.assertIn("repo_root() / \"data\"", source)


class TestResetScope(unittest.TestCase):
    def test_plan_lists_only_evaluation_objects(self) -> None:
        runtime = dbx_bootstrap.load_databricks_runtime_config()
        plan = dbx_bootstrap.plan_reset(runtime)
        joined = " ".join(plan.tables + plan.volume_files + plan.descriptions)
        self.assertTrue(all(item.startswith("workspace.") for item in plan.tables))
        self.assertIn("workspace.bronze.customers", plan.tables)
        self.assertIn("workspace.silver.quality_metrics", plan.tables)
        self.assertIn("workspace.gold.customer_segmentation", plan.tables)
        self.assertIn("/Volumes/workspace/de_c1/source_data/customers.csv", plan.volume_files)
        self.assertIn("Do not DROP CATALOG", joined)
        self.assertIn("Do not modify Git repository data/", joined)
        self.assertNotIn("hive_metastore", joined)
        self.assertNotIn("information_schema", joined)

    def test_reset_refuses_non_pipeline_schema(self) -> None:
        runtime = dbx_bootstrap.load_databricks_runtime_config(bronze_schema="analytics")
        with self.assertRaises(dbx_bootstrap.DatabricksBootstrapError):
            dbx_bootstrap.assert_reset_scope(runtime)

    def test_reset_refuses_other_source_schema(self) -> None:
        runtime = dbx_bootstrap.load_databricks_runtime_config(source_schema="other")
        with self.assertRaises(dbx_bootstrap.DatabricksBootstrapError):
            dbx_bootstrap.assert_reset_scope(runtime)

    def test_normal_policy_does_not_drop(self) -> None:
        policy = dbx_bootstrap.resource_policy()
        self.assertIn("safe_create_if_not_exists", policy)
        create = " ".join(policy["safe_create_if_not_exists"]).lower()
        self.assertIn("schema {catalog}.bronze", create)
        self.assertIn("volume {catalog}.de_c1.source_data", create)
        self.assertIn("ingest_metadata", " ".join(policy["append_only"]))
        self.assertIn("Git repository data", " ".join(policy["preserved_across_runs"]))
        self.assertNotIn("drop catalog", create)


class TestDashboardSqlLoader(unittest.TestCase):
    def test_required_queries_load(self) -> None:
        queries = dbx_validate.load_dashboard_queries("workspace.gold")
        for name in (
            "top_10_products",
            "customer_revenue_distribution",
            "customer_segmentation",
            "filter_values_category",
            "filter_values_customer_segment",
        ):
            self.assertIn(name, queries)
            self.assertIn("workspace.gold", queries[name])
            self.assertNotIn("{gold_schema}", queries[name])
            self.assertNotIn("-- DASHBOARD_QUERY", queries[name])
        top = queries["top_10_products"].upper()
        self.assertIn("LIMIT 10", top)
        self.assertIn("SALES_BY_PRODUCT", top)
        self.assertNotIn("BRONZE.", queries["top_10_products"].upper())
        self.assertNotIn("SILVER.", queries["top_10_products"].upper())


class TestOrchestratorUsesExistingCode(unittest.TestCase):
    def test_run_pipeline_calls_existing_modules(self) -> None:
        source = (DATABRICKS_DIR / "run_pipeline.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        calls: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    calls.add(func.id)
                elif isinstance(func, ast.Attribute):
                    calls.add(func.attr)
        self.assertIn("ingest_all", calls)
        self.assertIn("create_silver_tables", calls)
        self.assertIn("create_gold_tables", calls)
        self.assertIn("bootstrap_environment", calls)
        self.assertIn("validate_bronze", calls)
        self.assertIn("validate_silver", calls)
        self.assertIn("validate_gold", calls)
        self.assertIn("validate_dashboard_sql", calls)
        self.assertNotIn("dropDuplicates", source)
        self.assertNotIn("dropna(", source)

    def test_cli_defaults_and_reset_flag(self) -> None:
        run_pipeline = _load_module(
            "de_c1_run_pipeline", DATABRICKS_DIR / "run_pipeline.py"
        )
        parser = run_pipeline.build_parser()
        args = parser.parse_args([])
        self.assertEqual(args.stage, "all")
        self.assertFalse(args.reset)
        self.assertEqual(args.spark_app_name, "DE_C1_Databricks")
        reset_args = parser.parse_args(["--reset", "--stage", "bronze"])
        self.assertTrue(reset_args.reset)
        self.assertEqual(reset_args.stage, "bronze")

    def test_main_refuses_non_databricks_without_allow_local(self) -> None:
        run_pipeline = _load_module(
            "de_c1_run_pipeline_guard", DATABRICKS_DIR / "run_pipeline.py"
        )
        previous = os.environ.pop("DATABRICKS_RUNTIME_VERSION", None)
        try:
            code = run_pipeline.main([])
            self.assertEqual(code, 2)
        finally:
            if previous is not None:
                os.environ["DATABRICKS_RUNTIME_VERSION"] = previous

    def test_no_user_paths_or_secrets_in_databricks_modules(self) -> None:
        forbidden = (
            "udayan",
            "tothenew",
            "/workspace/users/",
            "akiai",
            "dapi",
            ".venv",
            "winutils",
            "c:\\users",
        )
        for path in DATABRICKS_DIR.glob("*.py"):
            lowered = path.read_text(encoding="utf-8").lower()
            for token in forbidden:
                self.assertNotIn(token, lowered, f"{path.name} contains {token}")


class TestGoldEligibilityConstant(unittest.TestCase):
    def test_eligibility_matches_frozen_rule(self) -> None:
        predicate = dbx_validate.GOLD_ELIGIBILITY_PREDICATE
        self.assertIn("Completed", predicate)
        self.assertIn("quality_check_result = 'PASS'", predicate)
        self.assertIn("AND", predicate)


if __name__ == "__main__":
    unittest.main()
