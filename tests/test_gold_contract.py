"""
Spark-free Gold contract tests.

Always runnable. They assert SQL eligibility filters, formulas, schema
placeholders, segmentation priority, DECIMAL usage, and that the
orchestrator does not replace SQL aggregations with PySpark groupBy/sum.

Spark execution lives in tests/test_gold_aggregations.py.
"""

from __future__ import annotations

import ast
import re
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
GOLD_DIR = SRC_DIR / "gold"
BRONZE_DIR = SRC_DIR / "bronze"

for _path in (str(SRC_DIR), str(BRONZE_DIR), str(GOLD_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from config import DEFAULT_GOLD_SCHEMA, load_config  # noqa: E402
from create_gold_tables import (  # noqa: E402
    GOLD_SQL_FILES,
    GOLD_TABLES,
    GoldError,
    render_gold_sql,
    split_gold_outputs,
)


def _read(filename: str) -> str:
    return (GOLD_DIR / filename).read_text(encoding="utf-8")


class TestGoldSqlContract(unittest.TestCase):
    def test_required_sql_files_exist(self) -> None:
        for filename in GOLD_SQL_FILES:
            self.assertTrue((GOLD_DIR / filename).is_file(), filename)
        self.assertTrue((GOLD_DIR / "create_gold_tables.py").is_file())

    def test_sales_by_product_columns_and_eligibility(self) -> None:
        sql = _read("01_sales_by_product.sql")
        for column in (
            "product_id",
            "product_name",
            "category",
            "total_orders",
            "total_revenue",
            "avg_order_value",
        ):
            self.assertIn(column, sql)
        self.assertIn("order_status = 'Completed'", sql)
        self.assertIn("quality_check_result = 'PASS'", sql)
        self.assertIn("NULLIF", sql)
        self.assertIn("DECIMAL(18, 2)", sql)
        self.assertIn("{silver_schema}", sql)
        executable = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        )
        self.assertNotRegex(executable, r"SUM\s*\(\s*quantity\s*\)", re.I)

    def test_revenue_by_customer_does_not_use_source_lifetime_value(self) -> None:
        sql = _read("02_revenue_by_customer.sql")
        for column in (
            "customer_id",
            "customer_name",
            "customer_segment",
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "lifetime_value_actual",
        ):
            self.assertIn(column, sql)
        self.assertIn("order_status = 'Completed'", sql)
        self.assertIn("quality_check_result = 'PASS'", sql)
        self.assertIn("NULLIF", sql)
        self.assertIn("LEFT JOIN", sql.upper())
        self.assertIn("AS lifetime_value_actual", sql)
        self.assertIn("COALESCE(a.total_revenue, 0)", sql)
        self.assertNotRegex(
            sql,
            r"lifetime_value\s+AS\s+lifetime_value_actual",
            re.I,
        )
        self.assertNotIn("c.lifetime_value", sql)
        self.assertIn("canonical_customers", sql)

    def test_trends_use_monday_week_and_decimal(self) -> None:
        sql = _read("03_daily_weekly_trends.sql")
        self.assertIn("trend_date", sql)
        self.assertIn("week_start_date", sql)
        self.assertIn("date_trunc('WEEK'", sql)
        self.assertIn("CAST(date_trunc('WEEK', order_date) AS DATE)", sql)
        self.assertIn("order_status = 'Completed'", sql)
        self.assertIn("quality_check_result = 'PASS'", sql)
        self.assertIn("DECIMAL(18, 2)", sql)
        self.assertIn("NULLIF", sql)
        self.assertIn("-- GOLD_OUTPUT: daily_trends", sql)
        self.assertIn("-- GOLD_OUTPUT: weekly_trends", sql)

    def test_segmentation_priority_and_threshold(self) -> None:
        sql = _read("04_customer_segmentation.sql")
        self.assertIn("Inactive", sql)
        self.assertIn("High-Value", sql)
        self.assertIn("Repeat", sql)
        self.assertIn("One-Time", sql)
        self.assertIn("CAST(1000.00 AS DECIMAL(18, 2))", sql)
        self.assertIn("WHEN total_orders = 0 THEN 'Inactive'", sql)
        self.assertIn("WHEN total_revenue >= CAST(1000.00 AS DECIMAL(18, 2)) THEN 'High-Value'", sql)
        self.assertIn("WHEN total_orders >= 2 THEN 'Repeat'", sql)
        self.assertIn("WHEN total_orders = 1 THEN 'One-Time'", sql)
        self.assertIn("customer_count", sql)
        self.assertIn("avg_revenue", sql)
        inactive_at = sql.find("WHEN total_orders = 0 THEN 'Inactive'")
        high_at = sql.find("THEN 'High-Value'")
        repeat_at = sql.find("THEN 'Repeat'")
        one_at = sql.find("THEN 'One-Time'")
        self.assertLess(inactive_at, high_at)
        self.assertLess(high_at, repeat_at)
        self.assertLess(repeat_at, one_at)

    def test_sql_has_no_environment_specific_paths_or_secrets(self) -> None:
        forbidden = (
            "/workspace/users/",
            "dbfs:/users/",
            "databricks.com",
            "akiai",
            r"d:\users",
            "udayan",
        )
        for filename in GOLD_SQL_FILES:
            lowered = _read(filename).lower()
            for token in forbidden:
                self.assertNotIn(token, lowered, filename)

    def test_split_gold_outputs_covers_all_tables(self) -> None:
        config = load_config()
        declared: list[str] = []
        for filename in GOLD_SQL_FILES:
            rendered = render_gold_sql(_read(filename), config)
            for table_name, statement in split_gold_outputs(rendered):
                declared.append(table_name)
                self.assertIn("SELECT", statement.upper())
                self.assertIn(config.silver_schema + ".", statement)
                self.assertNotIn("{silver_schema}", statement)
        self.assertEqual(declared, list(GOLD_TABLES))

    def test_render_rejects_unsubstituted_placeholders(self) -> None:
        with self.assertRaises(GoldError):
            split_gold_outputs("SELECT 1")
        config = load_config()
        rendered = render_gold_sql("SELECT * FROM {silver_schema}.orders", config)
        self.assertEqual(rendered, "SELECT * FROM silver.orders")


class TestGoldOrchestratorContract(unittest.TestCase):
    def test_gold_table_qualification(self) -> None:
        cfg = load_config()
        self.assertEqual(cfg.gold_schema, DEFAULT_GOLD_SCHEMA)
        self.assertEqual(cfg.gold_table("sales_by_product"), "gold.sales_by_product")
        cfg = load_config(catalog="dev")
        self.assertEqual(cfg.gold_table("daily_trends"), "dev.gold.daily_trends")

    def test_orchestrator_does_not_reimplement_aggregations(self) -> None:
        src = (GOLD_DIR / "create_gold_tables.py").read_text(encoding="utf-8")
        self.assertIn("spark.sql", src)
        self.assertIn("GOLD_SQL_FILES", src)
        self.assertNotIn("F.sum", src)
        self.assertNotIn("functions.sum", src)
        self.assertNotIn("groupBy", src)
        self.assertNotIn("winutils", src.lower())
        self.assertNotIn("NoWinutils", src)

    def test_orchestrator_ast_has_no_group_by_or_drop(self) -> None:
        tree = ast.parse((GOLD_DIR / "create_gold_tables.py").read_text(encoding="utf-8"))
        names: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Name):
                    names.add(func.id)
                elif isinstance(func, ast.Attribute):
                    names.add(func.attr)
        self.assertIn("sql", names)
        self.assertNotIn("groupBy", names)
        self.assertNotIn("dropna", names)
        self.assertNotIn("dropDuplicates", names)
        self.assertNotIn("drop_duplicates", names)


if __name__ == "__main__":
    unittest.main()
