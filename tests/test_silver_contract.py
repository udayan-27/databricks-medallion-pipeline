"""
Spark-free Silver completeness/uniqueness contract tests.

Always runnable. They assert field lists, code format, accumulation helpers,
and that the Silver quality modules do not call row-dropping APIs.

Spark execution lives in tests/test_silver_quality.py.
"""

from __future__ import annotations

import ast
import importlib.util
import sys
import unittest
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = REPO_ROOT / "src"
BRONZE_DIR = SRC_DIR / "bronze"
SILVER_DIR = SRC_DIR / "silver"

for _path in (str(SRC_DIR), str(BRONZE_DIR), str(SILVER_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from quality_common import (  # noqa: E402
    COMBINED_FAILED_CHECKS_COLUMN,
    MODULE_COMPLETENESS,
    MODULE_UNIQUENESS,
    QUALITY_CHECK_RESULT_COLUMN,
    failed_checks_column_name,
    metrics_from_counts,
    pass_column_name,
    quality_code,
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


class TestQualityCodeRepresentation(unittest.TestCase):
    def test_code_format_matches_strategy(self) -> None:
        self.assertEqual(
            quality_code("completeness", "customers", "email"),
            "completeness:customers.email",
        )
        self.assertEqual(
            quality_code("completeness", "orders", "customer_id"),
            "completeness:orders.customer_id",
        )
        self.assertEqual(
            quality_code("completeness", "orders", "product_id"),
            "completeness:orders.product_id",
        )
        self.assertEqual(
            quality_code("uniqueness", "customers", "customer_id"),
            "uniqueness:customers.customer_id",
        )
        self.assertEqual(
            quality_code("uniqueness", "orders", "order_id"),
            "uniqueness:orders.order_id",
        )

    def test_module_columns_do_not_collide(self) -> None:
        self.assertEqual(
            failed_checks_column_name(MODULE_COMPLETENESS),
            "completeness_failed_checks",
        )
        self.assertEqual(
            failed_checks_column_name(MODULE_UNIQUENESS),
            "uniqueness_failed_checks",
        )
        self.assertEqual(pass_column_name(MODULE_COMPLETENESS), "completeness_pass")
        self.assertEqual(pass_column_name(MODULE_UNIQUENESS), "uniqueness_pass")
        self.assertNotEqual(
            failed_checks_column_name(MODULE_COMPLETENESS),
            failed_checks_column_name(MODULE_UNIQUENESS),
        )
        self.assertNotEqual(
            pass_column_name(MODULE_COMPLETENESS),
            pass_column_name(MODULE_UNIQUENESS),
        )
        self.assertNotEqual(
            failed_checks_column_name(MODULE_COMPLETENESS),
            COMBINED_FAILED_CHECKS_COLUMN,
        )
        self.assertNotEqual(
            pass_column_name(MODULE_COMPLETENESS),
            QUALITY_CHECK_RESULT_COLUMN,
        )

    def test_completeness_fields_include_named_critical_and_documented_pks(self) -> None:
        self.assertEqual(
            completeness.COMPLETENESS_FIELDS["customers"],
            ("email", "customer_id"),
        )
        self.assertEqual(
            completeness.COMPLETENESS_FIELDS["orders"],
            ("customer_id", "product_id", "order_id"),
        )
        self.assertIn("email", completeness.CRITICAL_COMPLETENESS_FIELDS["customers"])
        self.assertEqual(
            completeness.CRITICAL_COMPLETENESS_FIELDS["orders"],
            ("customer_id", "product_id"),
        )
        self.assertNotIn("payment_date", completeness.COMPLETENESS_FIELDS["orders"])

    def test_uniqueness_keys_are_business_keys(self) -> None:
        self.assertEqual(uniqueness.UNIQUENESS_KEYS["customers"], "customer_id")
        self.assertEqual(uniqueness.UNIQUENESS_KEYS["orders"], "order_id")
        self.assertEqual(uniqueness.UNIQUENESS_KEYS["products"], "product_id")
        self.assertNotEqual(uniqueness.UNIQUENESS_KEYS["customers"], "_ingest_row_id")
        self.assertNotEqual(uniqueness.UNIQUENESS_KEYS["orders"], "_ingest_row_id")


class TestPhysicalRowMetrics(unittest.TestCase):
    def test_uniqueness_metrics_count_participating_rows_not_extra_keys(self) -> None:
        customer = metrics_from_counts(
            table_name="customers",
            check_name="uniqueness",
            total_evaluated=10010,
            failed=20,
        )
        self.assertEqual(customer.total_evaluated, 10010)
        self.assertEqual(customer.failed, 20)
        self.assertEqual(customer.passed, 9990)
        self.assertNotEqual(customer.failed, 10)
        order = metrics_from_counts(
            table_name="orders",
            check_name="uniqueness",
            total_evaluated=100020,
            failed=40,
        )
        self.assertEqual(order.failed, 40)
        self.assertEqual(order.passed, 99980)
        self.assertNotEqual(order.failed, 20)

    def test_percentages_use_physical_population(self) -> None:
        metrics = metrics_from_counts(
            table_name="customers",
            check_name="completeness:customers.email",
            total_evaluated=10010,
            failed=50,
        )
        self.assertEqual(metrics.pass_pct, Decimal("0.9950"))
        self.assertEqual(metrics.fail_pct, Decimal("0.0050"))
        self.assertEqual(metrics.pass_pct + metrics.fail_pct, Decimal("1.0000"))


class TestNoRowDroppingInSilverQuality(unittest.TestCase):
    FORBIDDEN_CALLS = {
        "dropDuplicates",
        "drop_duplicates",
        "dropna",
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

    def test_quality_modules_do_not_drop_or_dedupe_rows(self) -> None:
        for filename in (
            "quality_common.py",
            "01_quality_completeness.py",
            "02_quality_uniqueness.py",
        ):
            calls = self._call_names(SILVER_DIR / filename)
            forbidden = calls & self.FORBIDDEN_CALLS
            self.assertFalse(
                forbidden,
                f"{filename} calls forbidden row-drop/dedupe APIs: {forbidden}",
            )

    def test_modules_do_not_emit_ri_or_orphan_codes(self) -> None:
        completeness_src = (SILVER_DIR / "01_quality_completeness.py").read_text(
            encoding="utf-8"
        )
        uniqueness_src = (SILVER_DIR / "02_quality_uniqueness.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("ri:orders", completeness_src)
        self.assertNotIn("customer_id_orphan", completeness_src)
        self.assertNotIn("product_id_orphan", uniqueness_src)
        self.assertNotIn('withColumn("quality_check_result"', completeness_src)
        self.assertNotIn('withColumn("quality_check_result"', uniqueness_src)
        self.assertIn("completeness_failed_checks", completeness_src)
        self.assertIn("uniqueness_failed_checks", uniqueness_src)


if __name__ == "__main__":
    unittest.main()
