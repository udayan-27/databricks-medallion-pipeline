"""
Spark-free Silver quality contract tests.

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
GENERATOR_DIR = SRC_DIR / "data_generation"

for _path in (str(SRC_DIR), str(BRONZE_DIR), str(SILVER_DIR), str(GENERATOR_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from quality_common import (  # noqa: E402
    COMBINED_FAILED_CHECKS_COLUMN,
    MODULE_BUSINESS,
    MODULE_COMPLETENESS,
    MODULE_RI,
    MODULE_TYPE,
    MODULE_UNIQUENESS,
    POPULATION_DISTINCT_KEY,
    POPULATION_PHYSICAL_ROW,
    POPULATION_TABLE_OUTCOME,
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
type_validation = _load_silver("03_quality_type_validation.py", "quality_type_validation")
referential_integrity = _load_silver(
    "04_quality_referential_integrity.py", "quality_referential_integrity"
)
business_logic = _load_silver("05_quality_business_logic.py", "quality_business_logic")
create_silver_tables = _load_silver("create_silver_tables.py", "create_silver_tables")


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
        self.assertEqual(
            quality_code("type", "orders", "order_status_domain"),
            "type:orders.order_status_domain",
        )
        self.assertEqual(
            quality_code("type", "customers", "customer_segment_domain"),
            "type:customers.customer_segment_domain",
        )
        self.assertEqual(
            quality_code("type", "orders", "quantity"),
            "type:orders.quantity",
        )
        self.assertEqual(
            quality_code("ri", "orders", "customer_id_orphan"),
            "ri:orders.customer_id_orphan",
        )
        self.assertEqual(
            quality_code("ri", "orders", "product_id_orphan"),
            "ri:orders.product_id_orphan",
        )
        self.assertEqual(
            quality_code("business", "orders", "quantity_positive"),
            "business:orders.quantity_positive",
        )
        self.assertEqual(
            quality_code("business", "customers", "signup_not_future"),
            "business:customers.signup_not_future",
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
        self.assertEqual(
            failed_checks_column_name(MODULE_TYPE),
            "type_failed_checks",
        )
        self.assertEqual(pass_column_name(MODULE_TYPE), "type_validation_pass")
        self.assertEqual(
            failed_checks_column_name(MODULE_RI),
            "referential_integrity_failed_checks",
        )
        self.assertEqual(pass_column_name(MODULE_RI), "referential_integrity_pass")
        self.assertEqual(
            failed_checks_column_name(MODULE_BUSINESS),
            "business_logic_failed_checks",
        )
        self.assertEqual(pass_column_name(MODULE_BUSINESS), "business_logic_pass")
        self.assertNotEqual(
            failed_checks_column_name(MODULE_COMPLETENESS),
            failed_checks_column_name(MODULE_UNIQUENESS),
        )
        self.assertNotEqual(
            failed_checks_column_name(MODULE_TYPE),
            failed_checks_column_name(MODULE_RI),
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


class TestTypeAndRIContracts(unittest.TestCase):
    def test_type_null_fields_exclude_completeness_and_payment_date(self) -> None:
        self.assertEqual(
            type_validation.COMPLETENESS_OWNED_NULL_FIELDS,
            completeness.COMPLETENESS_FIELDS,
        )
        self.assertEqual(
            type_validation.NULLABLE_TYPED_FIELDS["orders"],
            ("payment_date",),
        )
        for table_name, fields in type_validation.TYPE_NULL_FIELDS.items():
            owned = set(type_validation.COMPLETENESS_OWNED_NULL_FIELDS[table_name])
            overlap = owned.intersection(fields)
            self.assertFalse(
                overlap,
                f"{table_name} type-null fields overlap completeness: {overlap}",
            )
            self.assertNotIn("payment_date", fields)
            self.assertEqual(fields, type_validation.derived_type_null_fields(table_name))

    def test_declared_bronze_types_are_covered(self) -> None:
        customers = dict(type_validation.declared_source_fields("customers"))
        orders = dict(type_validation.declared_source_fields("orders"))
        products = dict(type_validation.declared_source_fields("products"))
        self.assertEqual(customers["customer_id"], "INT")
        self.assertEqual(customers["customer_name"], "STRING")
        self.assertEqual(customers["email"], "STRING")
        self.assertEqual(customers["country"], "STRING")
        self.assertEqual(customers["signup_date"], "DATE")
        self.assertEqual(customers["customer_segment"], "STRING")
        self.assertEqual(customers["lifetime_value"], "DECIMAL(18,2)")
        self.assertEqual(orders["order_id"], "INT")
        self.assertEqual(orders["customer_id"], "INT")
        self.assertEqual(orders["order_date"], "DATE")
        self.assertEqual(orders["product_id"], "INT")
        self.assertEqual(orders["quantity"], "INT")
        self.assertEqual(orders["unit_price"], "DECIMAL(18,2)")
        self.assertEqual(orders["total_amount"], "DECIMAL(18,2)")
        self.assertEqual(orders["order_status"], "STRING")
        self.assertEqual(orders["payment_date"], "DATE")
        self.assertEqual(products["product_id"], "INT")
        self.assertEqual(products["product_name"], "STRING")
        self.assertEqual(products["category"], "STRING")
        self.assertEqual(products["price"], "DECIMAL(18,2)")
        self.assertEqual(products["cost"], "DECIMAL(18,2)")
        self.assertEqual(products["stock_quantity"], "INT")
        self.assertEqual(products["reorder_level"], "INT")

    def test_closed_domains_match_spec(self) -> None:
        customer_domains = dict(type_validation.DOMAIN_ALLOWLISTS["customers"])
        order_domains = dict(type_validation.DOMAIN_ALLOWLISTS["orders"])
        self.assertEqual(
            customer_domains["customer_segment"],
            ("Premium", "Standard", "Basic"),
        )
        self.assertEqual(
            order_domains["order_status"],
            ("Pending", "Completed", "Cancelled"),
        )
        self.assertEqual(type_validation.DOMAIN_ALLOWLISTS["products"], ())
        self.assertNotIn("country", customer_domains)
        self.assertNotIn("category", dict(type_validation.DOMAIN_ALLOWLISTS["products"]))

    def test_ri_orphan_codes_are_not_completeness_codes(self) -> None:
        self.assertEqual(
            referential_integrity.CUSTOMER_ORPHAN_CODE,
            "ri:orders.customer_id_orphan",
        )
        self.assertEqual(
            referential_integrity.PRODUCT_ORPHAN_CODE,
            "ri:orders.product_id_orphan",
        )
        self.assertNotEqual(
            referential_integrity.CUSTOMER_ORPHAN_CODE,
            "completeness:orders.customer_id",
        )
        self.assertNotEqual(
            referential_integrity.PRODUCT_ORPHAN_CODE,
            "completeness:orders.product_id",
        )

    def test_ri_metrics_count_orphans_not_null_plus_orphan(self) -> None:
        customer = metrics_from_counts(
            table_name="orders",
            check_name="ri:orders.customer_id_orphan",
            total_evaluated=100020,
            failed=50,
        )
        self.assertEqual(customer.failed, 50)
        self.assertNotEqual(customer.failed, 150)
        product = metrics_from_counts(
            table_name="orders",
            check_name="ri:orders.product_id_orphan",
            total_evaluated=100020,
            failed=30,
        )
        self.assertEqual(product.failed, 30)
        self.assertNotEqual(product.failed, 230)


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

    def test_empty_population_percentages_are_zero_not_an_error(self) -> None:
        empty = metrics_from_counts(
            table_name="orders",
            check_name="business",
            total_evaluated=0,
            failed=0,
        )
        self.assertEqual(empty.total_evaluated, 0)
        self.assertEqual(empty.passed, 0)
        self.assertEqual(empty.failed, 0)
        self.assertEqual(empty.pass_pct, Decimal("0.0000"))
        self.assertEqual(empty.fail_pct, Decimal("0.0000"))

    def test_distinct_key_population_is_not_physical_rows(self) -> None:
        keys = metrics_from_counts(
            table_name="customers",
            check_name="uniqueness:customers.customer_id.duplicate_keys",
            total_evaluated=10000,
            failed=10,
            population_kind=POPULATION_DISTINCT_KEY,
        )
        rows = metrics_from_counts(
            table_name="customers",
            check_name="uniqueness:customers.customer_id",
            total_evaluated=10010,
            failed=20,
            population_kind=POPULATION_PHYSICAL_ROW,
        )
        self.assertEqual(keys.failed, 10)
        self.assertEqual(rows.failed, 20)
        self.assertNotEqual(keys.total_evaluated, rows.total_evaluated)
        self.assertEqual(keys.population_kind, POPULATION_DISTINCT_KEY)
        self.assertEqual(rows.population_kind, POPULATION_PHYSICAL_ROW)

    def test_table_outcome_is_a_separate_population(self) -> None:
        outcome = metrics_from_counts(
            table_name="customers",
            check_name="quality_check_result",
            total_evaluated=10010,
            failed=100,
            population_kind=POPULATION_TABLE_OUTCOME,
        )
        self.assertEqual(outcome.population_kind, POPULATION_TABLE_OUTCOME)
        self.assertEqual(outcome.pass_percentage, Decimal("0.9900"))
        self.assertEqual(outcome.fail_percentage, Decimal("0.0100"))


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
            "03_quality_type_validation.py",
            "04_quality_referential_integrity.py",
            "05_quality_business_logic.py",
            "create_silver_tables.py",
        ):
            calls = self._call_names(SILVER_DIR / filename)
            forbidden = calls & self.FORBIDDEN_CALLS
            self.assertFalse(
                forbidden,
                f"{filename} calls forbidden row-drop/dedupe APIs: {forbidden}",
            )

    def test_modules_do_not_emit_cross_module_codes_or_last_writer_result(self) -> None:
        completeness_src = (SILVER_DIR / "01_quality_completeness.py").read_text(
            encoding="utf-8"
        )
        uniqueness_src = (SILVER_DIR / "02_quality_uniqueness.py").read_text(
            encoding="utf-8"
        )
        type_src = (SILVER_DIR / "03_quality_type_validation.py").read_text(
            encoding="utf-8"
        )
        ri_src = (SILVER_DIR / "04_quality_referential_integrity.py").read_text(
            encoding="utf-8"
        )
        bl_src = (SILVER_DIR / "05_quality_business_logic.py").read_text(encoding="utf-8")
        self.assertNotIn("ri:orders", completeness_src)
        self.assertNotIn("customer_id_orphan", completeness_src)
        self.assertNotIn("product_id_orphan", uniqueness_src)
        self.assertNotIn("completeness:orders.customer_id", type_src)
        self.assertNotIn("completeness:orders.customer_id", ri_src)
        self.assertNotIn("completeness:orders.customer_id", bl_src)
        for src in (completeness_src, uniqueness_src, type_src, ri_src, bl_src):
            self.assertNotIn('withColumn("quality_check_result"', src)
        self.assertIn("completeness_failed_checks", completeness_src)
        self.assertIn("uniqueness_failed_checks", uniqueness_src)
        self.assertIn("type_failed_checks", type_src)
        self.assertIn("referential_integrity_failed_checks", ri_src)
        self.assertIn("business_logic_failed_checks", bl_src)
        self.assertIn("distinct()", ri_src)
        self.assertIn("left", ri_src)
        self.assertNotIn("dropDuplicates", ri_src)
        self.assertNotIn("F.current_date", bl_src)
        self.assertNotIn("current_date()", bl_src)
        self.assertNotIn("cost_less_than_price", bl_src)
        self.assertNotIn("price_gte_cost", bl_src)


class TestBusinessLogicAndOrchestratorContracts(unittest.TestCase):
    def test_frozen_as_of_date_matches_generator(self) -> None:
        from generate_sample_data import AS_OF_DATE as GENERATOR_AS_OF

        self.assertEqual(business_logic.BUSINESS_AS_OF_DATE, GENERATOR_AS_OF)
        self.assertEqual(business_logic.BUSINESS_AS_OF_DATE.isoformat(), "2026-08-31")
        self.assertEqual(str(business_logic.AMOUNT_TOLERANCE), "0.01")

    def test_implemented_rules_match_frozen_strategy(self) -> None:
        self.assertEqual(
            business_logic.CUSTOMER_RULE_CODES,
            (
                "business:customers.signup_not_future",
                "business:customers.lifetime_value_non_negative",
            ),
        )
        self.assertEqual(
            business_logic.ORDER_RULE_CODES,
            (
                "business:orders.quantity_positive",
                "business:orders.unit_price_non_negative",
                "business:orders.total_amount_non_negative",
                "business:orders.amount_equals_qty_price",
                "business:orders.completed_has_payment",
                "business:orders.cancelled_without_payment",
                "business:orders.payment_on_or_after_order",
                "business:orders.order_not_before_signup",
            ),
        )
        self.assertEqual(
            business_logic.PRODUCT_RULE_CODES,
            (
                "business:products.price_non_negative",
                "business:products.cost_non_negative",
                "business:products.stock_non_negative",
                "business:products.reorder_non_negative",
            ),
        )
        all_codes = (
            business_logic.CUSTOMER_RULE_CODES
            + business_logic.ORDER_RULE_CODES
            + business_logic.PRODUCT_RULE_CODES
        )
        self.assertNotIn("business:products.price_gte_cost", all_codes)
        self.assertNotIn("business:orders.pending_payment", all_codes)

    def test_orchestrator_expected_counts_keep_future_signup_separate(self) -> None:
        self.assertEqual(
            create_silver_tables.INTENTIONAL_FAIL_COUNTS[
                ("customers", "business:customers.signup_not_future")
            ],
            30,
        )
        self.assertEqual(
            create_silver_tables.INTENTIONAL_FAIL_COUNTS[
                ("orders", "business:orders.order_not_before_signup")
            ],
            0,
        )
        self.assertEqual(create_silver_tables.MANDATORY_ISSUE_INSTANCES, 460)
        mandatory_sum = (
            50 + 10 + 100 + 200 + 50 + 30 + 20
        )
        self.assertEqual(mandatory_sum, 460)
        self.assertNotEqual(mandatory_sum + 30, 460)

    def test_combiner_column_names(self) -> None:
        self.assertEqual(COMBINED_FAILED_CHECKS_COLUMN, "failed_checks")
        self.assertEqual(QUALITY_CHECK_RESULT_COLUMN, "quality_check_result")
        orchestrator_src = (SILVER_DIR / "create_silver_tables.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("combine_quality_status", orchestrator_src)
        self.assertIn("quality_check_result", orchestrator_src)
        self.assertIn("_ingest_row_id", orchestrator_src)


if __name__ == "__main__":
    unittest.main()
