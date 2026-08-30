"""
Tests for the Stage 2 sample-data generator.

`tests/` is not in the assignment's required file tree, but the assignment
requires meaningful tests. This directory is the location frozen in
requirements-analysis.md §6.15 and design-notes.md.

These tests use the standard-library unittest runner (no extra dependencies).
"""

from __future__ import annotations

import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
GENERATOR_DIR = REPO_ROOT / "src" / "data_generation"
sys.path.insert(0, str(GENERATOR_DIR))

from generate_sample_data import (  # noqa: E402
    AS_OF_DATE,
    CUSTOMER_COLUMNS,
    DEFAULT_SEED,
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
    ORDER_COLUMNS,
    ORPHAN_CUSTOMER_ID_START,
    ORPHAN_PRODUCT_ID_START,
    PRODUCT_COLUMNS,
    generate_sample_data,
    parse_optional_date,
    parse_optional_int,
    read_csv,
    sha256_file,
)

DATA_DIR = REPO_ROOT / "data"


class GeneratedDatasetMixin:
    customers: list[dict[str, str]]
    orders: list[dict[str, str]]
    products: list[dict[str, str]]
    customer_cols: list[str]
    order_cols: list[str]
    product_cols: list[str]


class TestGeneratorContract(GeneratedDatasetMixin, unittest.TestCase):
    """Generate once into a temp dir and assert the frozen contract."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_dir = Path(tempfile.mkdtemp(prefix="de_c1_gen_"))
        cls.metadata = generate_sample_data(
            output_dir=cls.temp_dir,
            seed=DEFAULT_SEED,
            as_of_date=AS_OF_DATE,
        )
        cls.customer_cols, cls.customers = read_csv(cls.temp_dir / "customers.csv")
        cls.order_cols, cls.orders = read_csv(cls.temp_dir / "orders.csv")
        cls.product_cols, cls.products = read_csv(cls.temp_dir / "products.csv")

    def test_physical_row_counts(self) -> None:
        self.assertEqual(len(self.customers), N_CUSTOMERS + N_CUSTOMER_DUPLICATE_ROWS)
        self.assertEqual(len(self.orders), N_ORDERS + N_ORDER_DUPLICATE_ROWS)
        self.assertEqual(len(self.products), N_PRODUCTS)

    def test_unique_key_counts(self) -> None:
        customer_ids = {parse_optional_int(r["customer_id"]) for r in self.customers}
        order_ids = {parse_optional_int(r["order_id"]) for r in self.orders}
        product_ids = {parse_optional_int(r["product_id"]) for r in self.products}
        self.assertEqual(len(customer_ids), N_CUSTOMERS)
        self.assertEqual(len(order_ids), N_ORDERS)
        self.assertEqual(len(product_ids), N_PRODUCTS)

    def test_schema_columns(self) -> None:
        self.assertEqual(self.customer_cols, CUSTOMER_COLUMNS)
        self.assertEqual(self.order_cols, ORDER_COLUMNS)
        self.assertEqual(self.product_cols, PRODUCT_COLUMNS)

    def test_null_counts(self) -> None:
        self.assertEqual(sum(1 for r in self.customers if r["email"] == ""), N_CUSTOMER_NULL_EMAIL)
        self.assertEqual(sum(1 for r in self.orders if r["customer_id"] == ""), N_ORDER_NULL_CUSTOMER_ID)
        self.assertEqual(sum(1 for r in self.orders if r["product_id"] == ""), N_ORDER_NULL_PRODUCT_ID)
        self.assertEqual(sum(1 for r in self.customers if r["customer_id"] == ""), 0)
        self.assertEqual(sum(1 for r in self.orders if r["order_id"] == ""), 0)
        self.assertEqual(sum(1 for r in self.products if r["product_id"] == ""), 0)

    def test_duplicate_counts(self) -> None:
        customer_counts = Counter(r["customer_id"] for r in self.customers)
        order_counts = Counter(r["order_id"] for r in self.orders)
        product_counts = Counter(r["product_id"] for r in self.products)
        duplicated_customers = [k for k, n in customer_counts.items() if n > 1]
        duplicated_orders = [k for k, n in order_counts.items() if n > 1]
        self.assertEqual(len(duplicated_customers), N_CUSTOMER_DUPLICATE_ROWS)
        self.assertEqual(len(duplicated_orders), N_ORDER_DUPLICATE_ROWS)
        self.assertEqual(sum(n for n in customer_counts.values() if n > 1), N_CUSTOMER_DUPLICATE_ROWS * 2)
        self.assertEqual(sum(n for n in order_counts.values() if n > 1), N_ORDER_DUPLICATE_ROWS * 2)
        self.assertTrue(all(n == 2 for n in customer_counts.values() if n > 1))
        self.assertTrue(all(n == 2 for n in order_counts.values() if n > 1))
        self.assertTrue(all(n == 1 for n in product_counts.values()))

    def test_orphan_counts_exclude_nulls(self) -> None:
        customer_ids = {parse_optional_int(r["customer_id"]) for r in self.customers}
        product_ids = {parse_optional_int(r["product_id"]) for r in self.products}
        orphan_customers = [
            r
            for r in self.orders
            if r["customer_id"] != "" and parse_optional_int(r["customer_id"]) not in customer_ids
        ]
        orphan_products = [
            r
            for r in self.orders
            if r["product_id"] != "" and parse_optional_int(r["product_id"]) not in product_ids
        ]
        self.assertEqual(len(orphan_customers), N_ORDER_ORPHAN_CUSTOMER_ID)
        self.assertEqual(len(orphan_products), N_ORDER_ORPHAN_PRODUCT_ID)
        observed_cids = {parse_optional_int(r["customer_id"]) for r in orphan_customers}
        observed_pids = {parse_optional_int(r["product_id"]) for r in orphan_products}
        self.assertEqual(
            observed_cids,
            set(range(ORPHAN_CUSTOMER_ID_START, ORPHAN_CUSTOMER_ID_START + N_ORDER_ORPHAN_CUSTOMER_ID)),
        )
        self.assertEqual(
            observed_pids,
            set(range(ORPHAN_PRODUCT_ID_START, ORPHAN_PRODUCT_ID_START + N_ORDER_ORPHAN_PRODUCT_ID)),
        )

    def test_optional_future_signups(self) -> None:
        future = [
            r
            for r in self.customers
            if parse_optional_date(r["signup_date"]) is not None
            and parse_optional_date(r["signup_date"]) > AS_OF_DATE
        ]
        self.assertEqual(len(future), N_FUTURE_SIGNUPS)

    def test_metadata_records_duplicate_sources(self) -> None:
        self.assertEqual(len(self.metadata["duplicate_source_customer_ids"]), N_CUSTOMER_DUPLICATE_ROWS)
        self.assertEqual(len(self.metadata["duplicate_source_order_ids"]), N_ORDER_DUPLICATE_ROWS)
        customer_counts = Counter(r["customer_id"] for r in self.customers)
        for cid in self.metadata["duplicate_source_customer_ids"]:
            self.assertEqual(customer_counts[str(cid)], 2)
        order_counts = Counter(r["order_id"] for r in self.orders)
        for oid in self.metadata["duplicate_source_order_ids"]:
            self.assertEqual(order_counts[str(oid)], 2)

    def test_deterministic_same_seed(self) -> None:
        other_dir = Path(tempfile.mkdtemp(prefix="de_c1_gen_b_"))
        generate_sample_data(output_dir=other_dir, seed=DEFAULT_SEED, as_of_date=AS_OF_DATE)
        for name in ("customers.csv", "orders.csv", "products.csv"):
            self.assertEqual(
                sha256_file(self.temp_dir / name),
                sha256_file(other_dir / name),
                f"{name} differed across two runs with seed {DEFAULT_SEED}",
            )


class TestCommittedDataFiles(GeneratedDatasetMixin, unittest.TestCase):
    """The tracked data/ CSVs must satisfy the same contract (seed 42)."""

    @classmethod
    def setUpClass(cls) -> None:
        missing = [p for p in ("customers.csv", "orders.csv", "products.csv") if not (DATA_DIR / p).exists()]
        if missing:
            raise unittest.SkipTest(f"Committed data files missing: {missing}")
        cols_c, customers = read_csv(DATA_DIR / "customers.csv")
        if len(customers) == 0:
            raise unittest.SkipTest("Committed customers.csv is still header-only; run the generator first")
        cls.customer_cols, cls.customers = cols_c, customers
        cls.order_cols, cls.orders = read_csv(DATA_DIR / "orders.csv")
        cls.product_cols, cls.products = read_csv(DATA_DIR / "products.csv")

    def test_committed_physical_row_counts(self) -> None:
        self.assertEqual(len(self.customers), N_CUSTOMERS + N_CUSTOMER_DUPLICATE_ROWS)
        self.assertEqual(len(self.orders), N_ORDERS + N_ORDER_DUPLICATE_ROWS)
        self.assertEqual(len(self.products), N_PRODUCTS)

    def test_committed_unique_key_counts(self) -> None:
        self.assertEqual(len({r["customer_id"] for r in self.customers}), N_CUSTOMERS)
        self.assertEqual(len({r["order_id"] for r in self.orders}), N_ORDERS)
        self.assertEqual(len({r["product_id"] for r in self.products}), N_PRODUCTS)

    def test_committed_schema(self) -> None:
        self.assertEqual(self.customer_cols, CUSTOMER_COLUMNS)
        self.assertEqual(self.order_cols, ORDER_COLUMNS)
        self.assertEqual(self.product_cols, PRODUCT_COLUMNS)

    def test_committed_null_and_orphan_and_duplicate_counts(self) -> None:
        self.assertEqual(sum(1 for r in self.customers if r["email"] == ""), N_CUSTOMER_NULL_EMAIL)
        self.assertEqual(sum(1 for r in self.orders if r["customer_id"] == ""), N_ORDER_NULL_CUSTOMER_ID)
        self.assertEqual(sum(1 for r in self.orders if r["product_id"] == ""), N_ORDER_NULL_PRODUCT_ID)
        customer_ids = {parse_optional_int(r["customer_id"]) for r in self.customers}
        product_ids = {parse_optional_int(r["product_id"]) for r in self.products}
        self.assertEqual(
            sum(
                1
                for r in self.orders
                if r["customer_id"] != "" and parse_optional_int(r["customer_id"]) not in customer_ids
            ),
            N_ORDER_ORPHAN_CUSTOMER_ID,
        )
        self.assertEqual(
            sum(
                1
                for r in self.orders
                if r["product_id"] != "" and parse_optional_int(r["product_id"]) not in product_ids
            ),
            N_ORDER_ORPHAN_PRODUCT_ID,
        )
        customer_counts = Counter(r["customer_id"] for r in self.customers)
        order_counts = Counter(r["order_id"] for r in self.orders)
        self.assertEqual(sum(1 for n in customer_counts.values() if n > 1), N_CUSTOMER_DUPLICATE_ROWS)
        self.assertEqual(sum(1 for n in order_counts.values() if n > 1), N_ORDER_DUPLICATE_ROWS)

    def test_committed_matches_seed_42_regeneration(self) -> None:
        regen_dir = Path(tempfile.mkdtemp(prefix="de_c1_committed_"))
        generate_sample_data(output_dir=regen_dir, seed=DEFAULT_SEED, as_of_date=AS_OF_DATE)
        for name in ("customers.csv", "orders.csv", "products.csv"):
            self.assertEqual(
                sha256_file(DATA_DIR / name),
                sha256_file(regen_dir / name),
                f"Committed {name} does not match regeneration with seed {DEFAULT_SEED}",
            )


if __name__ == "__main__":
    unittest.main()
