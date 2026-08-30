"""
Deterministic synthetic e-commerce CSV generator for the DE C1 pipeline.

Produces:
  - data/customers.csv  — 10,000 unique customer_id + 10 extra duplicate rows = 10,010
  - data/orders.csv      — 100,000 unique order_id + 20 extra duplicate rows = 100,020
  - data/products.csv    — 500 unique product_id = 500

Mandatory injected defects (listed counts only; do not pad to ~700):
  Customers: 50 NULL email; 10 extra duplicate customer_id rows
  Orders:    100 NULL customer_id; 200 NULL product_id;
             50 orphan customer_id; 30 orphan product_id; 20 extra duplicate order_id rows

Optional documented business-logic defect (not part of the 460):
  30 customer signup_date values after the frozen as-of date 2026-08-31.

Default seed: 42. Same seed + configuration => byte-identical CSV (UTF-8, LF).

No Faker. No pandas. Standard library only.
See DATA_GENERATION_NOTES.md for distributions, ID ranges, and overlap rules.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import random
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal, ROUND_HALF_EVEN
from pathlib import Path
from typing import Any, Mapping, Sequence

LOGGER = logging.getLogger("generate_sample_data")

# ---------------------------------------------------------------------------
# Frozen counts (do not silently change)
# ---------------------------------------------------------------------------

DEFAULT_SEED = 42
AS_OF_DATE = date(2026, 8, 31)
SIGNUP_WINDOW_START = date(2022, 1, 1)
ORDER_WINDOW_START = date(2023, 1, 1)

N_CUSTOMERS = 10_000
N_CUSTOMER_DUPLICATE_ROWS = 10
N_CUSTOMER_NULL_EMAIL = 50
N_FUTURE_SIGNUPS = 30  # optional BL defect; not in the mandatory 460

N_PRODUCTS = 500

N_ORDERS = 100_000
N_ORDER_DUPLICATE_ROWS = 20
N_ORDER_NULL_CUSTOMER_ID = 100
N_ORDER_NULL_PRODUCT_ID = 200
N_ORDER_ORPHAN_CUSTOMER_ID = 50
N_ORDER_ORPHAN_PRODUCT_ID = 30

CUSTOMER_ID_START = 1
PRODUCT_ID_START = 1
ORDER_ID_START = 1

# Orphan namespaces: non-null FKs that cannot collide with valid PKs.
ORPHAN_CUSTOMER_ID_START = 90_001  # 90001..90050
ORPHAN_PRODUCT_ID_START = 9_001  # 9001..9030

TWOPLACES = Decimal("0.01")
MONEY_ROUNDING = ROUND_HALF_EVEN  # banker's rounding; documented in DATA_GENERATION_NOTES.md

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "email",
    "country",
    "signup_date",
    "customer_segment",
    "lifetime_value",
]
ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "order_status",
    "payment_date",
]
PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "price",
    "cost",
    "stock_quantity",
    "reorder_level",
]

CUSTOMER_SEGMENTS = ("Premium", "Standard", "Basic")
SEGMENT_WEIGHTS = (12, 53, 35)
SEGMENT_ORDER_WEIGHT = {"Premium": 3, "Standard": 2, "Basic": 1}

ORDER_STATUSES = ("Completed", "Pending", "Cancelled")
STATUS_WEIGHTS = (75, 15, 10)

QUANTITY_VALUES = (1, 2, 3, 4, 5, 6, 8, 10)
QUANTITY_WEIGHTS = (40, 25, 15, 8, 5, 3, 2, 2)

COUNTRIES = (
    "United States",
    "United Kingdom",
    "Canada",
    "Germany",
    "France",
    "Australia",
    "India",
    "Japan",
    "Brazil",
    "Netherlands",
    "Spain",
    "Mexico",
)
COUNTRY_WEIGHTS = (28, 10, 8, 8, 7, 6, 12, 6, 5, 4, 3, 3)

CATEGORIES = (
    "Electronics",
    "Clothing",
    "Home & Kitchen",
    "Sports",
    "Beauty",
    "Books",
    "Toys",
    "Grocery",
    "Office",
    "Automotive",
)
CATEGORY_WEIGHTS = (18, 16, 14, 10, 8, 8, 7, 8, 6, 5)

FIRST_NAMES = (
    "Ava", "Noah", "Mia", "Liam", "Zoe", "Ethan", "Ivy", "Owen", "Ruby", "Caleb",
    "Nora", "Leo", "Ella", "Miles", "Chloe", "Jonah", "Lila", "Theo", "Sadie", "Asher",
    "Piper", "Felix", "Iris", "Hugo", "Mila", "Silas", "Wren", "Nico", "Jade", "Arlo",
)
LAST_NAMES = (
    "Hart", "Brooks", "Quinn", "Walsh", "Nash", "Reed", "Blair", "Cole", "Drew", "Frost",
    "Lane", "Moss", "Pike", "Shaw", "Vale", "York", "Bell", "Cruz", "Dunn", "Finn",
    "Gray", "Hale", "Kerr", "Lang", "Park", "Snow", "Tate", "West", "Young", "Zane",
)
PRODUCT_ADJECTIVES = (
    "Aero", "Bright", "Cedar", "Delta", "Echo", "Flux", "Grove", "Halo", "Ivory", "Jade",
    "Keen", "Lumen", "Maple", "Nova", "Orbit", "Prime", "Quest", "Ridge", "Summit", "Terra",
    "Ultra", "Vivid", "Willow", "Zenith",
)
PRODUCT_NOUNS = (
    "Lamp", "Mug", "Chair", "Bottle", "Speaker", "Jacket", "Backpack", "Keyboard", "Monitor",
    "Headset", "Blender", "Toaster", "YogaMat", "Kettle", "Notebook", "Sneaker", "Helmet",
    "Camera", "Router", "Charger", "Candle", "Basket", "Pillow", "Toolkit", "Stand",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data"

Row = dict[str, Any]


class GenerationError(RuntimeError):
    """Raised when generation or validation fails."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def money(value: Decimal | int | str) -> Decimal:
    """Quantize to DECIMAL(18,2) using ROUND_HALF_EVEN."""
    return Decimal(value).quantize(TWOPLACES, rounding=MONEY_ROUNDING)


def format_money(value: Decimal) -> str:
    return f"{money(value):.2f}"


def random_date(rng: random.Random, start: date, end: date) -> date:
    span = (end - start).days
    if span < 0:
        raise GenerationError(f"Invalid date window: {start} > {end}")
    return start + timedelta(days=rng.randint(0, span))


def weighted_choice(rng: random.Random, values: Sequence[str], weights: Sequence[int]) -> str:
    return rng.choices(list(values), weights=list(weights), k=1)[0]


def serialize_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format_money(value)
    return str(value)


def write_csv(path: Path, columns: Sequence[str], rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(columns),
            extrasaction="raise",
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({col: serialize_cell(row[col]) for col in columns})


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_optional_int(raw: str) -> int | None:
    if raw == "":
        return None
    return int(raw)


def parse_optional_date(raw: str) -> date | None:
    if raw == "":
        return None
    return date.fromisoformat(raw)


def parse_decimal(raw: str) -> Decimal:
    return money(raw)


def read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise GenerationError(f"{path} has no header")
        columns = list(reader.fieldnames)
        rows = list(reader)
    return columns, rows


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

def generate_products(rng: random.Random) -> list[Row]:
    products: list[Row] = []
    used_names: set[str] = set()
    for offset in range(N_PRODUCTS):
        product_id = PRODUCT_ID_START + offset
        name = f"{rng.choice(PRODUCT_ADJECTIVES)} {rng.choice(PRODUCT_NOUNS)}"
        if name in used_names:
            name = f"{name} {product_id}"
        used_names.add(name)
        price = money(Decimal(rng.randint(599, 49_999)) / Decimal(100))
        cost_ratio = Decimal(rng.randint(40, 75)) / Decimal(100)
        cost = money(price * cost_ratio)
        products.append(
            {
                "product_id": product_id,
                "product_name": name,
                "category": weighted_choice(rng, CATEGORIES, CATEGORY_WEIGHTS),
                "price": price,
                "cost": cost,
                "stock_quantity": rng.randint(0, 2_000),
                "reorder_level": rng.randint(5, 80),
            }
        )
    return products


def lifetime_value_for_segment(rng: random.Random, segment: str) -> Decimal:
    if segment == "Premium":
        cents = rng.randint(50_000, 500_000)
    elif segment == "Standard":
        cents = rng.randint(5_000, 80_000)
    else:
        cents = rng.randint(0, 20_000)
    return money(Decimal(cents) / Decimal(100))


def generate_customers(rng: random.Random, as_of: date) -> list[Row]:
    customers: list[Row] = []
    for offset in range(N_CUSTOMERS):
        customer_id = CUSTOMER_ID_START + offset
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        segment = weighted_choice(rng, CUSTOMER_SEGMENTS, SEGMENT_WEIGHTS)
        customers.append(
            {
                "customer_id": customer_id,
                "customer_name": f"{first} {last}",
                "email": f"{first.lower()}.{last.lower()}{customer_id}@example.com",
                "country": weighted_choice(rng, COUNTRIES, COUNTRY_WEIGHTS),
                "signup_date": random_date(rng, SIGNUP_WINDOW_START, as_of),
                "customer_segment": segment,
                "lifetime_value": lifetime_value_for_segment(rng, segment),
            }
        )
    return customers


def inject_customer_defects(
    customers: list[Row],
    rng: random.Random,
    as_of: date,
) -> dict[str, list[int]]:
    """
    Inject NULL emails, future signups, and remember duplicate sources.

    All three sets are disjoint. Duplicate extra rows are appended by the caller
    so order generation can use the unique 10,000-row customer list.
    """
    needed = N_CUSTOMER_NULL_EMAIL + N_CUSTOMER_DUPLICATE_ROWS + N_FUTURE_SIGNUPS
    if len(customers) < needed:
        raise GenerationError("Not enough customer rows for disjoint defect injection")
    picked = rng.sample(range(len(customers)), needed)
    null_idx = picked[:N_CUSTOMER_NULL_EMAIL]
    dup_idx = picked[N_CUSTOMER_NULL_EMAIL : N_CUSTOMER_NULL_EMAIL + N_CUSTOMER_DUPLICATE_ROWS]
    future_idx = picked[N_CUSTOMER_NULL_EMAIL + N_CUSTOMER_DUPLICATE_ROWS :]

    for i in null_idx:
        customers[i]["email"] = None
    for day_offset, i in enumerate(future_idx, start=1):
        # Deterministic dates: as_of+1 .. as_of+30. Independent of extra RNG draws.
        customers[i]["signup_date"] = as_of + timedelta(days=day_offset)

    return {
        "null_email_customer_ids": [customers[i]["customer_id"] for i in null_idx],
        "duplicate_source_customer_ids": [customers[i]["customer_id"] for i in dup_idx],
        "future_signup_customer_ids": [customers[i]["customer_id"] for i in future_idx],
        "duplicate_source_indices": dup_idx,
    }


def payment_date_for(rng: random.Random, status: str, order_date: date) -> date | None:
    """
    Align payment_date with documented business rules:
      Completed -> payment_date NOT NULL and >= order_date
      Cancelled -> payment_date NULL
      Pending   -> either NULL or >= order_date
    """
    if status == "Cancelled":
        return None
    if status == "Completed":
        return order_date + timedelta(days=rng.randint(0, 14))
    if rng.random() < 0.40:
        return order_date + timedelta(days=rng.randint(0, 7))
    return None


def generate_orders(
    rng: random.Random,
    customers: Sequence[Row],
    products: Sequence[Row],
    as_of: date,
    future_signup_ids: set[int],
) -> list[Row]:
    """
    Generate N_ORDERS valid orders.

    Future-signup customers are excluded from the purchaser pool so we do not
    accidentally create order_date < signup_date failures (signup is after as-of,
    while order dates are capped at as-of).
    """
    orderable = [c for c in customers if c["customer_id"] not in future_signup_ids]
    if not orderable:
        raise GenerationError("No orderable customers after excluding future signups")
    orderable_ids = [c["customer_id"] for c in orderable]
    orderable_weights = [SEGMENT_ORDER_WEIGHT[c["customer_segment"]] for c in orderable]
    signup_by_id = {c["customer_id"]: c["signup_date"] for c in orderable}
    product_by_id = {p["product_id"]: p for p in products}
    product_ids = [p["product_id"] for p in products]

    orders: list[Row] = []
    for offset in range(N_ORDERS):
        if offset > 0 and offset % 25_000 == 0:
            LOGGER.info("Generated %s / %s orders", offset, N_ORDERS)
        customer_id = rng.choices(orderable_ids, weights=orderable_weights, k=1)[0]
        signup = signup_by_id[customer_id]
        min_order_date = max(signup, ORDER_WINDOW_START)
        if min_order_date > as_of:
            raise GenerationError(
                f"Customer {customer_id} has min_order_date {min_order_date} after as-of {as_of}"
            )
        order_date = random_date(rng, min_order_date, as_of)
        product_id = rng.choice(product_ids)
        product = product_by_id[product_id]
        quantity = rng.choices(list(QUANTITY_VALUES), weights=list(QUANTITY_WEIGHTS), k=1)[0]
        unit_price = product["price"]
        total_amount = money(Decimal(quantity) * unit_price)
        status = weighted_choice(rng, ORDER_STATUSES, STATUS_WEIGHTS)
        orders.append(
            {
                "order_id": ORDER_ID_START + offset,
                "customer_id": customer_id,
                "order_date": order_date,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                "order_status": status,
                "payment_date": payment_date_for(rng, status, order_date),
            }
        )
    return orders


def inject_order_defects(orders: list[Row], rng: random.Random) -> dict[str, list[int]]:
    """
    Overlay NULL FKs and orphan FKs on disjoint rows of the unique-order list.

    Duplicate extra rows are appended later from remaining clean rows so they
    cannot amplify NULL/orphan counts.
    """
    needed = (
        N_ORDER_NULL_CUSTOMER_ID
        + N_ORDER_NULL_PRODUCT_ID
        + N_ORDER_ORPHAN_CUSTOMER_ID
        + N_ORDER_ORPHAN_PRODUCT_ID
    )
    remaining_after_defects = len(orders) - needed
    if remaining_after_defects < N_ORDER_DUPLICATE_ROWS:
        raise GenerationError("Not enough clean order rows for duplicate copies")

    picked = rng.sample(range(len(orders)), needed)
    cursor = 0

    def take(n: int) -> list[int]:
        nonlocal cursor
        chunk = picked[cursor : cursor + n]
        cursor += n
        return chunk

    null_cid_idx = take(N_ORDER_NULL_CUSTOMER_ID)
    null_pid_idx = take(N_ORDER_NULL_PRODUCT_ID)
    orphan_cid_idx = take(N_ORDER_ORPHAN_CUSTOMER_ID)
    orphan_pid_idx = take(N_ORDER_ORPHAN_PRODUCT_ID)

    for i in null_cid_idx:
        orders[i]["customer_id"] = None
    for i in null_pid_idx:
        orders[i]["product_id"] = None
    for j, i_row in enumerate(orphan_cid_idx):
        orders[i_row]["customer_id"] = ORPHAN_CUSTOMER_ID_START + j
    for j, i_row in enumerate(orphan_pid_idx):
        orders[i_row]["product_id"] = ORPHAN_PRODUCT_ID_START + j

    defect_set = set(picked)
    clean_indices = [i for i in range(len(orders)) if i not in defect_set]
    dup_idx = rng.sample(clean_indices, N_ORDER_DUPLICATE_ROWS)

    return {
        "null_customer_id_order_ids": [orders[i]["order_id"] for i in null_cid_idx],
        "null_product_id_order_ids": [orders[i]["order_id"] for i in null_pid_idx],
        "orphan_customer_id_order_ids": [orders[i]["order_id"] for i in orphan_cid_idx],
        "orphan_product_id_order_ids": [orders[i]["order_id"] for i in orphan_pid_idx],
        "duplicate_source_order_ids": [orders[i]["order_id"] for i in dup_idx],
        "duplicate_source_indices": dup_idx,
    }


def append_exact_copies(rows: list[Row], source_indices: Sequence[int]) -> list[Row]:
    extras = [rows[i].copy() for i in source_indices]
    return rows + extras


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    name: str
    expected: Any
    observed: Any
    kind: str  # mandatory | optional | accidental

    @property
    def ok(self) -> bool:
        return self.expected == self.observed


@dataclass
class ValidationReport:
    checks: list[CheckResult] = field(default_factory=list)

    def add(self, name: str, expected: Any, observed: Any, kind: str) -> CheckResult:
        result = CheckResult(name=name, expected=expected, observed=observed, kind=kind)
        self.checks.append(result)
        return result

    @property
    def failed(self) -> list[CheckResult]:
        return [c for c in self.checks if not c.ok]

    @property
    def ok(self) -> bool:
        return not self.failed


def _load_typed(
    output_dir: Path,
) -> tuple[list[Row], list[Row], list[Row], list[str], list[str], list[str]]:
    cust_cols, cust_raw = read_csv(output_dir / "customers.csv")
    order_cols, order_raw = read_csv(output_dir / "orders.csv")
    prod_cols, prod_raw = read_csv(output_dir / "products.csv")

    customers: list[Row] = []
    for raw in cust_raw:
        customers.append(
            {
                "customer_id": parse_optional_int(raw["customer_id"]),
                "customer_name": raw["customer_name"],
                "email": raw["email"] if raw["email"] != "" else None,
                "country": raw["country"],
                "signup_date": parse_optional_date(raw["signup_date"]),
                "customer_segment": raw["customer_segment"],
                "lifetime_value": parse_decimal(raw["lifetime_value"]),
            }
        )
    orders: list[Row] = []
    for raw in order_raw:
        orders.append(
            {
                "order_id": parse_optional_int(raw["order_id"]),
                "customer_id": parse_optional_int(raw["customer_id"]),
                "order_date": parse_optional_date(raw["order_date"]),
                "product_id": parse_optional_int(raw["product_id"]),
                "quantity": parse_optional_int(raw["quantity"]),
                "unit_price": parse_decimal(raw["unit_price"]),
                "total_amount": parse_decimal(raw["total_amount"]),
                "order_status": raw["order_status"],
                "payment_date": parse_optional_date(raw["payment_date"]),
            }
        )
    products: list[Row] = []
    for raw in prod_raw:
        products.append(
            {
                "product_id": parse_optional_int(raw["product_id"]),
                "product_name": raw["product_name"],
                "category": raw["category"],
                "price": parse_decimal(raw["price"]),
                "cost": parse_decimal(raw["cost"]),
                "stock_quantity": parse_optional_int(raw["stock_quantity"]),
                "reorder_level": parse_optional_int(raw["reorder_level"]),
            }
        )
    return customers, orders, products, cust_cols, order_cols, prod_cols


def validate_generated_files(
    output_dir: Path,
    seed: int,
    as_of: date,
    metadata: Mapping[str, Any] | None = None,
) -> ValidationReport:
    """
    Post-generation contract checks.

    Distinguishes:
      - mandatory listed defects (must match exactly)
      - optional future signups (must match the Stage 2 decision of 30)
      - accidental extras (must be zero)
    """
    report = ValidationReport()
    customers, orders, products, cust_cols, order_cols, prod_cols = _load_typed(output_dir)

    customer_ids = [r["customer_id"] for r in customers]
    order_ids = [r["order_id"] for r in orders]
    product_ids = [r["product_id"] for r in products]
    unique_customer_ids = {cid for cid in customer_ids if cid is not None}
    unique_order_ids = {oid for oid in order_ids if oid is not None}
    unique_product_ids = {pid for pid in product_ids if pid is not None}

    # 1–2. Physical row counts and unique-key counts
    report.add("customers.physical_rows", N_CUSTOMERS + N_CUSTOMER_DUPLICATE_ROWS, len(customers), "mandatory")
    report.add("orders.physical_rows", N_ORDERS + N_ORDER_DUPLICATE_ROWS, len(orders), "mandatory")
    report.add("products.physical_rows", N_PRODUCTS, len(products), "mandatory")
    report.add("customers.unique_customer_id", N_CUSTOMERS, len(unique_customer_ids), "mandatory")
    report.add("orders.unique_order_id", N_ORDERS, len(unique_order_ids), "mandatory")
    report.add("products.unique_product_id", N_PRODUCTS, len(unique_product_ids), "mandatory")

    # 3. Schema / no drift
    report.add("customers.columns", CUSTOMER_COLUMNS, cust_cols, "mandatory")
    report.add("orders.columns", ORDER_COLUMNS, order_cols, "mandatory")
    report.add("products.columns", PRODUCT_COLUMNS, prod_cols, "mandatory")

    # 4. NULL counts
    null_email = sum(1 for r in customers if r["email"] is None)
    null_order_cid = sum(1 for r in orders if r["customer_id"] is None)
    null_order_pid = sum(1 for r in orders if r["product_id"] is None)
    report.add("customers.null_email", N_CUSTOMER_NULL_EMAIL, null_email, "mandatory")
    report.add("orders.null_customer_id", N_ORDER_NULL_CUSTOMER_ID, null_order_cid, "mandatory")
    report.add("orders.null_product_id", N_ORDER_NULL_PRODUCT_ID, null_order_pid, "mandatory")

    pk_nulls = (
        sum(1 for r in customers if r["customer_id"] is None)
        + sum(1 for r in orders if r["order_id"] is None)
        + sum(1 for r in products if r["product_id"] is None)
    )
    report.add("accidental.null_primary_keys", 0, pk_nulls, "accidental")

    other_customer_nulls = sum(
        1
        for r in customers
        if r["customer_name"] in (None, "")
        or r["country"] in (None, "")
        or r["signup_date"] is None
        or r["customer_segment"] in (None, "")
        or r["lifetime_value"] is None
    )
    report.add("accidental.customers_other_nulls", 0, other_customer_nulls, "accidental")

    # 5. Duplicate counts (extra rows vs uniqueness FAIL rows)
    customer_id_counts = Counter(customer_ids)
    order_id_counts = Counter(order_ids)
    product_id_counts = Counter(product_ids)
    duplicated_customer_keys = [k for k, n in customer_id_counts.items() if n > 1]
    duplicated_order_keys = [k for k, n in order_id_counts.items() if n > 1]
    extra_customer_dup_rows = sum(n - 1 for n in customer_id_counts.values() if n > 1)
    extra_order_dup_rows = sum(n - 1 for n in order_id_counts.values() if n > 1)
    uniqueness_fail_customer_rows = sum(n for n in customer_id_counts.values() if n > 1)
    uniqueness_fail_order_rows = sum(n for n in order_id_counts.values() if n > 1)
    over_duplicated = sum(1 for n in list(customer_id_counts.values()) + list(order_id_counts.values()) + list(product_id_counts.values()) if n > 2)

    report.add("customers.duplicate_extra_rows", N_CUSTOMER_DUPLICATE_ROWS, extra_customer_dup_rows, "mandatory")
    report.add("customers.duplicated_key_count", N_CUSTOMER_DUPLICATE_ROWS, len(duplicated_customer_keys), "mandatory")
    report.add("customers.uniqueness_fail_rows", N_CUSTOMER_DUPLICATE_ROWS * 2, uniqueness_fail_customer_rows, "mandatory")
    report.add("orders.duplicate_extra_rows", N_ORDER_DUPLICATE_ROWS, extra_order_dup_rows, "mandatory")
    report.add("orders.duplicated_key_count", N_ORDER_DUPLICATE_ROWS, len(duplicated_order_keys), "mandatory")
    report.add("orders.uniqueness_fail_rows", N_ORDER_DUPLICATE_ROWS * 2, uniqueness_fail_order_rows, "mandatory")
    report.add("products.duplicate_extra_rows", 0, sum(n - 1 for n in product_id_counts.values() if n > 1), "accidental")
    report.add("accidental.keys_appearing_more_than_twice", 0, over_duplicated, "accidental")

    # Exact-copy check for duplicated keys
    def _group_rows(rows: Sequence[Row], key: str) -> dict[Any, list[Row]]:
        grouped: dict[Any, list[Row]] = {}
        for row in rows:
            grouped.setdefault(row[key], []).append(row)
        return grouped

    customer_groups = _group_rows(customers, "customer_id")
    order_groups = _group_rows(orders, "order_id")
    inexact_customer_dups = 0
    for key in duplicated_customer_keys:
        copies = customer_groups[key]
        if any(copy != copies[0] for copy in copies[1:]):
            inexact_customer_dups += 1
    inexact_order_dups = 0
    for key in duplicated_order_keys:
        copies = order_groups[key]
        if any(copy != copies[0] for copy in copies[1:]):
            inexact_order_dups += 1
    report.add("customers.inexact_duplicate_copies", 0, inexact_customer_dups, "accidental")
    report.add("orders.inexact_duplicate_copies", 0, inexact_order_dups, "accidental")

    # 6–7. Referential integrity: valid FKs vs intended orphans (NULL ≠ orphan)
    parent_customers = unique_customer_ids
    parent_products = unique_product_ids
    orphan_customer_rows = [
        r for r in orders if r["customer_id"] is not None and r["customer_id"] not in parent_customers
    ]
    orphan_product_rows = [
        r for r in orders if r["product_id"] is not None and r["product_id"] not in parent_products
    ]
    report.add("orders.orphan_customer_id", N_ORDER_ORPHAN_CUSTOMER_ID, len(orphan_customer_rows), "mandatory")
    report.add("orders.orphan_product_id", N_ORDER_ORPHAN_PRODUCT_ID, len(orphan_product_rows), "mandatory")
    # NULL FKs are excluded above (IS NOT NULL). Completeness owns NULLs; RI owns orphans.

    expected_orphan_cids = set(range(ORPHAN_CUSTOMER_ID_START, ORPHAN_CUSTOMER_ID_START + N_ORDER_ORPHAN_CUSTOMER_ID))
    expected_orphan_pids = set(range(ORPHAN_PRODUCT_ID_START, ORPHAN_PRODUCT_ID_START + N_ORDER_ORPHAN_PRODUCT_ID))
    observed_orphan_cids = {r["customer_id"] for r in orphan_customer_rows}
    observed_orphan_pids = {r["product_id"] for r in orphan_product_rows}
    report.add("orders.orphan_customer_id_namespace", expected_orphan_cids, observed_orphan_cids, "mandatory")
    report.add("orders.orphan_product_id_namespace", expected_orphan_pids, observed_orphan_pids, "mandatory")

    valid_customer_fk = sum(
        1 for r in orders if r["customer_id"] is not None and r["customer_id"] in parent_customers
    )
    valid_product_fk = sum(
        1 for r in orders if r["product_id"] is not None and r["product_id"] in parent_products
    )
    expected_valid_cid = (
        N_ORDERS + N_ORDER_DUPLICATE_ROWS - N_ORDER_NULL_CUSTOMER_ID - N_ORDER_ORPHAN_CUSTOMER_ID
    )
    expected_valid_pid = (
        N_ORDERS + N_ORDER_DUPLICATE_ROWS - N_ORDER_NULL_PRODUCT_ID - N_ORDER_ORPHAN_PRODUCT_ID
    )
    report.add("orders.valid_customer_fk_rows", expected_valid_cid, valid_customer_fk, "mandatory")
    report.add("orders.valid_product_fk_rows", expected_valid_pid, valid_product_fk, "mandatory")

    # 8. Value domains
    bad_segments = sum(1 for r in customers if r["customer_segment"] not in CUSTOMER_SEGMENTS)
    bad_status = sum(1 for r in orders if r["order_status"] not in ORDER_STATUSES)
    report.add("customers.invalid_segment", 0, bad_segments, "accidental")
    report.add("orders.invalid_status", 0, bad_status, "accidental")

    # 9. Date ranges and payment/status consistency
    future_signups = sum(1 for r in customers if r["signup_date"] is not None and r["signup_date"] > as_of)
    signup_before_window = sum(
        1 for r in customers if r["signup_date"] is not None and r["signup_date"] < SIGNUP_WINDOW_START
    )
    order_out_of_window = sum(
        1
        for r in orders
        if r["order_date"] is None or r["order_date"] < ORDER_WINDOW_START or r["order_date"] > as_of
    )
    completed_missing_payment = sum(
        1 for r in orders if r["order_status"] == "Completed" and r["payment_date"] is None
    )
    cancelled_has_payment = sum(
        1 for r in orders if r["order_status"] == "Cancelled" and r["payment_date"] is not None
    )
    payment_before_order = sum(
        1
        for r in orders
        if r["payment_date"] is not None and r["order_date"] is not None and r["payment_date"] < r["order_date"]
    )
    signup_by_id = {}
    for row in customers:
        cid = row["customer_id"]
        if cid not in signup_by_id:
            signup_by_id[cid] = row["signup_date"]
    order_before_signup = 0
    for row in orders:
        cid = row["customer_id"]
        if cid is None or cid not in signup_by_id:
            continue
        if row["order_date"] is not None and signup_by_id[cid] is not None and row["order_date"] < signup_by_id[cid]:
            order_before_signup += 1

    report.add("customers.future_signup_dates", N_FUTURE_SIGNUPS, future_signups, "optional")
    report.add("accidental.signup_before_window", 0, signup_before_window, "accidental")
    report.add("accidental.order_date_out_of_window", 0, order_out_of_window, "accidental")
    report.add("accidental.completed_missing_payment", 0, completed_missing_payment, "accidental")
    report.add("accidental.cancelled_has_payment", 0, cancelled_has_payment, "accidental")
    report.add("accidental.payment_before_order", 0, payment_before_order, "accidental")
    report.add("accidental.order_before_signup", 0, order_before_signup, "accidental")

    # Future-signup customers must not appear on valid (non-orphan) orders
    future_ids = {r["customer_id"] for r in customers if r["signup_date"] is not None and r["signup_date"] > as_of}
    orders_from_future = sum(1 for r in orders if r["customer_id"] in future_ids)
    report.add("accidental.orders_for_future_signup_customers", 0, orders_from_future, "accidental")

    # 10. Financial consistency and non-negative amounts
    amount_mismatch = 0
    negative_money = 0
    non_positive_qty = 0
    for row in orders:
        if row["quantity"] is None or row["quantity"] <= 0:
            non_positive_qty += 1
        if row["unit_price"] < 0 or row["total_amount"] < 0:
            negative_money += 1
        if row["quantity"] is not None and row["unit_price"] is not None and row["total_amount"] is not None:
            expected_total = money(Decimal(row["quantity"]) * row["unit_price"])
            if abs(row["total_amount"] - expected_total) > TWOPLACES:
                amount_mismatch += 1
    product_negative = sum(
        1
        for r in products
        if r["price"] < 0 or r["cost"] < 0 or r["stock_quantity"] < 0 or r["reorder_level"] < 0
    )
    customer_negative_ltv = sum(1 for r in customers if r["lifetime_value"] < 0)
    report.add("orders.amount_not_qty_times_price", 0, amount_mismatch, "accidental")
    report.add("accidental.non_positive_quantity", 0, non_positive_qty, "accidental")
    report.add("accidental.negative_order_money", 0, negative_money, "accidental")
    report.add("accidental.negative_product_or_stock", 0, product_negative, "accidental")
    report.add("accidental.negative_lifetime_value", 0, customer_negative_ltv, "accidental")

    # 11. Disjoint mandatory issue classes (no unplanned overlap)
    null_cid_set = {r["order_id"] for r in orders if r["customer_id"] is None}
    null_pid_set = {r["order_id"] for r in orders if r["product_id"] is None}
    orphan_cid_set = {r["order_id"] for r in orphan_customer_rows}
    orphan_pid_set = {r["order_id"] for r in orphan_product_rows}
    overlap_pairs = [
        ("null_cid_vs_null_pid", null_cid_set & null_pid_set),
        ("null_cid_vs_orphan_cid", null_cid_set & orphan_cid_set),
        ("null_pid_vs_orphan_pid", null_pid_set & orphan_pid_set),
        ("orphan_cid_vs_orphan_pid", orphan_cid_set & orphan_pid_set),
        ("null_cid_vs_orphan_pid", null_cid_set & orphan_pid_set),
        ("null_pid_vs_orphan_cid", null_pid_set & orphan_cid_set),
    ]
    overlapping_order_ids = set()
    for _, shared in overlap_pairs:
        overlapping_order_ids |= shared
    report.add("orders.overlapping_mandatory_defect_rows", 0, len(overlapping_order_ids), "accidental")

    dup_order_id_set = set(duplicated_order_keys)
    defect_order_ids = null_cid_set | null_pid_set | orphan_cid_set | orphan_pid_set
    dup_on_defect = dup_order_id_set & defect_order_ids
    report.add("orders.duplicate_copies_of_defect_rows", 0, len(dup_on_defect), "accidental")

    null_email_ids = {r["customer_id"] for r in customers if r["email"] is None}
    future_and_null = null_email_ids & future_ids
    dup_and_null = set(duplicated_customer_keys) & null_email_ids
    dup_and_future = set(duplicated_customer_keys) & future_ids
    report.add("customers.null_email_and_future_overlap", 0, len(future_and_null), "accidental")
    report.add("customers.duplicate_and_null_email_overlap", 0, len(dup_and_null), "accidental")
    report.add("customers.duplicate_and_future_overlap", 0, len(dup_and_future), "accidental")

    # 12. Reproducibility metadata
    report.add("reproducibility.seed", seed, seed if metadata is None else metadata.get("seed", seed), "mandatory")
    report.add("reproducibility.as_of_date", as_of.isoformat(), as_of.isoformat(), "mandatory")
    customers_hash = sha256_file(output_dir / "customers.csv")
    orders_hash = sha256_file(output_dir / "orders.csv")
    products_hash = sha256_file(output_dir / "products.csv")
    report.add("reproducibility.customers_sha256_recorded", True, bool(customers_hash), "mandatory")
    report.add("reproducibility.orders_sha256_recorded", True, bool(orders_hash), "mandatory")
    report.add("reproducibility.products_sha256_recorded", True, bool(products_hash), "mandatory")

    LOGGER.info("SHA256 customers.csv = %s", customers_hash)
    LOGGER.info("SHA256 orders.csv     = %s", orders_hash)
    LOGGER.info("SHA256 products.csv    = %s", products_hash)

    _log_report(report)
    return report


def _log_report(report: ValidationReport) -> None:
    LOGGER.info("Validation summary (expected vs observed)")
    LOGGER.info("%-55s %-12s %-12s %-10s %s", "check", "expected", "observed", "kind", "result")
    for check in report.checks:
        status = "PASS" if check.ok else "FAIL"
        LOGGER.info(
            "%-55s %-12s %-12s %-10s %s",
            check.name,
            _short(check.expected),
            _short(check.observed),
            check.kind,
            status,
        )
    if report.ok:
        LOGGER.info("Validation PASSED: all contract checks matched.")
    else:
        LOGGER.error("Validation FAILED (%s checks):", len(report.failed))
        for check in report.failed:
            LOGGER.error("  %s: expected %r observed %r", check.name, check.expected, check.observed)


def _short(value: Any) -> str:
    text = str(value)
    if len(text) > 12:
        return text[:9] + "..."
    return text


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def generate_sample_data(
    output_dir: Path | str | None = None,
    seed: int = DEFAULT_SEED,
    as_of_date: date | None = None,
) -> dict[str, Any]:
    """
    Generate the three CSVs, validate them, and return run metadata.

    Raises GenerationError if validation fails. Files may still be present on disk
    for debugging.
    """
    as_of = as_of_date or AS_OF_DATE
    dest = Path(output_dir) if output_dir is not None else DEFAULT_OUTPUT_DIR
    dest = dest.resolve()
    dest.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Generating sample data into %s (seed=%s, as_of=%s)", dest, seed, as_of)
    rng = random.Random(seed)

    LOGGER.info("Generating %s products", N_PRODUCTS)
    products = generate_products(rng)

    LOGGER.info("Generating %s unique customers", N_CUSTOMERS)
    customers = generate_customers(rng, as_of)
    customer_meta = inject_customer_defects(customers, rng, as_of)
    future_ids = set(customer_meta["future_signup_customer_ids"])

    LOGGER.info("Generating %s unique orders", N_ORDERS)
    orders = generate_orders(rng, customers, products, as_of, future_ids)
    order_meta = inject_order_defects(orders, rng)

    customers = append_exact_copies(customers, customer_meta["duplicate_source_indices"])
    orders = append_exact_copies(orders, order_meta["duplicate_source_indices"])

    LOGGER.info(
        "Writing CSVs: %s customer rows, %s order rows, %s product rows",
        len(customers),
        len(orders),
        len(products),
    )
    write_csv(dest / "customers.csv", CUSTOMER_COLUMNS, customers)
    write_csv(dest / "orders.csv", ORDER_COLUMNS, orders)
    write_csv(dest / "products.csv", PRODUCT_COLUMNS, products)

    metadata: dict[str, Any] = {
        "seed": seed,
        "as_of_date": as_of.isoformat(),
        "output_dir": str(dest),
        "physical_rows": {
            "customers": len(customers),
            "orders": len(orders),
            "products": len(products),
        },
        "unique_keys": {
            "customers": N_CUSTOMERS,
            "orders": N_ORDERS,
            "products": N_PRODUCTS,
        },
        "duplicate_source_customer_ids": customer_meta["duplicate_source_customer_ids"],
        "duplicate_source_order_ids": order_meta["duplicate_source_order_ids"],
        "null_email_customer_ids": customer_meta["null_email_customer_ids"],
        "future_signup_customer_ids": customer_meta["future_signup_customer_ids"],
        "null_customer_id_order_ids": order_meta["null_customer_id_order_ids"],
        "null_product_id_order_ids": order_meta["null_product_id_order_ids"],
        "orphan_customer_id_order_ids": order_meta["orphan_customer_id_order_ids"],
        "orphan_product_id_order_ids": order_meta["orphan_product_id_order_ids"],
        "file_sha256": {
            "customers.csv": sha256_file(dest / "customers.csv"),
            "orders.csv": sha256_file(dest / "orders.csv"),
            "products.csv": sha256_file(dest / "products.csv"),
        },
    }

    LOGGER.info("Duplicate source customer_id values: %s", metadata["duplicate_source_customer_ids"])
    LOGGER.info("Duplicate source order_id values: %s", metadata["duplicate_source_order_ids"])
    LOGGER.info("NULL email customer_id values: %s", metadata["null_email_customer_ids"])
    LOGGER.info("Future signup customer_id values: %s", metadata["future_signup_customer_ids"])

    report = validate_generated_files(dest, seed=seed, as_of=as_of, metadata=metadata)
    metadata["validation_ok"] = report.ok
    metadata["failed_checks"] = [c.name for c in report.failed]
    if not report.ok:
        raise GenerationError(
            "Validation failed: " + ", ".join(f"{c.name} expected={c.expected!r} observed={c.observed!r}" for c in report.failed)
        )
    LOGGER.info("Generation complete and validated.")
    return metadata


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate deterministic synthetic e-commerce CSVs for the DE C1 pipeline."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory for customers.csv, orders.csv, and products.csv (default: <repo>/data)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"RNG seed (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=AS_OF_DATE,
        help=f"Frozen as-of date for 'future' signups (default: {AS_OF_DATE.isoformat()})",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = build_arg_parser().parse_args(argv)
    try:
        generate_sample_data(output_dir=args.output_dir, seed=args.seed, as_of_date=args.as_of_date)
    except GenerationError as exc:
        LOGGER.error("%s", exc)
        return 1
    except OSError as exc:
        LOGGER.error("Failed to write sample data: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
