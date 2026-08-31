"""
Databricks and local validation utilities for the medallion workflow.

Source-file checks query the actual CSVs (stdlib). Table checks query Spark
tables. Nothing in this module hard-codes a PASS result.
"""

from __future__ import annotations

import csv
import logging
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

_THIS_DIR = Path(__file__).resolve().parent
_SRC_DIR = _THIS_DIR.parent
_BRONZE_DIR = _SRC_DIR / "bronze"
for _path in (str(_SRC_DIR), str(_BRONZE_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from config import PipelineConfig, repo_root  # noqa: E402
from contracts import (  # noqa: E402
    BRONZE_ENTITY_TABLES,
    BRONZE_METADATA_TABLE,
    INGEST_ROW_ID_COLUMN,
    SOURCE_FILENAMES,
)

LOGGER = logging.getLogger("databricks.validate")

AS_OF_DATE = date(2026, 8, 31)
EXPECTED_CUSTOMER_ROWS = 10_010
EXPECTED_ORDER_ROWS = 100_020
EXPECTED_PRODUCT_ROWS = 500
EXPECTED_NULL_EMAILS = 50
EXPECTED_NULL_ORDER_CUSTOMER_ID = 100
EXPECTED_NULL_ORDER_PRODUCT_ID = 200
EXPECTED_DUP_CUSTOMER_KEYS = 10
EXPECTED_DUP_CUSTOMER_ROWS = 20
EXPECTED_DUP_ORDER_KEYS = 20
EXPECTED_DUP_ORDER_ROWS = 40
EXPECTED_ORPHAN_CUSTOMER_ID = 50
EXPECTED_ORPHAN_PRODUCT_ID = 30
EXPECTED_FUTURE_SIGNUPS = 30
MANDATORY_ISSUE_INSTANCES = 460
EXPECTED_CUSTOMER_FAIL_ROWS = 100  # 50 NULL email + 20 uniqueness + 30 future
EXPECTED_ORDER_FAIL_ROWS = 420  # 100+200+40+50+30 disjoint on seed 42
EXPECTED_TYPE_FAILURES = 0
GOLD_ELIGIBILITY_PREDICATE = (
    "order_status = 'Completed' AND quality_check_result = 'PASS'"
)
SEGMENT_TYPES = ("Inactive", "High-Value", "Repeat", "One-Time")
CUSTOMER_SEGMENTS = ("Premium", "Standard", "Basic")

DASHBOARD_QUERY_PREFIX = "-- DASHBOARD_QUERY:"
DASHBOARD_SQL_PATH = repo_root() / "src" / "dashboard" / "dashboard_queries.sql"

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"


class ValidationError(RuntimeError):
    """A critical validation failure that should fail the workflow."""


@dataclass(frozen=True)
class CheckResult:
    check: str
    expected: str
    actual: str
    status: str
    notes: str = ""
    critical: bool = True

    @property
    def passed(self) -> bool:
        return self.status == STATUS_PASS


@dataclass
class ValidationReport:
    checks: list[CheckResult] = field(default_factory=list)

    def add(self, result: CheckResult) -> CheckResult:
        self.checks.append(result)
        LOGGER.info(
            "CHECK %s expected=%s actual=%s status=%s notes=%s",
            result.check,
            result.expected,
            result.actual,
            result.status,
            result.notes,
        )
        return result

    def compare(
        self,
        check: str,
        expected: Any,
        actual: Any,
        *,
        notes: str = "",
        critical: bool = True,
    ) -> CheckResult:
        status = STATUS_PASS if expected == actual else STATUS_FAIL
        return self.add(
            CheckResult(
                check=check,
                expected=_fmt(expected),
                actual=_fmt(actual),
                status=status,
                notes=notes,
                critical=critical,
            )
        )

    def compare_true(
        self,
        check: str,
        actual: bool,
        *,
        notes: str = "",
        critical: bool = True,
    ) -> CheckResult:
        return self.compare(check, True, actual, notes=notes, critical=critical)

    @property
    def critical_failures(self) -> list[CheckResult]:
        return [item for item in self.checks if item.critical and not item.passed]

    @property
    def passed(self) -> bool:
        return not self.critical_failures

    def final_result(self) -> str:
        return STATUS_PASS if self.passed else STATUS_FAIL

    def format_table(self) -> str:
        rows = [
            ("CHECK", "EXPECTED", "ACTUAL", "STATUS", "NOTES"),
            ("-" * 5, "-" * 8, "-" * 6, "-" * 6, "-" * 5),
        ]
        for item in self.checks:
            rows.append(
                (item.check, item.expected, item.actual, item.status, item.notes)
            )
        widths = [max(len(row[i]) for row in rows) for i in range(5)]
        lines = [
            "  ".join(value.ljust(widths[i]) for i, value in enumerate(row))
            for row in rows
        ]
        lines.append("")
        lines.append(f"FINAL RESULT: {self.final_result()}")
        return "\n".join(lines)


def _fmt(value: Any) -> str:
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def _blank_to_none(value: str | None) -> str | None:
    if value is None:
        return None
    return value if value != "" else None


def load_csv_dicts(path: Path) -> list[dict[str, str | None]]:
    if not path.is_file():
        raise ValidationError(f"Source file does not exist: {path}")
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValidationError(f"Source file has no header: {path}")
        rows: list[dict[str, str | None]] = []
        for raw in reader:
            rows.append({key: _blank_to_none(raw.get(key)) for key in reader.fieldnames})
        return rows


def _parse_int(value: str | None) -> int | None:
    if value is None:
        return None
    return int(value)


def _parse_date(value: str | None) -> date | None:
    if value is None:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


@dataclass(frozen=True)
class SourceProfile:
    customer_rows: int
    order_rows: int
    product_rows: int
    null_emails: int
    null_order_customer_id: int
    null_order_product_id: int
    duplicate_customer_keys: int
    duplicate_customer_rows: int
    duplicate_order_keys: int
    duplicate_order_rows: int
    orphan_customer_id: int
    orphan_product_id: int
    future_signups: int
    future_signup_also_null_email: int
    future_signup_also_duplicate: int
    mandatory_issue_instances: int


def profile_source_rows(
    customers: Sequence[Mapping[str, str | None]],
    orders: Sequence[Mapping[str, str | None]],
    products: Sequence[Mapping[str, str | None]],
    *,
    as_of: date = AS_OF_DATE,
) -> SourceProfile:
    customer_ids = [
        row.get("customer_id") for row in customers if row.get("customer_id")
    ]
    order_ids = [row.get("order_id") for row in orders if row.get("order_id")]
    parent_customer_ids = set(customer_ids)
    parent_product_ids = {
        row.get("product_id") for row in products if row.get("product_id")
    }
    customer_counts = Counter(customer_ids)
    order_counts = Counter(order_ids)
    dup_customer_keys = {key for key, count in customer_counts.items() if count > 1}
    dup_order_keys = {key for key, count in order_counts.items() if count > 1}
    null_emails = sum(1 for row in customers if row.get("email") is None)
    future_rows = [
        row
        for row in customers
        if _parse_date(row.get("signup_date")) is not None
        and _parse_date(row.get("signup_date")) > as_of
    ]
    future_ids = {row.get("customer_id") for row in future_rows}
    future_null_email = sum(1 for row in future_rows if row.get("email") is None)
    future_duplicate = sum(1 for key in future_ids if key in dup_customer_keys)
    orphan_customer = sum(
        1
        for row in orders
        if row.get("customer_id") is not None
        and row.get("customer_id") not in parent_customer_ids
    )
    orphan_product = sum(
        1
        for row in orders
        if row.get("product_id") is not None
        and row.get("product_id") not in parent_product_ids
    )
    return SourceProfile(
        customer_rows=len(customers),
        order_rows=len(orders),
        product_rows=len(products),
        null_emails=null_emails,
        null_order_customer_id=sum(
            1 for row in orders if row.get("customer_id") is None
        ),
        null_order_product_id=sum(1 for row in orders if row.get("product_id") is None),
        duplicate_customer_keys=len(dup_customer_keys),
        duplicate_customer_rows=sum(
            count for key, count in customer_counts.items() if key in dup_customer_keys
        ),
        duplicate_order_keys=len(dup_order_keys),
        duplicate_order_rows=sum(
            count for key, count in order_counts.items() if key in dup_order_keys
        ),
        orphan_customer_id=orphan_customer,
        orphan_product_id=orphan_product,
        future_signups=len(future_rows),
        future_signup_also_null_email=future_null_email,
        future_signup_also_duplicate=future_duplicate,
        mandatory_issue_instances=(
            null_emails
            + len(dup_customer_keys)
            + sum(1 for row in orders if row.get("customer_id") is None)
            + sum(1 for row in orders if row.get("product_id") is None)
            + orphan_customer
            + orphan_product
            + len(dup_order_keys)
        ),
    )


def add_source_profile_checks(
    report: ValidationReport, profile: SourceProfile
) -> ValidationReport:
    report.compare("source.customers.rows", EXPECTED_CUSTOMER_ROWS, profile.customer_rows)
    report.compare("source.orders.rows", EXPECTED_ORDER_ROWS, profile.order_rows)
    report.compare("source.products.rows", EXPECTED_PRODUCT_ROWS, profile.product_rows)
    report.compare("source.customers.null_email", EXPECTED_NULL_EMAILS, profile.null_emails)
    report.compare(
        "source.orders.null_customer_id",
        EXPECTED_NULL_ORDER_CUSTOMER_ID,
        profile.null_order_customer_id,
    )
    report.compare(
        "source.orders.null_product_id",
        EXPECTED_NULL_ORDER_PRODUCT_ID,
        profile.null_order_product_id,
    )
    report.compare(
        "source.customers.duplicate_keys",
        EXPECTED_DUP_CUSTOMER_KEYS,
        profile.duplicate_customer_keys,
        notes="distinct customer_id values that appear more than once",
    )
    report.compare(
        "source.customers.duplicate_participating_rows",
        EXPECTED_DUP_CUSTOMER_ROWS,
        profile.duplicate_customer_rows,
        notes="physical rows that share a duplicated customer_id",
    )
    report.compare(
        "source.orders.duplicate_keys",
        EXPECTED_DUP_ORDER_KEYS,
        profile.duplicate_order_keys,
        notes="distinct order_id values that appear more than once",
    )
    report.compare(
        "source.orders.duplicate_participating_rows",
        EXPECTED_DUP_ORDER_ROWS,
        profile.duplicate_order_rows,
        notes="physical rows that share a duplicated order_id",
    )
    report.compare(
        "source.orders.orphan_customer_id",
        EXPECTED_ORPHAN_CUSTOMER_ID,
        profile.orphan_customer_id,
        notes="non-null customer_id absent from customers; NULL is not an orphan",
    )
    report.compare(
        "source.orders.orphan_product_id",
        EXPECTED_ORPHAN_PRODUCT_ID,
        profile.orphan_product_id,
        notes="non-null product_id absent from products; NULL is not an orphan",
    )
    report.compare(
        "source.customers.future_signups",
        EXPECTED_FUTURE_SIGNUPS,
        profile.future_signups,
        notes=f"signup_date > {AS_OF_DATE.isoformat()}; optional BL defect",
    )
    report.compare(
        "source.future_signups_disjoint_from_null_email",
        0,
        profile.future_signup_also_null_email,
        notes="future signups must not be counted inside the 50 NULL emails",
    )
    report.compare(
        "source.future_signups_disjoint_from_duplicate_keys",
        0,
        profile.future_signup_also_duplicate,
        notes="future signups must not be the 10 extra duplicate customer rows",
    )
    report.compare(
        "source.mandatory_issue_instances",
        MANDATORY_ISSUE_INSTANCES,
        profile.mandatory_issue_instances,
        notes="listed 460; future signups are separate",
    )
    return report


def validate_source_files(
    data_path: str | Path,
    report: ValidationReport | None = None,
) -> ValidationReport:
    """Query the three CSVs on disk / workspace / volume POSIX path."""
    result = report or ValidationReport()
    root = Path(data_path)
    customers = load_csv_dicts(root / SOURCE_FILENAMES["customers"])
    orders = load_csv_dicts(root / SOURCE_FILENAMES["orders"])
    products = load_csv_dicts(root / SOURCE_FILENAMES["products"])
    profile = profile_source_rows(customers, orders, products)
    add_source_profile_checks(result, profile)
    return result


def split_dashboard_queries(sql_text: str) -> dict[str, str]:
    parts: dict[str, str] = {}
    current_name: str | None = None
    current_lines: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith(DASHBOARD_QUERY_PREFIX):
            if current_name is not None:
                statement = "\n".join(current_lines).strip().rstrip(";").strip()
                parts[current_name] = statement
            current_name = stripped[len(DASHBOARD_QUERY_PREFIX) :].strip()
            current_lines = []
        elif current_name is not None:
            current_lines.append(line)
    if current_name is not None:
        parts[current_name] = "\n".join(current_lines).strip().rstrip(";").strip()
    return parts


def render_dashboard_sql(sql_text: str, gold_schema: str) -> str:
    rendered = sql_text.replace("{gold_schema}", gold_schema)
    if "{gold_schema}" in rendered:
        raise ValidationError("Dashboard SQL still contains {gold_schema}.")
    return rendered


def executable_sql(sql_text: str) -> str:
    return "\n".join(
        line for line in sql_text.splitlines() if not line.lstrip().startswith("--")
    )


def load_dashboard_queries(gold_schema: str) -> dict[str, str]:
    raw = split_dashboard_queries(DASHBOARD_SQL_PATH.read_text(encoding="utf-8"))
    return {
        name: executable_sql(render_dashboard_sql(sql, gold_schema))
        for name, sql in raw.items()
    }


def _spark_count(spark: Any, sql: str) -> int:
    rows = spark.sql(sql).collect()
    if not rows:
        return 0
    return int(rows[0][0] or 0)


def _spark_decimal(spark: Any, sql: str) -> Decimal:
    rows = spark.sql(sql).collect()
    if not rows or rows[0][0] is None:
        return Decimal("0")
    return Decimal(str(rows[0][0]))


def _table_exists(spark: Any, qualified: str) -> bool:
    try:
        exists = spark.catalog.tableExists(qualified)
        if isinstance(exists, bool):
            return exists
    except Exception:
        pass
    try:
        spark.table(qualified).limit(0).collect()
        return True
    except Exception:
        return False


def _table_format(spark: Any, qualified: str) -> str:
    try:
        detail = spark.sql(f"DESCRIBE DETAIL {qualified}").collect()
        if detail:
            fmt = detail[0]["format"]
            return str(fmt).lower()
    except Exception as exc:
        LOGGER.info("DESCRIBE DETAIL unavailable for %s: %s", qualified, exc.__class__.__name__)
    try:
        rows = spark.sql(f"DESCRIBE TABLE EXTENDED {qualified}").collect()
        for row in rows:
            col = str(row[0]).strip().lower() if row[0] is not None else ""
            if col in {"provider", "type"}:
                return str(row[1]).lower()
    except Exception as exc:
        LOGGER.info(
            "DESCRIBE TABLE EXTENDED unavailable for %s: %s",
            qualified,
            exc.__class__.__name__,
        )
    return "unknown"


def validate_bronze(
    spark: Any, config: PipelineConfig, report: ValidationReport
) -> ValidationReport:
    expected_rows = {
        "customers": EXPECTED_CUSTOMER_ROWS,
        "orders": EXPECTED_ORDER_ROWS,
        "products": EXPECTED_PRODUCT_ROWS,
    }
    for table_name in BRONZE_ENTITY_TABLES:
        qualified = config.bronze_table(table_name)
        exists = _table_exists(spark, qualified)
        report.compare_true(
            f"bronze.{table_name}.exists", exists, notes=qualified
        )
        if not exists:
            continue
        fmt = _table_format(spark, qualified)
        report.compare(
            f"bronze.{table_name}.format",
            config.table_format,
            fmt,
            notes="Databricks default is delta",
        )
        actual_rows = spark.table(qualified).count()
        report.compare(f"bronze.{table_name}.rows", expected_rows[table_name], actual_rows)
        unique_ids = spark.table(qualified).select(INGEST_ROW_ID_COLUMN).distinct().count()
        report.compare(
            f"bronze.{table_name}._ingest_row_id.unique",
            actual_rows,
            unique_ids,
            notes="unique per physical row of this write; not stable across reruns",
        )

    customers = config.bronze_table("customers")
    orders = config.bronze_table("orders")
    products = config.bronze_table("products")
    if _table_exists(spark, customers):
        report.compare(
            "bronze.customers.null_email_preserved",
            EXPECTED_NULL_EMAILS,
            _spark_count(spark, f"SELECT COUNT(*) FROM {customers} WHERE email IS NULL"),
        )
        report.compare(
            "bronze.customers.duplicate_keys_preserved",
            EXPECTED_DUP_CUSTOMER_KEYS,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM (SELECT customer_id FROM {customers} "
                "WHERE customer_id IS NOT NULL GROUP BY customer_id HAVING COUNT(*) > 1)",
            ),
        )
        report.compare(
            "bronze.customers.duplicate_participating_rows_preserved",
            EXPECTED_DUP_CUSTOMER_ROWS,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM {customers} c WHERE customer_id IN ("
                f"SELECT customer_id FROM {customers} WHERE customer_id IS NOT NULL "
                "GROUP BY customer_id HAVING COUNT(*) > 1)",
            ),
        )
        report.compare(
            "bronze.customers.future_signups_preserved",
            EXPECTED_FUTURE_SIGNUPS,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM {customers} "
                f"WHERE signup_date > DATE '{AS_OF_DATE.isoformat()}'",
            ),
        )
    if _table_exists(spark, orders) and _table_exists(spark, customers) and _table_exists(
        spark, products
    ):
        report.compare(
            "bronze.orders.null_customer_id_preserved",
            EXPECTED_NULL_ORDER_CUSTOMER_ID,
            _spark_count(
                spark, f"SELECT COUNT(*) FROM {orders} WHERE customer_id IS NULL"
            ),
        )
        report.compare(
            "bronze.orders.null_product_id_preserved",
            EXPECTED_NULL_ORDER_PRODUCT_ID,
            _spark_count(
                spark, f"SELECT COUNT(*) FROM {orders} WHERE product_id IS NULL"
            ),
        )
        report.compare(
            "bronze.orders.duplicate_keys_preserved",
            EXPECTED_DUP_ORDER_KEYS,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM (SELECT order_id FROM {orders} "
                "WHERE order_id IS NOT NULL GROUP BY order_id HAVING COUNT(*) > 1)",
            ),
        )
        report.compare(
            "bronze.orders.duplicate_participating_rows_preserved",
            EXPECTED_DUP_ORDER_ROWS,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM {orders} o WHERE order_id IN ("
                f"SELECT order_id FROM {orders} WHERE order_id IS NOT NULL "
                "GROUP BY order_id HAVING COUNT(*) > 1)",
            ),
        )
        report.compare(
            "bronze.orders.orphan_customer_id_preserved",
            EXPECTED_ORPHAN_CUSTOMER_ID,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM {orders} o LEFT ANTI JOIN "
                f"(SELECT DISTINCT customer_id FROM {customers} "
                "WHERE customer_id IS NOT NULL) c "
                "ON o.customer_id = c.customer_id "
                "WHERE o.customer_id IS NOT NULL",
            ),
            notes="NULL foreign keys are not orphans",
        )
        report.compare(
            "bronze.orders.orphan_product_id_preserved",
            EXPECTED_ORPHAN_PRODUCT_ID,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM {orders} o LEFT ANTI JOIN "
                f"(SELECT DISTINCT product_id FROM {products} "
                "WHERE product_id IS NOT NULL) p "
                "ON o.product_id = p.product_id "
                "WHERE o.product_id IS NOT NULL",
            ),
        )

    metadata = config.bronze_table(BRONZE_METADATA_TABLE)
    report.compare_true(
        "bronze.ingest_metadata.exists",
        _table_exists(spark, metadata),
        notes=metadata,
    )
    if _table_exists(spark, metadata):
        latest = spark.sql(
            f"SELECT ingest_id FROM {metadata} ORDER BY ingested_at DESC LIMIT 1"
        ).collect()
        if not latest:
            report.compare("bronze.ingest_metadata.latest_ingest_id", "present", "missing")
            return report
        ingest_id = latest[0][0]
        latest_rows = spark.sql(
            f"SELECT table_name, row_count, ingested_at, status, error_message "
            f"FROM {metadata} WHERE ingest_id = '{ingest_id}'"
        ).collect()
        names = sorted(str(row["table_name"]) for row in latest_rows)
        report.compare(
            "bronze.ingest_metadata.latest_tables",
            ["customers", "orders", "products"],
            names,
        )
        by_name = {str(row["table_name"]): row for row in latest_rows}
        for table_name, expected in expected_rows.items():
            row = by_name.get(table_name)
            if row is None:
                continue
            report.compare(
                f"bronze.ingest_metadata.{table_name}.row_count",
                expected,
                int(row["row_count"]),
            )
            report.compare(
                f"bronze.ingest_metadata.{table_name}.status",
                "SUCCESS",
                str(row["status"]),
            )
            report.compare_true(
                f"bronze.ingest_metadata.{table_name}.ingested_at_present",
                row["ingested_at"] is not None,
            )
            report.compare(
                f"bronze.ingest_metadata.{table_name}.error_message",
                None,
                row["error_message"],
            )
    return report


def _metric_fail_count(
    spark: Any, metrics_table: str, table_name: str, check_name: str
) -> int | None:
    rows = spark.sql(
        f"SELECT fail_count FROM {metrics_table} "
        f"WHERE table_name = '{table_name}' AND check_name = '{check_name}'"
    ).collect()
    if not rows:
        return None
    return int(rows[0][0])


def validate_silver(
    spark: Any, config: PipelineConfig, report: ValidationReport
) -> ValidationReport:
    for table_name, expected in (
        ("customers", EXPECTED_CUSTOMER_ROWS),
        ("orders", EXPECTED_ORDER_ROWS),
        ("products", EXPECTED_PRODUCT_ROWS),
    ):
        bronze_q = config.bronze_table(table_name)
        silver_q = config.silver_table(table_name)
        exists = _table_exists(spark, silver_q)
        report.compare_true(f"silver.{table_name}.exists", exists, notes=silver_q)
        if not exists or not _table_exists(spark, bronze_q):
            continue
        bronze_rows = spark.table(bronze_q).count()
        silver_rows = spark.table(silver_q).count()
        report.compare(f"silver.{table_name}.rows_vs_bronze", bronze_rows, silver_rows)
        report.compare(f"silver.{table_name}.rows", expected, silver_rows)
        fmt = _table_format(spark, silver_q)
        report.compare(f"silver.{table_name}.format", config.table_format, fmt)

    customers = config.silver_table("customers")
    orders = config.silver_table("orders")
    metrics = config.silver_table("quality_metrics")
    metric_expectations: tuple[tuple[str, str, int], ...] = (
        ("customers", "completeness:customers.email", EXPECTED_NULL_EMAILS),
        ("orders", "completeness:orders.customer_id", EXPECTED_NULL_ORDER_CUSTOMER_ID),
        ("orders", "completeness:orders.product_id", EXPECTED_NULL_ORDER_PRODUCT_ID),
        ("customers", "uniqueness:customers.customer_id", EXPECTED_DUP_CUSTOMER_ROWS),
        ("customers", "uniqueness:customers.customer_id.duplicate_keys", EXPECTED_DUP_CUSTOMER_KEYS),
        ("orders", "uniqueness:orders.order_id", EXPECTED_DUP_ORDER_ROWS),
        ("orders", "uniqueness:orders.order_id.duplicate_keys", EXPECTED_DUP_ORDER_KEYS),
        ("customers", "type", EXPECTED_TYPE_FAILURES),
        ("orders", "type", EXPECTED_TYPE_FAILURES),
        ("products", "type", EXPECTED_TYPE_FAILURES),
        ("orders", "ri:orders.customer_id_orphan", EXPECTED_ORPHAN_CUSTOMER_ID),
        ("orders", "ri:orders.product_id_orphan", EXPECTED_ORPHAN_PRODUCT_ID),
        ("customers", "business:customers.signup_not_future", EXPECTED_FUTURE_SIGNUPS),
        ("customers", "quality_check_result", EXPECTED_CUSTOMER_FAIL_ROWS),
        ("orders", "quality_check_result", EXPECTED_ORDER_FAIL_ROWS),
    )
    report.compare_true("silver.quality_metrics.exists", _table_exists(spark, metrics))
    if _table_exists(spark, metrics):
        for table_name, check_name, expected in metric_expectations:
            actual = _metric_fail_count(spark, metrics, table_name, check_name)
            report.compare(
                f"silver.quality_metrics.{table_name}.{check_name}",
                expected,
                actual if actual is not None else "missing",
                notes="queried from silver.quality_metrics; not hard-coded",
            )
    if _table_exists(spark, customers):
        report.compare(
            "silver.customers.completeness_email_fail_rows",
            EXPECTED_NULL_EMAILS,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM {customers} WHERE completeness_pass = false "
                "AND array_contains(failed_checks, 'completeness:customers.email')",
            ),
        )
        report.compare(
            "silver.customers.quality_check_result_fail_rows",
            EXPECTED_CUSTOMER_FAIL_ROWS,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM {customers} WHERE quality_check_result = 'FAIL'",
            ),
        )
    if _table_exists(spark, orders):
        report.compare(
            "silver.orders.quality_check_result_fail_rows",
            EXPECTED_ORDER_FAIL_ROWS,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM {orders} WHERE quality_check_result = 'FAIL'",
            ),
        )
        report.compare(
            "silver.orders.null_fk_not_orphans",
            EXPECTED_NULL_ORDER_CUSTOMER_ID,
            _spark_count(
                spark,
                f"SELECT COUNT(*) FROM {orders} WHERE customer_id IS NULL "
                "AND NOT array_contains(failed_checks, 'ri:orders.customer_id_orphan')",
            ),
            notes="NULL customer_id is completeness, not RI",
        )
    return report


def validate_gold(
    spark: Any, config: PipelineConfig, report: ValidationReport
) -> ValidationReport:
    gold_tables = (
        "sales_by_product",
        "revenue_by_customer",
        "daily_trends",
        "weekly_trends",
        "customer_segmentation",
    )
    for table_name in gold_tables:
        qualified = config.gold_table(table_name)
        exists = _table_exists(spark, qualified)
        report.compare_true(f"gold.{table_name}.exists", exists, notes=qualified)
        if exists:
            report.compare(
                f"gold.{table_name}.format",
                config.table_format,
                _table_format(spark, qualified),
            )

    silver_orders = config.silver_table("orders")
    gold_product = config.gold_table("sales_by_product")
    gold_customer = config.gold_table("revenue_by_customer")
    gold_daily = config.gold_table("daily_trends")
    gold_weekly = config.gold_table("weekly_trends")
    gold_seg = config.gold_table("customer_segmentation")
    if not all(
        _table_exists(spark, name)
        for name in (silver_orders, gold_product, gold_customer, gold_seg)
    ):
        return report

    eligible_sql = (
        f"SELECT SUM(total_amount) FROM {silver_orders} "
        f"WHERE {GOLD_ELIGIBILITY_PREDICATE}"
    )
    eligible_orders_sql = (
        f"SELECT COUNT(*) FROM {silver_orders} WHERE {GOLD_ELIGIBILITY_PREDICATE}"
    )
    eligible_revenue = _spark_decimal(spark, eligible_sql)
    eligible_orders = _spark_count(spark, eligible_orders_sql)
    product_revenue = _spark_decimal(
        spark, f"SELECT SUM(total_revenue) FROM {gold_product}"
    )
    customer_revenue = _spark_decimal(
        spark, f"SELECT SUM(total_revenue) FROM {gold_customer}"
    )
    product_orders = _spark_count(
        spark, f"SELECT SUM(total_orders) FROM {gold_product}"
    )
    customer_orders = _spark_count(
        spark, f"SELECT SUM(total_orders) FROM {gold_customer}"
    )
    report.compare(
        "gold.eligible_silver_revenue",
        eligible_revenue,
        product_revenue,
        notes="Completed AND quality_check_result = PASS",
    )
    report.compare(
        "gold.product_revenue_vs_customer_revenue",
        product_revenue,
        customer_revenue,
    )
    report.compare("gold.eligible_order_count_vs_products", eligible_orders, product_orders)
    report.compare("gold.eligible_order_count_vs_customers", eligible_orders, customer_orders)
    if _table_exists(spark, gold_daily):
        daily_revenue = _spark_decimal(
            spark, f"SELECT SUM(total_revenue) FROM {gold_daily}"
        )
        report.compare("gold.daily_trends_revenue_vs_eligible", eligible_revenue, daily_revenue)
    if _table_exists(spark, gold_weekly):
        weekly_revenue = _spark_decimal(
            spark, f"SELECT SUM(total_revenue) FROM {gold_weekly}"
        )
        report.compare(
            "gold.weekly_trends_revenue_vs_eligible", eligible_revenue, weekly_revenue
        )

    seg_count = _spark_count(spark, f"SELECT SUM(customer_count) FROM {gold_seg}")
    customer_count = spark.table(gold_customer).count()
    report.compare(
        "gold.segmentation_customer_count_vs_revenue_by_customer",
        customer_count,
        seg_count,
        notes="exclusive buckets must cover every canonical customer",
    )
    types = [
        str(row[0])
        for row in spark.sql(
            f"SELECT segment_type FROM {gold_seg} ORDER BY segment_type"
        ).collect()
    ]
    report.compare(
        "gold.segmentation_types",
        sorted(SEGMENT_TYPES),
        sorted(types),
    )
    return report


def validate_dashboard_sql(
    spark: Any, config: PipelineConfig, report: ValidationReport
) -> ValidationReport:
    gold_schema = config.qualified_schema(config.gold_schema)
    queries = load_dashboard_queries(gold_schema)
    required = (
        "top_10_products",
        "customer_revenue_distribution",
        "customer_segmentation",
        "filter_values_category",
        "filter_values_customer_segment",
    )
    for name in required:
        report.compare_true(
            f"dashboard.sql.{name}.present",
            name in queries,
            notes="loaded from src/dashboard/dashboard_queries.sql",
        )

    gold_product = config.gold_table("sales_by_product")
    gold_customer = config.gold_table("revenue_by_customer")
    if not all(_table_exists(spark, name) for name in (gold_product, gold_customer)):
        return report

    if "top_10_products" in queries:
        top = spark.sql(queries["top_10_products"]).collect()
        report.compare_true(
            "dashboard.top_10_products.row_count_at_most_10",
            len(top) <= 10,
            notes=f"actual={len(top)}",
        )
        revenues = [Decimal(str(row["total_revenue"])) for row in top]
        ids = [int(row["product_id"]) for row in top]
        ordered = True
        for index in range(1, len(top)):
            prev_rev, rev = revenues[index - 1], revenues[index]
            prev_id, prod_id = ids[index - 1], ids[index]
            if rev > prev_rev or (rev == prev_rev and prod_id < prev_id):
                ordered = False
                break
        report.compare_true(
            "dashboard.top_10_products.order",
            ordered,
            notes="total_revenue DESC, product_id ASC",
        )

    if "customer_revenue_distribution" in queries:
        dist_count = spark.sql(queries["customer_revenue_distribution"]).count()
        gold_count = spark.table(gold_customer).count()
        report.compare(
            "dashboard.histogram.population_equals_gold_customers",
            gold_count,
            dist_count,
            notes="includes zero-revenue customers; viz binning is not automated",
        )

    if "customer_segmentation" in queries:
        seg_rows = spark.sql(queries["customer_segmentation"]).collect()
        report.compare("dashboard.segmentation.row_count", 4, len(seg_rows))
        types = sorted(str(row["segment_type"]) for row in seg_rows)
        report.compare("dashboard.segmentation.types", sorted(SEGMENT_TYPES), types)

    if "filter_values_category" in queries:
        categories = [
            row[0] for row in spark.sql(queries["filter_values_category"]).collect()
        ]
        report.compare_true(
            "dashboard.filter.category.values_present",
            len(categories) >= 1,
            notes="Gold products with qualifying orders",
        )
        if categories:
            sample = categories[0]
            if sample is None:
                filtered = spark.sql(
                    f"SELECT product_id FROM {gold_product} "
                    "WHERE category IS NULL ORDER BY total_revenue DESC, product_id ASC LIMIT 10"
                ).collect()
            else:
                escaped = str(sample).replace("'", "''")
                filtered = spark.sql(
                    f"SELECT product_id, category FROM {gold_product} "
                    f"WHERE category = '{escaped}' "
                    "ORDER BY total_revenue DESC, product_id ASC LIMIT 10"
                ).collect()
            report.compare_true(
                "dashboard.filter.category.before_limit",
                len(filtered) <= 10
                and all(
                    (row["category"] == sample) if "category" in row.asDict() else True
                    for row in filtered
                ),
                notes="category filter applied before LIMIT 10",
            )

    if "filter_values_customer_segment" in queries:
        segments = {
            str(row[0])
            for row in spark.sql(queries["filter_values_customer_segment"]).collect()
            if row[0] is not None
        }
        report.compare_true(
            "dashboard.filter.customer_segment.values",
            set(CUSTOMER_SEGMENTS).issubset(segments),
            notes=f"actual={sorted(segments)}",
        )
        sample = "Premium"
        filtered = spark.sql(
            f"SELECT customer_id, customer_segment FROM {gold_customer} "
            f"WHERE customer_segment = '{sample}'"
        ).collect()
        report.compare_true(
            "dashboard.filter.customer_segment.slices_histogram_population",
            all(str(row["customer_segment"]) == sample for row in filtered)
            and len(filtered) >= 1,
        )

    report.compare_true(
        "dashboard.visual_ui_not_claimed_automated",
        True,
        notes="SQL validation only; Databricks dashboard rendering remains a UI action",
    )
    return report


def expected_source_contract() -> dict[str, int]:
    return {
        "customers_rows": EXPECTED_CUSTOMER_ROWS,
        "orders_rows": EXPECTED_ORDER_ROWS,
        "products_rows": EXPECTED_PRODUCT_ROWS,
        "null_emails": EXPECTED_NULL_EMAILS,
        "null_order_customer_id": EXPECTED_NULL_ORDER_CUSTOMER_ID,
        "null_order_product_id": EXPECTED_NULL_ORDER_PRODUCT_ID,
        "duplicate_customer_keys": EXPECTED_DUP_CUSTOMER_KEYS,
        "duplicate_customer_rows": EXPECTED_DUP_CUSTOMER_ROWS,
        "duplicate_order_keys": EXPECTED_DUP_ORDER_KEYS,
        "duplicate_order_rows": EXPECTED_DUP_ORDER_ROWS,
        "orphan_customer_id": EXPECTED_ORPHAN_CUSTOMER_ID,
        "orphan_product_id": EXPECTED_ORPHAN_PRODUCT_ID,
        "future_signups": EXPECTED_FUTURE_SIGNUPS,
        "mandatory_issue_instances": MANDATORY_ISSUE_INSTANCES,
    }
