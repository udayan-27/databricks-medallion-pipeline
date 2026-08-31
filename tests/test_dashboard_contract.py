"""
Spark-free dashboard contract tests.

Always runnable. They assert Gold-only sources, Top-N / histogram / pie
contracts, no Bronze/Silver reimplementation, no SQL histogram buckets, and
that documented filters are real Gold fields.

Spark execution lives in tests/test_dashboard_queries.py.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_DIR = REPO_ROOT / "src" / "dashboard"
DASHBOARD_SQL_PATH = DASHBOARD_DIR / "dashboard_queries.sql"
DASHBOARD_GUIDE_PATH = DASHBOARD_DIR / "DASHBOARD_GUIDE.md"

DASHBOARD_QUERY_PREFIX = "-- DASHBOARD_QUERY:"

REQUIRED_QUERIES = (
    "top_10_products",
    "customer_revenue_distribution",
    "customer_segmentation",
)

SUPPORTING_QUERIES = (
    "filter_values_category",
    "filter_values_customer_segment",
)

GOLD_PRODUCT_COLUMNS = {
    "product_id",
    "product_name",
    "category",
    "total_orders",
    "total_revenue",
    "avg_order_value",
}
GOLD_CUSTOMER_COLUMNS = {
    "customer_id",
    "customer_name",
    "customer_segment",
    "total_orders",
    "total_revenue",
    "avg_order_value",
    "lifetime_value_actual",
}
GOLD_SEGMENT_COLUMNS = {
    "segment_type",
    "customer_count",
    "avg_revenue",
    "total_revenue",
}


def split_dashboard_queries(sql_text: str) -> list[tuple[str, str]]:
    """Split ``dashboard_queries.sql`` into ``(name, select_sql)`` parts."""
    parts: list[tuple[str, str]] = []
    current_name: str | None = None
    current_lines: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith(DASHBOARD_QUERY_PREFIX):
            if current_name is not None:
                statement = "\n".join(current_lines).strip().rstrip(";").strip()
                if not statement:
                    raise AssertionError(f"Dashboard query {current_name!r} is empty.")
                parts.append((current_name, statement))
            current_name = stripped[len(DASHBOARD_QUERY_PREFIX) :].strip()
            if not current_name:
                raise AssertionError("DASHBOARD_QUERY marker is missing a name.")
            current_lines = []
        elif current_name is not None:
            current_lines.append(line)
    if current_name is None:
        raise AssertionError("dashboard_queries.sql has no DASHBOARD_QUERY markers.")
    statement = "\n".join(current_lines).strip().rstrip(";").strip()
    if not statement:
        raise AssertionError(f"Dashboard query {current_name!r} is empty.")
    parts.append((current_name, statement))
    return parts


def render_dashboard_sql(sql_text: str, gold_schema: str) -> str:
    rendered = sql_text.replace("{gold_schema}", gold_schema)
    if "{gold_schema}" in rendered:
        raise AssertionError("Dashboard SQL still contains {gold_schema}.")
    return rendered


def executable_sql(sql_text: str) -> str:
    return "\n".join(
        line for line in sql_text.splitlines() if not line.lstrip().startswith("--")
    )


def dashboard_queries() -> dict[str, str]:
    return dict(split_dashboard_queries(DASHBOARD_SQL_PATH.read_text(encoding="utf-8")))


class TestDashboardFiles(unittest.TestCase):
    def test_required_files_exist(self) -> None:
        self.assertTrue(DASHBOARD_SQL_PATH.is_file())
        self.assertTrue(DASHBOARD_GUIDE_PATH.is_file())
        guide = DASHBOARD_GUIDE_PATH.read_text(encoding="utf-8").lower()
        self.assertIn("does **not** create a databricks dashboard", guide)
        self.assertIn("local validation", guide)
        self.assertIn("databricks sql validation", guide)
        self.assertIn("parquet", guide)
        self.assertIn("delta", guide)


class TestDashboardSqlContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sql = DASHBOARD_SQL_PATH.read_text(encoding="utf-8")
        cls.queries = dict(split_dashboard_queries(cls.sql))

    def test_required_and_supporting_query_names(self) -> None:
        for name in REQUIRED_QUERIES + SUPPORTING_QUERIES:
            self.assertIn(name, self.queries, name)
        self.assertEqual(
            list(self.queries),
            list(REQUIRED_QUERIES + SUPPORTING_QUERIES),
        )

    def test_queries_use_gold_placeholder_and_tables(self) -> None:
        top = self.queries["top_10_products"]
        dist = self.queries["customer_revenue_distribution"]
        seg = self.queries["customer_segmentation"]
        self.assertIn("{gold_schema}.sales_by_product", top)
        self.assertIn("{gold_schema}.revenue_by_customer", dist)
        self.assertIn("{gold_schema}.customer_segmentation", seg)
        self.assertIn("{gold_schema}.sales_by_product", self.queries["filter_values_category"])
        self.assertIn(
            "{gold_schema}.revenue_by_customer",
            self.queries["filter_values_customer_segment"],
        )

    def test_executable_sql_does_not_read_bronze_or_silver(self) -> None:
        executable = executable_sql(self.sql).lower()
        self.assertNotIn("bronze.", executable)
        self.assertNotIn("silver.", executable)
        self.assertNotIn("customers.csv", executable)
        self.assertNotIn("orders.csv", executable)
        self.assertNotIn("products.csv", executable)

    def test_no_hidden_gold_reimplementation(self) -> None:
        executable = executable_sql(self.sql)
        lowered = executable.lower()
        self.assertNotIn("order_status", executable)
        self.assertNotIn("quality_check_result", executable)
        self.assertNotIn("1000.00", executable)
        self.assertNotIn("lifetime_value AS lifetime_value_actual", executable)
        self.assertNotRegex(executable, r"SUM\s*\(\s*quantity\s*\)", re.I)
        self.assertNotIn("WHEN total_orders = 0 THEN 'Inactive'", executable)
        self.assertNotIn("THEN 'High-Value'", executable)
        self.assertNotIn("THEN 'Repeat'", executable)
        self.assertNotIn("THEN 'One-Time'", executable)
        self.assertNotIn("date_trunc", lowered)
        self.assertNotIn("bronze", lowered)
        self.assertNotIn("silver", lowered)

    def test_no_sql_histogram_buckets(self) -> None:
        executable = executable_sql(self.sql).upper()
        self.assertNotIn("WIDTH_BUCKET", executable)
        self.assertNotIn("NTILE", executable)
        self.assertNotIn("FLOOR(", executable)

    def test_top_10_contract(self) -> None:
        top = self.queries["top_10_products"]
        executable = executable_sql(top).upper()
        self.assertIn("PRODUCT_NAME", executable)
        self.assertIn("CATEGORY", executable)
        self.assertIn("TOTAL_REVENUE", executable)
        self.assertRegex(executable, r"LIMIT\s+10\b")
        self.assertIn("ORDER BY", executable)
        self.assertIn("TOTAL_REVENUE DESC", executable)
        self.assertIn("PRODUCT_ID ASC", executable)
        self.assertNotRegex(executable, r"LIMIT\s+11\b")
        self.assertNotIn("RANK(", executable)
        self.assertNotIn("ROW_NUMBER", executable)

    def test_histogram_population_is_gold_customer_grain(self) -> None:
        dist = self.queries["customer_revenue_distribution"]
        executable = executable_sql(dist)
        self.assertIn("customer_id", executable)
        self.assertIn("lifetime_value_actual", executable)
        self.assertIn("customer_segment", executable)
        self.assertNotIn("GROUP BY", executable.upper())
        self.assertNotIn("LIMIT", executable.upper())

    def test_segmentation_selects_gold_counts_only(self) -> None:
        seg = executable_sql(self.queries["customer_segmentation"])
        self.assertIn("segment_type", seg)
        self.assertIn("customer_count", seg)
        self.assertNotIn("avg_revenue", seg)
        self.assertNotIn("GROUP BY", seg.upper())
        self.assertNotIn("CASE", seg.upper())

    def test_header_documents_required_segment_types(self) -> None:
        header = self.sql.split("-- DASHBOARD_QUERY:")[0]
        for name in ("High-Value", "Repeat", "One-Time", "Inactive"):
            self.assertIn(name, self.sql)
        self.assertIn("mutually exclusive", self.sql.lower() + header.lower())

    def test_render_substitutes_gold_schema(self) -> None:
        rendered = render_dashboard_sql(self.queries["top_10_products"], "gold")
        self.assertIn("FROM gold.sales_by_product", rendered)
        self.assertNotIn("{gold_schema}", rendered)
        qualified = render_dashboard_sql(self.queries["top_10_products"], "dev.gold")
        self.assertIn("FROM dev.gold.sales_by_product", qualified)

    def test_no_environment_specific_paths_or_secrets(self) -> None:
        forbidden = (
            "/workspace/users/",
            "dbfs:/users/",
            "databricks.com",
            "akiai",
            r"d:\users",
            "udayan",
        )
        lowered = self.sql.lower() + DASHBOARD_GUIDE_PATH.read_text(encoding="utf-8").lower()
        for token in forbidden:
            self.assertNotIn(token, lowered, token)


class TestDashboardFilterContract(unittest.TestCase):
    def test_documented_filters_are_gold_fields(self) -> None:
        sql = DASHBOARD_SQL_PATH.read_text(encoding="utf-8")
        guide = DASHBOARD_GUIDE_PATH.read_text(encoding="utf-8")
        self.assertIn("category", GOLD_PRODUCT_COLUMNS)
        self.assertIn("customer_segment", GOLD_CUSTOMER_COLUMNS)
        self.assertIn("segment_type", GOLD_SEGMENT_COLUMNS)
        self.assertIn("category", sql)
        self.assertIn("customer_segment", sql)
        self.assertIn("applied BEFORE LIMIT", sql)
        self.assertIn("Tile 1 only", guide)
        self.assertIn("Tile 2 only", guide)
        lowered = guide.lower()
        self.assertIn("country", lowered)
        self.assertIn("not a gold", lowered)
        self.assertIn("order date", lowered)

    def test_guide_rejects_misleading_filters(self) -> None:
        guide = DASHBOARD_GUIDE_PATH.read_text(encoding="utf-8").lower()
        self.assertIn(
            "do not filter this tile by segment_type",
            DASHBOARD_SQL_PATH.read_text(encoding="utf-8").lower(),
        )
        self.assertIn("**before** `limit 10`", guide)
        self.assertIn("already-limited", guide)
        self.assertIn("**not** been implemented because it has not been observed", guide)

    def test_guide_does_not_claim_databricks_ui(self) -> None:
        guide = DASHBOARD_GUIDE_PATH.read_text(encoding="utf-8")
        lowered = guide.lower()
        self.assertIn("cursor did not generate", lowered)
        self.assertIn("manually in the databricks ui", lowered)
        self.assertIn("NOT rendered from this environment", DASHBOARD_SQL_PATH.read_text(encoding="utf-8"))
        self.assertNotIn("dashboard ui rendering: passed", lowered)
        self.assertNotIn("rendered successfully in databricks", lowered)


if __name__ == "__main__":
    unittest.main()
