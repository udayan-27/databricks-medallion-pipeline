"""
Spark-free tests for the version-controlled Databricks dashboard definition.

These tests parse dashboards/DE_C1_E-Commerce_Sales_Dashboard.lvdash.json.
They do not require a Databricks workspace and do not publish or update
anything. They assert structure of the exported Lakeview definition, not
runtime tile rendering.
"""

from __future__ import annotations

import json
import re
import unittest
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACT_PATH = (
    REPO_ROOT / "dashboards" / "DE_C1_E-Commerce_Sales_Dashboard.lvdash.json"
)

REQUIRED_DATASETS = {
    "top_10_products": "workspace.gold.sales_by_product",
    "customer_revenue_distribution": "workspace.gold.revenue_by_customer",
    "customer_segmentation": "workspace.gold.customer_segmentation",
}

REQUIRED_VISUALIZATIONS = {
    "Top 10 Products by Revenue": "bar",
    "Customer Revenue Distribution": "histogram",
    "Customer Segmentation": "pie",
}

SECRET_PATTERNS = (
    (r"dapi[a-z0-9]{20,}", "Databricks PAT-like token"),
    (r"dkea[a-z0-9]{10,}", "Databricks OAuth secret-like token"),
    (r"bearer\s+[a-z0-9._\-]+", "Bearer token"),
    (r"begin (rsa |openssh |ec )?private key", "private key block"),
    (r"akia[0-9a-z]{8,}", "AWS access key"),
    (r"\"password\"\s*:", "password field"),
    (r"\"secret\"\s*:", "secret field"),
    (r"\"api[_-]?key\"\s*:", "api_key field"),
    (r"client_secret", "client_secret"),
    (r"access_token", "access_token"),
)

LOCAL_PATH_PATTERNS = (
    r"[c-z]:\\users\\",
    r"d:\\de c1",
    r"file:/",
)


def load_artifact() -> dict[str, Any]:
    text = ARTIFACT_PATH.read_text(encoding="utf-8")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise AssertionError("Dashboard artifact JSON root must be an object.")
    return data


def dataset_sql(dataset: dict[str, Any]) -> str:
    return "".join(dataset.get("queryLines") or [])


def iter_widgets(definition: dict[str, Any]) -> list[dict[str, Any]]:
    widgets: list[dict[str, Any]] = []
    for page in definition.get("pages") or []:
        for item in page.get("layout") or []:
            widget = item.get("widget")
            if isinstance(widget, dict):
                widgets.append(widget)
    return widgets


def widget_title(widget: dict[str, Any]) -> str:
    spec = widget.get("spec") or {}
    frame = spec.get("frame") or {}
    title = frame.get("title")
    if isinstance(title, dict):
        return str(title.get("value") or "")
    if isinstance(title, str):
        return title
    return ""


def widget_type(widget: dict[str, Any]) -> str:
    spec = widget.get("spec") or {}
    return str(spec.get("widgetType") or "")


def widget_dataset_names(widget: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for query in widget.get("queries") or []:
        dataset_name = (query.get("query") or {}).get("datasetName")
        if dataset_name:
            names.append(str(dataset_name))
    return names


def filter_field_names(widget: dict[str, Any]) -> list[str]:
    spec = widget.get("spec") or {}
    encodings = spec.get("encodings") or {}
    fields = encodings.get("fields") or []
    names: list[str] = []
    for field in fields:
        if isinstance(field, dict) and field.get("fieldName"):
            names.append(str(field["fieldName"]))
    return names


class TestDashboardArtifactFile(unittest.TestCase):
    def test_artifact_exists(self) -> None:
        self.assertTrue(ARTIFACT_PATH.is_file(), ARTIFACT_PATH)

    def test_artifact_is_valid_json(self) -> None:
        definition = load_artifact()
        self.assertIn("datasets", definition)
        self.assertIn("pages", definition)
        self.assertIsInstance(definition["datasets"], list)
        self.assertIsInstance(definition["pages"], list)
        self.assertGreaterEqual(len(definition["datasets"]), 3)
        self.assertGreaterEqual(len(definition["pages"]), 1)


class TestDashboardArtifactStructure(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.definition = load_artifact()
        cls.datasets = {
            str(item.get("displayName")): item
            for item in cls.definition["datasets"]
            if isinstance(item, dict)
        }
        cls.widgets = iter_widgets(cls.definition)
        cls.visualizations = [
            widget
            for widget in cls.widgets
            if widget_type(widget) in REQUIRED_VISUALIZATIONS.values()
        ]
        cls.filters = [
            widget
            for widget in cls.widgets
            if str(widget_type(widget)).startswith("filter")
        ]

    def test_required_datasets_and_gold_tables(self) -> None:
        self.assertEqual(set(self.datasets), set(REQUIRED_DATASETS))
        for display_name, gold_table in REQUIRED_DATASETS.items():
            sql = dataset_sql(self.datasets[display_name])
            self.assertIn(gold_table, sql, display_name)
            self.assertRegex(sql, rf"FROM\s+{re.escape(gold_table)}", display_name)

    def test_three_visualization_definitions(self) -> None:
        titles = {widget_title(widget): widget_type(widget) for widget in self.visualizations}
        self.assertEqual(titles, REQUIRED_VISUALIZATIONS)
        self.assertEqual(len(self.visualizations), 3)

    def test_visualization_dataset_bindings(self) -> None:
        name_by_id = {
            item["name"]: item["displayName"]
            for item in self.definition["datasets"]
        }
        expected = {
            "Top 10 Products by Revenue": "top_10_products",
            "Customer Revenue Distribution": "customer_revenue_distribution",
            "Customer Segmentation": "customer_segmentation",
        }
        for widget in self.visualizations:
            title = widget_title(widget)
            bound = [name_by_id[name] for name in widget_dataset_names(widget)]
            self.assertEqual(bound, [expected[title]], title)

    def test_category_filter_definition(self) -> None:
        matches = [
            widget
            for widget in self.filters
            if "category" in filter_field_names(widget)
        ]
        self.assertEqual(len(matches), 1)
        self.assertEqual(widget_type(matches[0]), "filter-single-select")
        self.assertIn("a1d9e50d", widget_dataset_names(matches[0]))

    def test_customer_segment_filter_definition(self) -> None:
        matches = [
            widget
            for widget in self.filters
            if "customer_segment" in filter_field_names(widget)
        ]
        self.assertEqual(len(matches), 1)
        self.assertEqual(widget_type(matches[0]), "filter-single-select")
        self.assertIn("60e0bea2", widget_dataset_names(matches[0]))

    def test_gold_only_query_sources(self) -> None:
        sql_blob = "\n".join(dataset_sql(item) for item in self.datasets.values())
        lowered = sql_blob.lower()
        self.assertIn("workspace.gold.", lowered)
        self.assertNotIn("bronze.", lowered)
        self.assertNotIn("silver.", lowered)
        self.assertNotIn("from gold.", lowered)
        for token in ("width_bucket", "case when", "1000.00"):
            self.assertNotIn(token, lowered)

    def test_no_obvious_secrets_or_local_paths(self) -> None:
        blob = ARTIFACT_PATH.read_text(encoding="utf-8").lower()
        for pattern, label in SECRET_PATTERNS:
            self.assertIsNone(re.search(pattern, blob, re.I), label)
        for pattern in LOCAL_PATH_PATTERNS:
            self.assertIsNone(re.search(pattern, blob, re.I), pattern)
        for token in ("dapi", "akia", "-----begin", "/workspace/users/"):
            self.assertNotIn(token, blob)


if __name__ == "__main__":
    unittest.main()
