"""
Pipeline configuration for the DE C1 medallion jobs.

Paths, catalog, and schema names come from environment variables or CLI
arguments. Nothing in this module embeds a Windows user profile, a Databricks
workspace URL, a bucket name, or credentials.

Local default data path is ``<repo>/data``, resolved from this file's location
(the same pattern as the Stage 2 generator). On Databricks, set
``MEDALLION_DATA_PATH`` to a UC volume, DBFS, or S3 prefix that contains the
three CSV files.

Catalog default is unset (no ``catalog.`` prefix) so local Spark / Hive
metastore works without assuming a Unity Catalog name such as ``main``.
Set ``MEDALLION_CATALOG`` in the workspace if UC is in use.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

ENV_DATA_PATH = "MEDALLION_DATA_PATH"
ENV_CATALOG = "MEDALLION_CATALOG"
ENV_BRONZE_SCHEMA = "MEDALLION_BRONZE_SCHEMA"
ENV_SILVER_SCHEMA = "MEDALLION_SILVER_SCHEMA"
ENV_GOLD_SCHEMA = "MEDALLION_GOLD_SCHEMA"
ENV_TABLE_FORMAT = "MEDALLION_TABLE_FORMAT"
ENV_SPARK_APP_NAME = "MEDALLION_SPARK_APP_NAME"

DEFAULT_BRONZE_SCHEMA = "bronze"
DEFAULT_SILVER_SCHEMA = "silver"
DEFAULT_GOLD_SCHEMA = "gold"
DEFAULT_TABLE_FORMAT = "delta"
DEFAULT_SPARK_APP_NAME = "de-c1-bronze"


class ConfigError(ValueError):
    """Invalid configuration (bad identifier, empty required path, etc.)."""


def repo_root() -> Path:
    """Repository root: parent of ``src/``."""
    return Path(__file__).resolve().parents[1]


def default_data_path() -> str:
    return str(repo_root() / "data")


def require_sql_identifier(value: str, label: str) -> str:
    """Reject catalog/schema/table names that cannot be interpolated safely."""
    if not value or not _IDENT_RE.fullmatch(value):
        raise ConfigError(
            f"Invalid {label} {value!r}. Use a simple SQL identifier "
            "(letters, digits, underscore; must start with a letter or underscore). "
            "Do not put workspace URLs or file paths in catalog/schema names."
        )
    return value


def _optional_env(name: str) -> str | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped if stripped else None


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable snapshot of settings for one job invocation."""

    data_path: str
    catalog: str | None
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    table_format: str
    spark_app_name: str

    def qualified_schema(self, layer_schema: str) -> str:
        if self.catalog:
            return f"{self.catalog}.{layer_schema}"
        return layer_schema

    def qualified_table(self, layer_schema: str, table: str) -> str:
        return f"{self.qualified_schema(layer_schema)}.{table}"

    def bronze_table(self, table: str) -> str:
        return self.qualified_table(self.bronze_schema, table)


def load_config(
    *,
    data_path: str | None = None,
    catalog: str | None = None,
    bronze_schema: str | None = None,
    silver_schema: str | None = None,
    gold_schema: str | None = None,
    table_format: str | None = None,
    spark_app_name: str | None = None,
) -> PipelineConfig:
    """
    Build config from explicit arguments, then environment, then defaults.

    Explicit ``None`` means "not provided" and falls through. Pass an empty
    catalog string only via env; an explicit catalog of ``""`` is treated as unset.
    """
    resolved_data = data_path or _optional_env(ENV_DATA_PATH) or default_data_path()
    resolved_catalog = catalog if catalog is not None else _optional_env(ENV_CATALOG)
    if resolved_catalog == "":
        resolved_catalog = None
    resolved_bronze = bronze_schema or _optional_env(ENV_BRONZE_SCHEMA) or DEFAULT_BRONZE_SCHEMA
    resolved_silver = silver_schema or _optional_env(ENV_SILVER_SCHEMA) or DEFAULT_SILVER_SCHEMA
    resolved_gold = gold_schema or _optional_env(ENV_GOLD_SCHEMA) or DEFAULT_GOLD_SCHEMA
    resolved_format = (table_format or _optional_env(ENV_TABLE_FORMAT) or DEFAULT_TABLE_FORMAT).lower()
    resolved_app = spark_app_name or _optional_env(ENV_SPARK_APP_NAME) or DEFAULT_SPARK_APP_NAME

    if not resolved_data.strip():
        raise ConfigError(
            "data_path is empty. Set --data-path or MEDALLION_DATA_PATH to the "
            "directory that contains customers.csv, orders.csv, and products.csv."
        )
    if resolved_catalog is not None:
        require_sql_identifier(resolved_catalog, "catalog")
    require_sql_identifier(resolved_bronze, "bronze schema")
    require_sql_identifier(resolved_silver, "silver schema")
    require_sql_identifier(resolved_gold, "gold schema")
    require_sql_identifier(resolved_format, "table format")
    if resolved_format not in {"delta", "parquet"}:
        raise ConfigError(
            f"Unsupported table format {resolved_format!r}. "
            "Use 'delta' on Databricks, or 'parquet' for local Spark without Delta."
        )

    return PipelineConfig(
        data_path=resolved_data.rstrip("\\/"),
        catalog=resolved_catalog,
        bronze_schema=resolved_bronze,
        silver_schema=resolved_silver,
        gold_schema=resolved_gold,
        table_format=resolved_format,
        spark_app_name=resolved_app,
    )
