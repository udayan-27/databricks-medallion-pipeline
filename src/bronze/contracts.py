"""
Spark-free Bronze contracts.

This module must stay importable without PySpark so unit tests can assert the
schema, CSV options, and table names in any environment.

Spark StructType construction lives in ingest_core.py and is a 1:1 mapping of
these declarations. Do not let schema inference drive the written table.
"""

from __future__ import annotations

from dataclasses import dataclass

# Table names inside the bronze schema (not bronze_customers in default).
# Qualified form: {catalog?}{bronze_schema}.customers
BRONZE_CUSTOMERS_TABLE = "customers"
BRONZE_ORDERS_TABLE = "orders"
BRONZE_PRODUCTS_TABLE = "products"
BRONZE_METADATA_TABLE = "ingest_metadata"

BRONZE_ENTITY_TABLES = (
    BRONZE_CUSTOMERS_TABLE,
    BRONZE_ORDERS_TABLE,
    BRONZE_PRODUCTS_TABLE,
)

SOURCE_FILENAMES = {
    BRONZE_CUSTOMERS_TABLE: "customers.csv",
    BRONZE_ORDERS_TABLE: "orders.csv",
    BRONZE_PRODUCTS_TABLE: "products.csv",
}

INGEST_ROW_ID_COLUMN = "_ingest_row_id"

# Declared types match data-model.md. DECIMAL is DECIMAL(18,2).
CUSTOMER_SOURCE_FIELDS: tuple[tuple[str, str], ...] = (
    ("customer_id", "INT"),
    ("customer_name", "STRING"),
    ("email", "STRING"),
    ("country", "STRING"),
    ("signup_date", "DATE"),
    ("customer_segment", "STRING"),
    ("lifetime_value", "DECIMAL(18,2)"),
)

ORDER_SOURCE_FIELDS: tuple[tuple[str, str], ...] = (
    ("order_id", "INT"),
    ("customer_id", "INT"),
    ("order_date", "DATE"),
    ("product_id", "INT"),
    ("quantity", "INT"),
    ("unit_price", "DECIMAL(18,2)"),
    ("total_amount", "DECIMAL(18,2)"),
    ("order_status", "STRING"),
    ("payment_date", "DATE"),
)

PRODUCT_SOURCE_FIELDS: tuple[tuple[str, str], ...] = (
    ("product_id", "INT"),
    ("product_name", "STRING"),
    ("category", "STRING"),
    ("price", "DECIMAL(18,2)"),
    ("cost", "DECIMAL(18,2)"),
    ("stock_quantity", "INT"),
    ("reorder_level", "INT"),
)

METADATA_FIELDS: tuple[tuple[str, str], ...] = (
    ("ingest_id", "STRING"),
    ("source_file", "STRING"),
    ("table_name", "STRING"),
    ("row_count", "BIGINT"),
    ("ingested_at", "TIMESTAMP"),
    ("status", "STRING"),
    ("error_message", "STRING"),
)

# Frozen Spark CSV options. Permissive parsing keeps malformed tokens as null
# rather than dropping the physical row. Do not add a drop-malformed mode.
CSV_READ_OPTIONS: dict[str, str] = {
    "header": "true",
    "mode": "PERMISSIVE",
    "dateFormat": "yyyy-MM-dd",
    "nullValue": "",
    "emptyValue": "",
    "encoding": "UTF-8",
    "inferSchema": "false",
    "enforceSchema": "true",
    "multiLine": "false",
    "sep": ",",
    "quote": '"',
    "escape": '"',
    # Do not trim: trimming enums/names would be a silent business cleanse.
    "ignoreLeadingWhiteSpace": "false",
    "ignoreTrailingWhiteSpace": "false",
}


@dataclass(frozen=True)
class EntityContract:
    table_name: str
    filename: str
    source_fields: tuple[tuple[str, str], ...]

    @property
    def source_columns(self) -> tuple[str, ...]:
        return tuple(name for name, _type in self.source_fields)


ENTITY_CONTRACTS: dict[str, EntityContract] = {
    BRONZE_CUSTOMERS_TABLE: EntityContract(
        table_name=BRONZE_CUSTOMERS_TABLE,
        filename=SOURCE_FILENAMES[BRONZE_CUSTOMERS_TABLE],
        source_fields=CUSTOMER_SOURCE_FIELDS,
    ),
    BRONZE_ORDERS_TABLE: EntityContract(
        table_name=BRONZE_ORDERS_TABLE,
        filename=SOURCE_FILENAMES[BRONZE_ORDERS_TABLE],
        source_fields=ORDER_SOURCE_FIELDS,
    ),
    BRONZE_PRODUCTS_TABLE: EntityContract(
        table_name=BRONZE_PRODUCTS_TABLE,
        filename=SOURCE_FILENAMES[BRONZE_PRODUCTS_TABLE],
        source_fields=PRODUCT_SOURCE_FIELDS,
    ),
}


def has_uri_scheme(path: str) -> bool:
    """True for DBFS/S3/ABFS/UC volume URIs; False for local OS paths (including ``D:\\``)."""
    if "://" in path:
        return True
    lowered = path.replace("\\", "/").lower()
    if lowered.startswith("dbfs:"):
        return True
    if lowered.startswith("/volumes/"):
        return True
    return False


def join_source_path(data_path: str, filename: str) -> str:
    if has_uri_scheme(data_path):
        return data_path.rstrip("/") + "/" + filename
    from pathlib import Path

    return str(Path(data_path) / filename)


def spark_input_path(source_file: str) -> str:
    """
    Path string Spark / Hadoop should read.

    Remote URIs (``s3://``, ``dbfs:``, ``abfss://``, ``/Volumes/...``) pass
    through unchanged so Databricks DBFS/Volume/S3 behaviour is untouched.

    Local OS paths are resolved to an absolute path using forward slashes.
    They are **not** converted with ``Path.as_uri()``. Hadoop 3.3.4 on Windows
    treats percent-encoded ``file:`` URIs (``DE%20C1``) as literal path
    segments and raises ``PATH_NOT_FOUND``. POSIX / Windows path strings
    with spaces are accepted by this runtime.
    """
    if has_uri_scheme(source_file):
        return source_file
    from pathlib import Path

    return Path(source_file).expanduser().resolve().as_posix()
