"""
Create Gold tables by executing the Gold SQL files.

Aggregation logic lives in the ``.sql`` files. This module substitutes
configured schema names, runs each SELECT via Spark SQL, and overwrites
Gold tables. It must not replace those queries with undocumented PySpark
aggregations.

Local vs Databricks
-------------------
Databricks: run on a cluster with Silver tables already written (Delta).
Local: the same code can write parquet Gold tables when
``MEDALLION_TABLE_FORMAT=parquet``. Absence of Databricks is not a
successful Databricks run.

Rerun: full-refresh overwrite of Gold entity tables from current Silver.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_GOLD_DIR = Path(__file__).resolve().parent
_SRC_DIR = _GOLD_DIR.parent
_BRONZE_DIR = _SRC_DIR / "bronze"
for _path in (str(_SRC_DIR), str(_BRONZE_DIR), str(_GOLD_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from config import PipelineConfig, load_config  # noqa: E402
from ingest_core import get_spark_session  # noqa: E402

LOGGER = logging.getLogger("gold.create_tables")

GOLD_OUTPUT_PREFIX = "-- GOLD_OUTPUT:"

# Execution order: segmentation recomputes from Silver (not from
# revenue_by_customer), so 04 does not require 02's table. Order still
# matches the numbered files.
GOLD_SQL_FILES: tuple[str, ...] = (
    "01_sales_by_product.sql",
    "02_revenue_by_customer.sql",
    "03_daily_weekly_trends.sql",
    "04_customer_segmentation.sql",
)

GOLD_TABLES: tuple[str, ...] = (
    "sales_by_product",
    "revenue_by_customer",
    "daily_trends",
    "weekly_trends",
    "customer_segmentation",
)


class GoldError(RuntimeError):
    """Actionable Gold failure. Messages must not contain secrets."""


@dataclass(frozen=True)
class GoldTableResult:
    table_name: str
    qualified_table: str
    row_count: int


@dataclass(frozen=True)
class GoldBuildResult:
    tables: dict[str, Any]
    table_results: dict[str, GoldTableResult]


def gold_sql_path(filename: str) -> Path:
    path = _GOLD_DIR / filename
    if not path.is_file():
        raise GoldError(f"Gold SQL file is missing: {path}")
    return path


def render_gold_sql(sql_text: str, config: PipelineConfig) -> str:
    """Substitute schema placeholders. Does not interpolate unsanitized paths."""
    silver = config.qualified_schema(config.silver_schema)
    gold = config.qualified_schema(config.gold_schema)
    rendered = sql_text.replace("{silver_schema}", silver).replace("{gold_schema}", gold)
    if "{silver_schema}" in rendered or "{gold_schema}" in rendered:
        raise GoldError("Gold SQL still contains unsubstituted schema placeholders.")
    return rendered


def split_gold_outputs(sql_text: str) -> list[tuple[str, str]]:
    """
    Split a SQL file into ``(output_table, select_sql)`` parts.

    Each statement must be introduced by a line ``-- GOLD_OUTPUT: <table>``.
    Comments before the first marker are the file header and are not executed.
    """
    parts: list[tuple[str, str]] = []
    current_name: str | None = None
    current_lines: list[str] = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith(GOLD_OUTPUT_PREFIX):
            if current_name is not None:
                statement = "\n".join(current_lines).strip().rstrip(";").strip()
                if not statement:
                    raise GoldError(f"Gold output {current_name!r} has an empty SELECT.")
                parts.append((current_name, statement))
            current_name = stripped[len(GOLD_OUTPUT_PREFIX) :].strip()
            if not current_name:
                raise GoldError("GOLD_OUTPUT marker is missing a table name.")
            current_lines = []
        elif current_name is not None:
            current_lines.append(line)
    if current_name is None:
        raise GoldError("Gold SQL file has no GOLD_OUTPUT markers.")
    statement = "\n".join(current_lines).strip().rstrip(";").strip()
    if not statement:
        raise GoldError(f"Gold output {current_name!r} has an empty SELECT.")
    parts.append((current_name, statement))
    return parts


def load_gold_statements(config: PipelineConfig) -> list[tuple[str, str, str]]:
    """Return ``(sql_filename, table_name, select_sql)`` in execution order."""
    statements: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for filename in GOLD_SQL_FILES:
        rendered = render_gold_sql(gold_sql_path(filename).read_text(encoding="utf-8"), config)
        outputs = split_gold_outputs(rendered)
        for table_name, select_sql in outputs:
            if table_name not in GOLD_TABLES:
                raise GoldError(
                    f"{filename} declares unknown Gold table {table_name!r}. "
                    f"Expected one of {GOLD_TABLES}."
                )
            if table_name in seen:
                raise GoldError(f"Gold table {table_name!r} is declared more than once.")
            seen.add(table_name)
            statements.append((filename, table_name, select_sql))
    missing = [name for name in GOLD_TABLES if name not in seen]
    if missing:
        raise GoldError(f"Gold SQL files did not declare tables: {missing}")
    return statements


def _import_pyspark():
    try:
        from pyspark.sql.utils import AnalysisException
    except ImportError as exc:
        raise GoldError(
            "PySpark is not installed. Gold table creation requires Spark. "
            "On Databricks, run this on a cluster with an active SparkSession. "
            "For local validation install PySpark and JDK 11 or 17. "
            "Do not treat a missing Spark runtime as a successful Databricks run."
        ) from exc
    return AnalysisException


def ensure_gold_schema(spark: Any, config: PipelineConfig) -> None:
    qualified = config.qualified_schema(config.gold_schema)
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified}")
    except Exception as exc:
        raise GoldError(
            f"Cannot create schema {qualified}. On Databricks, set MEDALLION_CATALOG "
            "to an existing Unity Catalog catalog if you are using UC. "
            f"Original error: {exc.__class__.__name__}: {exc}"
        ) from exc


def _format_write_error(config: PipelineConfig, qualified: str, exc: BaseException) -> GoldError:
    text = str(exc).lower()
    if config.table_format == "delta" and "delta" in text:
        return GoldError(
            f"Failed to write {qualified} as delta. On Databricks use a cluster with "
            "Delta. For local Spark without Delta set MEDALLION_TABLE_FORMAT=parquet. "
            f"Original error: {exc.__class__.__name__}"
        )
    return GoldError(
        f"Failed to write {qualified} as {config.table_format}: "
        f"{exc.__class__.__name__}: {exc}"
    )


def write_gold_entity(
    spark: Any, dataframe: Any, config: PipelineConfig, table_name: str
) -> str:
    ensure_gold_schema(spark, config)
    qualified = config.gold_table(table_name)
    LOGGER.info("Overwriting Gold table %s (format=%s)", qualified, config.table_format)
    try:
        (
            dataframe.write.mode("overwrite")
            .format(config.table_format)
            .option("overwriteSchema", "true")
            .saveAsTable(qualified)
        )
    except Exception as exc:
        raise _format_write_error(config, qualified, exc) from exc
    return qualified


def build_gold_dataframes(
    spark: Any,
    config: PipelineConfig,
) -> GoldBuildResult:
    """Execute Gold SQL against Silver. Does not write."""
    tables: dict[str, Any] = {}
    table_results: dict[str, GoldTableResult] = {}
    for filename, table_name, select_sql in load_gold_statements(config):
        LOGGER.info("Running %s -> %s", filename, table_name)
        try:
            frame = spark.sql(select_sql)
        except Exception as exc:
            raise GoldError(
                f"Gold SQL failed for {table_name} ({filename}): "
                f"{exc.__class__.__name__}: {exc}"
            ) from exc
        row_count = frame.count()
        tables[table_name] = frame
        table_results[table_name] = GoldTableResult(
            table_name=table_name,
            qualified_table=config.gold_table(table_name),
            row_count=row_count,
        )
        LOGGER.info("gold %s rows=%s", table_name, row_count)
    return GoldBuildResult(tables=tables, table_results=table_results)


def create_gold_tables(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    write: bool = True,
) -> GoldBuildResult:
    """
    Orchestrate Gold SQL files and optionally write tables.

    Full-refresh overwrite of Gold entity tables. Does not mutate Bronze or Silver.
    """
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    result = build_gold_dataframes(session, cfg)
    if write:
        for table_name, frame in result.tables.items():
            write_gold_entity(session, frame, cfg, table_name)
        written: dict[str, Any] = {}
        table_results: dict[str, GoldTableResult] = {}
        for table_name in GOLD_TABLES:
            qualified = cfg.gold_table(table_name)
            written_frame = session.table(qualified)
            written[table_name] = written_frame
            table_results[table_name] = GoldTableResult(
                table_name=table_name,
                qualified_table=qualified,
                row_count=written_frame.count(),
            )
        return GoldBuildResult(tables=written, table_results=table_results)
    return result


def _cli() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        description="Create Gold tables by executing src/gold/*.sql against Silver."
    )
    parser.add_argument("--data-path", default=None)
    parser.add_argument("--catalog", default=None)
    parser.add_argument("--bronze-schema", default=None)
    parser.add_argument("--silver-schema", default=None)
    parser.add_argument("--gold-schema", default=None)
    parser.add_argument(
        "--table-format",
        default=None,
        choices=["delta", "parquet"],
        help="Must match the format used when Silver tables were written.",
    )
    args = parser.parse_args()
    config = load_config(
        data_path=args.data_path,
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
        table_format=args.table_format,
    )
    create_gold_tables(config=config)


if __name__ == "__main__":
    _cli()
