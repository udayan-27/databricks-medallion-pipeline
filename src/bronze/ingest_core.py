"""
Shared Bronze ingest logic (PySpark).

Design constraints (frozen):
- Explicit schema from contracts.py / data-model.md; inference does not drive writes.
- PERMISSIVE CSV mode; physical rows are never dropped for parse failures.
- Source column values are not cleaned, filled, deduplicated, or repaired.
- ``_ingest_row_id`` is ingest lineage, not a business key.
- Entity tables are full-refresh overwrite; ingest_metadata is append-only.

Local vs Databricks
-------------------
Databricks: run these modules on a cluster with an active SparkSession. Set
MEDALLION_DATA_PATH to a volume/DBFS/S3 directory that contains the three CSVs.
Set MEDALLION_CATALOG if Unity Catalog is used. Default table format is delta.

Local: Spark is optional. If PySpark + a JDK are installed, the same code can
write parquet tables (MEDALLION_TABLE_FORMAT=parquet) into a local warehouse.
On Windows, locally created SparkSessions use a Java FileSystem that avoids
winutils/hadoop.dll; Databricks keeps using the cluster session unchanged.
Absence of Spark does **not** mean Databricks ingest succeeded.

Rerun behaviour
---------------
Given the same CSVs: source columns on entity tables match the files again
(overwrite). ``_ingest_row_id`` is regenerated per execution and is not stable.
ingest_metadata grows by one SUCCESS (or FAILED) row per entity per run.
Do not run overlapping ingest_all jobs.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

_BRONZE_DIR = Path(__file__).resolve().parent
_SRC_DIR = _BRONZE_DIR.parent
for _path in (str(_SRC_DIR), str(_BRONZE_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from config import PipelineConfig, load_config  # noqa: E402
from contracts import (  # noqa: E402
    BRONZE_ENTITY_TABLES,
    BRONZE_METADATA_TABLE,
    CSV_READ_OPTIONS,
    ENTITY_CONTRACTS,
    INGEST_ROW_ID_COLUMN,
    METADATA_FIELDS,
    EntityContract,
    has_uri_scheme,
    join_source_path,
    spark_input_path,
)
from spark_local import apply_local_spark_config  # noqa: E402

LOGGER = logging.getLogger("bronze.ingest")

STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"


class BronzeIngestError(RuntimeError):
    """Actionable ingest failure. Messages must not contain secrets."""


@dataclass(frozen=True)
class IngestResult:
    table_name: str
    source_file: str
    qualified_table: str
    row_count: int
    ingest_id: str
    ingested_at: datetime
    status: str


def new_ingest_id() -> str:
    """Per-execution run id. Not derived from source bytes."""
    return str(uuid.uuid4())


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _import_pyspark():
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql import types as T
        from pyspark.sql.utils import AnalysisException
    except ImportError as exc:
        raise BronzeIngestError(
            "PySpark is not installed. Bronze ingest requires Spark. "
            "On Databricks, run this on a cluster with an active SparkSession. "
            "For local validation install PySpark and JDK 11 or 17, then set "
            "MEDALLION_TABLE_FORMAT=parquet if Delta is unavailable. "
            "Do not treat a missing Spark runtime as a successful Databricks run."
        ) from exc
    return SparkSession, F, T, AnalysisException


def _spark_type(type_name: str, T: Any) -> Any:
    mapping = {
        "INT": T.IntegerType(),
        "STRING": T.StringType(),
        "DATE": T.DateType(),
        "DECIMAL(18,2)": T.DecimalType(18, 2),
        "BIGINT": T.LongType(),
        "TIMESTAMP": T.TimestampType(),
    }
    try:
        return mapping[type_name]
    except KeyError as exc:
        raise BronzeIngestError(f"Unsupported contract type {type_name!r}.") from exc


def build_source_schema(contract: EntityContract) -> Any:
    """Explicit Spark schema for source columns. All fields nullable (defects must land)."""
    _SparkSession, _F, T, _AnalysisException = _import_pyspark()
    return T.StructType(
        [
            T.StructField(name, _spark_type(type_name, T), nullable=True)
            for name, type_name in contract.source_fields
        ]
    )


def build_metadata_schema() -> Any:
    _SparkSession, _F, T, _AnalysisException = _import_pyspark()
    nullable = {"error_message"}
    return T.StructType(
        [
            T.StructField(name, _spark_type(type_name, T), nullable=(name in nullable))
            for name, type_name in METADATA_FIELDS
        ]
    )


def get_spark_session(app_name: str = "de-c1-bronze") -> Any:
    SparkSession, _F, _T, _AnalysisException = _import_pyspark()
    existing = SparkSession.getActiveSession()
    if existing is not None:
        return existing
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
    )
    builder = apply_local_spark_config(builder)
    return builder.getOrCreate()


def add_ingest_row_id(dataframe: Any) -> Any:
    """
    Add ``_ingest_row_id`` (BIGINT).

    Generation: ``monotonically_increasing_id()`` — unique within this write,
    **not** deterministic across reruns or clusters.

    Why per-execution rather than a hash of business columns:
    Stage 2 injects exact duplicate rows (identical business keys *and* identical
    payloads). A content hash would collide and hide those duplicates. The
    identifier exists so Silver can join quality-module outputs without using
    duplicated ``customer_id`` / ``order_id`` as the join key.

    It must not replace the business primary key and must not be used to drop
    or collapse duplicates.
    """
    _SparkSession, F, _T, _AnalysisException = _import_pyspark()
    if INGEST_ROW_ID_COLUMN in dataframe.columns:
        raise BronzeIngestError(
            f"Source data already contains {INGEST_ROW_ID_COLUMN}; refusing to overwrite lineage."
        )
    return dataframe.withColumn(INGEST_ROW_ID_COLUMN, F.monotonically_increasing_id())


def _read_local_header(path: Path) -> list[str]:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle)
            try:
                header = next(reader)
            except StopIteration as exc:
                raise BronzeIngestError(f"Source file has no header row: {path}") from exc
    except OSError as exc:
        raise BronzeIngestError(
            f"Source file is unreadable: {path} ({exc.__class__.__name__}: {exc})"
        ) from exc
    return header


def _normalize_header(header: Sequence[str]) -> list[str]:
    return [col.replace("\ufeff", "") for col in header]


def validate_header(actual: Sequence[str], contract: EntityContract, source_file: str) -> None:
    expected = list(contract.source_columns)
    cleaned = _normalize_header(actual)
    if list(actual) != cleaned:
        raise BronzeIngestError(
            f"UTF-8 BOM detected in header of {source_file}. "
            "Re-save the CSV as UTF-8 without BOM (Stage 2 files have no BOM)."
        )
    if cleaned != expected:
        extra = [c for c in cleaned if c not in expected]
        missing = [c for c in expected if c not in cleaned]
        raise BronzeIngestError(
            f"Header mismatch for {contract.table_name} at {source_file}.\n"
            f"  Expected ({len(expected)}): {expected}\n"
            f"  Actual   ({len(cleaned)}): {cleaned}\n"
            f"  Missing: {missing}\n"
            f"  Extra: {extra}\n"
            "Bronze will not guess column order. Fix the source or the contract."
        )


def assert_source_exists(source_file: str) -> None:
    """Local filesystem existence/emptiness checks. Remote URIs are checked at Spark read."""
    if has_uri_scheme(source_file):
        return
    path = Path(source_file)
    if not path.exists():
        raise BronzeIngestError(
            f"Source file not found: {source_file}. "
            "Set --data-path or MEDALLION_DATA_PATH to the directory that contains "
            "customers.csv, orders.csv, and products.csv."
        )
    if not path.is_file():
        raise BronzeIngestError(f"Source path is not a file: {source_file}")
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise BronzeIngestError(f"Source file is unreadable: {source_file} ({exc})") from exc
    if size == 0:
        raise BronzeIngestError(
            f"Source file is empty: {source_file}. Refusing to write an empty Bronze table."
        )


def read_header(spark: Any, source_file: str) -> list[str]:
    assert_source_exists(source_file)
    if not has_uri_scheme(source_file):
        return _read_local_header(Path(source_file))
    _SparkSession, _F, _T, AnalysisException = _import_pyspark()
    spark_path = spark_input_path(source_file)
    try:
        header_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .csv(spark_path)
        )
        return list(header_df.columns)
    except AnalysisException as exc:
        raise BronzeIngestError(
            f"Cannot read source header at {source_file}: {exc}"
        ) from exc
    except Exception as exc:  # Spark/Hadoop missing-file errors vary by runtime
        raise BronzeIngestError(
            f"Cannot read source at {source_file} ({exc.__class__.__name__}: {exc})"
        ) from exc


def read_source_csv(spark: Any, source_file: str, contract: EntityContract) -> Any:
    """
    Read one CSV with the declared schema.

    CSV options (see contracts.CSV_READ_OPTIONS):
    - header=true: first row is names, not data
    - mode=PERMISSIVE: unparsable field values become null; rows are kept
    - dateFormat=yyyy-MM-dd: matches the generator
    - nullValue/emptyValue empty string: Stage 2 writes NULL as an empty field
    - inferSchema=false: declared schema only
    - ignoreLeading/TrailingWhiteSpace=false: no silent trim
    """
    _SparkSession, _F, _T, AnalysisException = _import_pyspark()
    header = read_header(spark, source_file)
    validate_header(header, contract, source_file)
    spark_path = spark_input_path(source_file)
    schema = build_source_schema(contract)
    reader = spark.read.format("csv").schema(schema)
    for key, value in CSV_READ_OPTIONS.items():
        reader = reader.option(key, value)
    try:
        dataframe = reader.load(spark_path)
    except AnalysisException as exc:
        raise BronzeIngestError(
            f"Spark failed to read {source_file} with the declared {contract.table_name} schema: {exc}"
        ) from exc
    except Exception as exc:
        raise BronzeIngestError(
            f"Spark failed to read {source_file} ({exc.__class__.__name__}: {exc})"
        ) from exc
    actual_cols = list(dataframe.columns)
    expected_cols = list(contract.source_columns)
    if actual_cols != expected_cols:
        raise BronzeIngestError(
            f"Spark produced unexpected columns for {contract.table_name}: {actual_cols} "
            f"(expected {expected_cols})"
        )
    LOGGER.info(
        "Read %s with explicit schema %s (inference was not used to type the table)",
        source_file,
        [(f.name, f.dataType.simpleString()) for f in schema.fields],
    )
    return dataframe


def _format_write_error(config: PipelineConfig, qualified: str, exc: BaseException) -> BronzeIngestError:
    hint = ""
    text = str(exc).lower()
    if config.table_format == "delta" and "delta" in text:
        hint = (
            " On Databricks, Delta should be available on the cluster. "
            "For local Spark without delta-spark, pass --table-format parquet "
            "or set MEDALLION_TABLE_FORMAT=parquet."
        )
    return BronzeIngestError(
        f"Failed to write {qualified} as {config.table_format}: "
        f"{exc.__class__.__name__}: {exc}.{hint}"
    )


def ensure_bronze_schema(spark: Any, config: PipelineConfig) -> None:
    qualified = config.qualified_schema(config.bronze_schema)
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {qualified}")
    except Exception as exc:
        raise BronzeIngestError(
            f"Cannot create schema {qualified}. On Databricks, set MEDALLION_CATALOG "
            "to an existing Unity Catalog catalog if you are using UC. "
            f"Original error: {exc.__class__.__name__}: {exc}"
        ) from exc


def write_entity_table(spark: Any, dataframe: Any, config: PipelineConfig, table_name: str) -> str:
    ensure_bronze_schema(spark, config)
    qualified = config.bronze_table(table_name)
    LOGGER.info("Overwriting Bronze entity table %s (format=%s)", qualified, config.table_format)
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


def write_metadata_row(
    spark: Any,
    config: PipelineConfig,
    *,
    ingest_id: str,
    source_file: str,
    table_name: str,
    row_count: int,
    ingested_at: datetime,
    status: str,
    error_message: str | None,
) -> None:
    ensure_bronze_schema(spark, config)
    schema = build_metadata_schema()
    ingested_at_naive = ingested_at.astimezone(timezone.utc).replace(tzinfo=None)
    record = [
        (
            ingest_id,
            source_file,
            table_name,
            int(row_count),
            ingested_at_naive,
            status,
            error_message,
        )
    ]
    meta_df = spark.createDataFrame(record, schema=schema)
    qualified = config.bronze_table(BRONZE_METADATA_TABLE)
    LOGGER.info(
        "Appending ingest metadata: table=%s status=%s row_count=%s ingest_id=%s",
        table_name,
        status,
        row_count,
        ingest_id,
    )
    try:
        (
            meta_df.write.mode("append")
            .format(config.table_format)
            .saveAsTable(qualified)
        )
    except Exception as exc:
        raise _format_write_error(config, qualified, exc) from exc


def _record_failure_metadata(
    spark: Any,
    config: PipelineConfig,
    *,
    ingest_id: str,
    source_file: str,
    table_name: str,
    ingested_at: datetime,
    error: BaseException,
    row_count: int = 0,
) -> None:
    message = f"{error.__class__.__name__}: {error}"
    if len(message) > 2000:
        message = message[:2000] + "…"
    try:
        write_metadata_row(
            spark,
            config,
            ingest_id=ingest_id,
            source_file=source_file,
            table_name=table_name,
            row_count=int(row_count),
            ingested_at=ingested_at,
            status=STATUS_FAILED,
            error_message=message,
        )
    except Exception:
        LOGGER.exception(
            "Failed to write FAILED ingest metadata for %s; re-raising original error",
            table_name,
        )


def ingest_entity(
    spark: Any,
    config: PipelineConfig,
    table_name: str,
    *,
    ingest_id: str | None = None,
    ingested_at: datetime | None = None,
    record_failed_metadata: bool = True,
) -> IngestResult:
    """
    Ingest one CSV into Bronze. Does not drop, dedupe, fill, or repair rows.

    Forbidden operations are simply not invoked: no drop, no dropDuplicates,
    no na.drop, no quality filters, no FK repair.
    """
    if table_name not in ENTITY_CONTRACTS:
        raise BronzeIngestError(f"Unknown Bronze entity {table_name!r}.")
    contract = ENTITY_CONTRACTS[table_name]
    run_id = ingest_id or new_ingest_id()
    run_time = ingested_at or utc_now()
    source_file = join_source_path(config.data_path, contract.filename)
    LOGGER.info(
        "Starting Bronze ingest table=%s source=%s ingest_id=%s",
        table_name,
        source_file,
        run_id,
    )
    observed_count = 0
    try:
        source_df = read_source_csv(spark, source_file, contract)
        bronze_df = add_ingest_row_id(source_df)
        row_count = bronze_df.count()
        observed_count = row_count
        if row_count == 0:
            raise BronzeIngestError(
                f"Source {source_file} produced 0 data rows for {table_name}. "
                "Header-only or empty files are not a successful ingest."
            )
        qualified = write_entity_table(spark, bronze_df, config, table_name)
        written = spark.table(qualified).count()
        observed_count = written
        if written != row_count:
            raise BronzeIngestError(
                f"Row-count mismatch writing {qualified}: "
                f"read {row_count} rows, table has {written}. Refusing SUCCESS metadata."
            )
        write_metadata_row(
            spark,
            config,
            ingest_id=run_id,
            source_file=source_file,
            table_name=table_name,
            row_count=written,
            ingested_at=run_time,
            status=STATUS_SUCCESS,
            error_message=None,
        )
        LOGGER.info(
            "Bronze ingest SUCCESS table=%s rows=%s qualified=%s",
            table_name,
            written,
            qualified,
        )
        return IngestResult(
            table_name=table_name,
            source_file=source_file,
            qualified_table=qualified,
            row_count=written,
            ingest_id=run_id,
            ingested_at=run_time,
            status=STATUS_SUCCESS,
        )
    except Exception as exc:
        if record_failed_metadata:
            _record_failure_metadata(
                spark,
                config,
                ingest_id=run_id,
                source_file=source_file,
                table_name=table_name,
                ingested_at=run_time,
                error=exc,
                row_count=observed_count,
            )
        if isinstance(exc, BronzeIngestError):
            raise
        raise BronzeIngestError(
            f"Bronze ingest failed for {table_name} from {source_file}: "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc


def preflight_all_sources(spark: Any, config: PipelineConfig) -> None:
    """Validate all three sources before writing any entity table."""
    errors: list[str] = []
    for table_name in BRONZE_ENTITY_TABLES:
        contract = ENTITY_CONTRACTS[table_name]
        source_file = join_source_path(config.data_path, contract.filename)
        try:
            header = read_header(spark, source_file)
            validate_header(header, contract, source_file)
        except BronzeIngestError as exc:
            errors.append(str(exc))
    if errors:
        raise BronzeIngestError(
            "Preflight failed; no Bronze entity tables were overwritten.\n"
            + "\n".join(f"- {item}" for item in errors)
        )


def ingest_customers(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    ingest_id: str | None = None,
    ingested_at: datetime | None = None,
) -> IngestResult:
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    return ingest_entity(
        session, cfg, "customers", ingest_id=ingest_id, ingested_at=ingested_at
    )


def ingest_orders(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    ingest_id: str | None = None,
    ingested_at: datetime | None = None,
) -> IngestResult:
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    return ingest_entity(
        session, cfg, "orders", ingest_id=ingest_id, ingested_at=ingested_at
    )


def ingest_products(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    ingest_id: str | None = None,
    ingested_at: datetime | None = None,
) -> IngestResult:
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    return ingest_entity(
        session, cfg, "products", ingest_id=ingest_id, ingested_at=ingested_at
    )


def ingest_all(
    spark: Any | None = None,
    config: PipelineConfig | None = None,
    *,
    ingest_id: str | None = None,
    ingested_at: datetime | None = None,
) -> list[IngestResult]:
    """
    Orchestrate customers, orders, and products without duplicating ingest logic.

    All three share one ingest_id and ingested_at. Sources are preflighted so a
    missing orders file does not leave customers overwritten from this run and
    orders stale from a previous run. After preflight, entities are ingested
    sequentially; a mid-write Spark failure fail-fasts (later entities skipped).
    Rerun the full job after fixing the error.
    """
    cfg = config or load_config()
    session = spark or get_spark_session(cfg.spark_app_name)
    run_id = ingest_id or new_ingest_id()
    run_time = ingested_at or utc_now()
    LOGGER.info("Bronze ingest_all start ingest_id=%s data_path=%s", run_id, cfg.data_path)
    preflight_all_sources(session, cfg)
    results: list[IngestResult] = []
    for table_name in BRONZE_ENTITY_TABLES:
        results.append(
            ingest_entity(
                session,
                cfg,
                table_name,
                ingest_id=run_id,
                ingested_at=run_time,
            )
        )
    LOGGER.info(
        "Bronze ingest_all SUCCESS ingest_id=%s counts=%s",
        run_id,
        {item.table_name: item.row_count for item in results},
    )
    return results


def add_config_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    parser.add_argument(
        "--data-path",
        default=None,
        help="Directory containing customers.csv, orders.csv, products.csv "
        "(default: MEDALLION_DATA_PATH or <repo>/data).",
    )
    parser.add_argument(
        "--catalog",
        default=None,
        help="Optional Unity Catalog name. Omit for local HMS. Env: MEDALLION_CATALOG.",
    )
    parser.add_argument(
        "--bronze-schema",
        default=None,
        help="Schema/database for Bronze tables (default: bronze).",
    )
    parser.add_argument(
        "--table-format",
        default=None,
        choices=["delta", "parquet"],
        help="delta on Databricks; parquet for local Spark without Delta.",
    )
    return parser


def config_from_args(args: argparse.Namespace) -> PipelineConfig:
    return load_config(
        data_path=args.data_path,
        catalog=args.catalog,
        bronze_schema=args.bronze_schema,
        table_format=args.table_format,
    )


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def cli_main(target: str) -> None:
    configure_logging()
    parser = argparse.ArgumentParser(description=f"Bronze ingest: {target}")
    add_config_arguments(parser)
    args = parser.parse_args()
    config = config_from_args(args)
    if target == "all":
        ingest_all(config=config)
    elif target == "customers":
        ingest_customers(config=config)
    elif target == "orders":
        ingest_orders(config=config)
    elif target == "products":
        ingest_products(config=config)
    else:
        raise BronzeIngestError(f"Unknown CLI target {target!r}")
