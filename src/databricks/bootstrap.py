"""
Databricks environment bootstrap: schemas, volume, source-file copy, optional reset.

Source of truth for transformations remains ``src/bronze``, ``src/silver``,
and ``src/gold``. This module only prepares Unity Catalog objects and copies
the version-controlled CSVs from the Git folder into a UC Volume.

Source-data transfer (Databricks Free Edition / Serverless)
----------------------------------------------------------
Do not regenerate or edit the Stage 2 CSVs. Copy the Git-folder files.

Supported mechanisms, in the order this module tries them:

1. **POSIX / FUSE Python I/O** (primary). Official Databricks file docs:
   workspace files (including Git folders) are readable via OSS Python
   (``os.listdir('/Workspace/...')``, ``open()``, ``pathlib``). UC Volumes are
   writable via OSS Python (``os.listdir('/Volumes/...')``) and
   ``shutil.copyfile`` onto a volume path. Serverless environment 2+ supports
   programmatic workspace-file access. Git-folder paths are derived from this
   repository layout (``<repo>/data``), not from a hard-coded user folder.
2. **Driver temp + volume write.** If a direct Workspace→Volume write fails,
   copy bytes to local driver temp then ``shutil.copyfile`` onto the volume
   (the documented pattern for writing files onto volumes).
3. **``dbutils.fs.cp``** with a ``file:/`` workspace URI. Databricks utilities
   and Spark require the ``file:/`` scheme for workspace files. Volume
   destinations use ``/Volumes/<catalog>/<schema>/<volume>/...``.

Databricks CLI and REST Files APIs are not used here: they need extra
credentials that must not live in this repository, and they are not the
serverless notebook/job default.

If every programmatic copy fails, this module raises with the exact remaining
manual action rather than pretending the copy succeeded.
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

_THIS_DIR = Path(__file__).resolve().parent
_SRC_DIR = _THIS_DIR.parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from config import (  # noqa: E402
    DEFAULT_BRONZE_SCHEMA,
    DEFAULT_GOLD_SCHEMA,
    DEFAULT_SILVER_SCHEMA,
    DEFAULT_TABLE_FORMAT,
    PipelineConfig,
    load_config,
    repo_root,
    require_sql_identifier,
)

LOGGER = logging.getLogger("databricks.bootstrap")

SOURCE_FILENAMES: tuple[str, ...] = ("customers.csv", "orders.csv", "products.csv")

DEFAULT_DATABRICKS_CATALOG = "workspace"
DEFAULT_SOURCE_SCHEMA = "de_c1"
DEFAULT_SOURCE_VOLUME = "source_data"
DEFAULT_DATABRICKS_APP_NAME = "DE_C1_Databricks"

ENV_SOURCE_SCHEMA = "MEDALLION_SOURCE_SCHEMA"
ENV_SOURCE_VOLUME = "MEDALLION_SOURCE_VOLUME"
ENV_GIT_DATA_PATH = "MEDALLION_GIT_DATA_PATH"

RESET_LAYER_SCHEMAS: tuple[str, ...] = ("bronze", "silver", "gold")
RESET_BRONZE_TABLES: tuple[str, ...] = (
    "customers",
    "orders",
    "products",
    "ingest_metadata",
)
RESET_SILVER_TABLES: tuple[str, ...] = (
    "customers",
    "orders",
    "products",
    "quality_metrics",
)
RESET_GOLD_TABLES: tuple[str, ...] = (
    "sales_by_product",
    "revenue_by_customer",
    "daily_trends",
    "weekly_trends",
    "customer_segmentation",
)


class DatabricksBootstrapError(RuntimeError):
    """Actionable bootstrap failure. Messages must not contain secrets."""


@dataclass(frozen=True)
class DatabricksRuntimeConfig:
    """Unity Catalog + Git-folder settings for one Databricks run."""

    catalog: str
    source_schema: str
    source_volume: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    table_format: str
    spark_app_name: str
    git_data_path: str

    @property
    def volume_path(self) -> str:
        return posix_volume_path(self.catalog, self.source_schema, self.source_volume)

    @property
    def source_schema_qualified(self) -> str:
        return f"{self.catalog}.{self.source_schema}"

    @property
    def volume_qualified(self) -> str:
        return f"{self.catalog}.{self.source_schema}.{self.source_volume}"

    def pipeline_config(self) -> PipelineConfig:
        return load_config(
            data_path=self.volume_path,
            catalog=self.catalog,
            bronze_schema=self.bronze_schema,
            silver_schema=self.silver_schema,
            gold_schema=self.gold_schema,
            table_format=self.table_format,
            spark_app_name=self.spark_app_name,
        )


@dataclass(frozen=True)
class CopiedSourceFile:
    filename: str
    source_path: str
    destination_path: str
    bytes_copied: int
    sha256: str
    mechanism: str


@dataclass(frozen=True)
class ResetPlan:
    catalog: str
    schemas: tuple[str, ...]
    tables: tuple[str, ...]
    volume_files: tuple[str, ...]
    descriptions: tuple[str, ...]


def posix_volume_path(catalog: str, schema: str, volume: str) -> str:
    require_sql_identifier(catalog, "catalog")
    require_sql_identifier(schema, "source schema")
    require_sql_identifier(volume, "source volume")
    return f"/Volumes/{catalog}/{schema}/{volume}"


def default_git_data_path() -> Path:
    """Version-controlled CSVs. Derived from repo layout, not a user folder."""
    return repo_root() / "data"


def _optional_env(name: str) -> str | None:
    raw = os.environ.get(name)
    if raw is None:
        return None
    stripped = raw.strip()
    return stripped if stripped else None


def load_databricks_runtime_config(
    *,
    catalog: str | None = None,
    source_schema: str | None = None,
    source_volume: str | None = None,
    bronze_schema: str | None = None,
    silver_schema: str | None = None,
    gold_schema: str | None = None,
    table_format: str | None = None,
    spark_app_name: str | None = None,
    git_data_path: str | None = None,
) -> DatabricksRuntimeConfig:
    resolved_catalog = (
        catalog
        or _optional_env("MEDALLION_CATALOG")
        or DEFAULT_DATABRICKS_CATALOG
    )
    resolved_source_schema = (
        source_schema or _optional_env(ENV_SOURCE_SCHEMA) or DEFAULT_SOURCE_SCHEMA
    )
    resolved_volume = (
        source_volume or _optional_env(ENV_SOURCE_VOLUME) or DEFAULT_SOURCE_VOLUME
    )
    resolved_git = (
        git_data_path
        or _optional_env(ENV_GIT_DATA_PATH)
        or str(default_git_data_path())
    )
    require_sql_identifier(resolved_catalog, "catalog")
    require_sql_identifier(resolved_source_schema, "source schema")
    require_sql_identifier(resolved_volume, "source volume")
    pipeline = load_config(
        data_path=posix_volume_path(
            resolved_catalog, resolved_source_schema, resolved_volume
        ),
        catalog=resolved_catalog,
        bronze_schema=bronze_schema or DEFAULT_BRONZE_SCHEMA,
        silver_schema=silver_schema or DEFAULT_SILVER_SCHEMA,
        gold_schema=gold_schema or DEFAULT_GOLD_SCHEMA,
        table_format=table_format or DEFAULT_TABLE_FORMAT,
        spark_app_name=spark_app_name or DEFAULT_DATABRICKS_APP_NAME,
    )
    return DatabricksRuntimeConfig(
        catalog=pipeline.catalog or resolved_catalog,
        source_schema=resolved_source_schema,
        source_volume=resolved_volume,
        bronze_schema=pipeline.bronze_schema,
        silver_schema=pipeline.silver_schema,
        gold_schema=pipeline.gold_schema,
        table_format=pipeline.table_format,
        spark_app_name=pipeline.spark_app_name,
        git_data_path=str(Path(resolved_git)),
    )


def resource_policy() -> dict[str, tuple[str, ...]]:
    """
    What a normal run creates, overwrites, appends, or leaves alone.

    RESET is a separate explicit mode and is not part of normal execution.
    """
    return {
        "safe_create_if_not_exists": (
            "schema {catalog}.bronze",
            "schema {catalog}.silver",
            "schema {catalog}.gold",
            "schema {catalog}.de_c1",
            "volume {catalog}.de_c1.source_data",
        ),
        "overwritten_by_pipeline": (
            "{catalog}.bronze.customers",
            "{catalog}.bronze.orders",
            "{catalog}.bronze.products",
            "{catalog}.silver.customers",
            "{catalog}.silver.orders",
            "{catalog}.silver.products",
            "{catalog}.silver.quality_metrics",
            "{catalog}.gold.sales_by_product",
            "{catalog}.gold.revenue_by_customer",
            "{catalog}.gold.daily_trends",
            "{catalog}.gold.weekly_trends",
            "{catalog}.gold.customer_segmentation",
            "volume files customers.csv / orders.csv / products.csv",
        ),
        "append_only": ("{catalog}.bronze.ingest_metadata",),
        "preserved_across_runs": (
            "Git repository data/*.csv",
            "unrelated catalogs",
            "unrelated schemas",
            "the Unity Catalog catalog itself",
            "schema objects other than the known pipeline tables",
        ),
        "reset_targets_only": (
            "{catalog}.bronze (known pipeline tables)",
            "{catalog}.silver (known pipeline tables)",
            "{catalog}.gold (known pipeline tables)",
            "{catalog}.de_c1.source_data (the three CSV files only)",
        ),
    }


def assert_catalog_exists(spark: Any, catalog: str) -> None:
    require_sql_identifier(catalog, "catalog")
    try:
        rows = spark.sql("SHOW CATALOGS").collect()
    except Exception as exc:
        raise DatabricksBootstrapError(
            f"Cannot list catalogs to validate {catalog!r}: "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc
    names = {str(row[0]) for row in rows}
    if catalog not in names:
        raise DatabricksBootstrapError(
            f"Catalog {catalog!r} does not exist in this workspace. "
            "Databricks Free Edition typically provides catalog 'workspace'. "
            "Pass --catalog with an existing catalog. This workflow does not "
            "CREATE CATALOG."
        )
    LOGGER.info("Catalog %s exists", catalog)


def ensure_schema(spark: Any, qualified_schema: str) -> None:
    catalog, _, schema = qualified_schema.partition(".")
    if not schema:
        raise DatabricksBootstrapError(
            f"Schema {qualified_schema!r} must be catalog.schema."
        )
    require_sql_identifier(catalog, "catalog")
    require_sql_identifier(schema, "schema")
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    except Exception as exc:
        raise DatabricksBootstrapError(
            f"Cannot CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}: "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc
    LOGGER.info("Schema ready: %s.%s", catalog, schema)


def ensure_volume(spark: Any, catalog: str, schema: str, volume: str) -> None:
    require_sql_identifier(catalog, "catalog")
    require_sql_identifier(schema, "source schema")
    require_sql_identifier(volume, "source volume")
    ensure_schema(spark, f"{catalog}.{schema}")
    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
    except Exception as exc:
        raise DatabricksBootstrapError(
            f"Cannot CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}: "
            f"{exc.__class__.__name__}: {exc}"
        ) from exc
    LOGGER.info("Volume ready: %s.%s.%s", catalog, schema, volume)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def verify_git_source_files(git_data_path: str | Path) -> dict[str, Path]:
    root = Path(git_data_path)
    if not root.exists():
        raise DatabricksBootstrapError(
            f"Git-folder source directory does not exist: {root}. "
            "Connect this repository as a Databricks Git folder so data/ is "
            "present as workspace files, or pass --git-data-path."
        )
    found: dict[str, Path] = {}
    missing: list[str] = []
    for name in SOURCE_FILENAMES:
        path = root / name
        if not path.is_file():
            missing.append(name)
            continue
        found[name] = path
    if missing:
        raise DatabricksBootstrapError(
            "Git-folder source files are missing: "
            + ", ".join(missing)
            + f" (looked in {root}). Do not regenerate the datasets. "
            "Sync the Git folder so data/customers.csv, data/orders.csv, and "
            "data/products.csv are present."
        )
    LOGGER.info("Git-folder source files present under %s", root)
    return found


def _copy_bytes(source: Path, destination: Path) -> int:
    copied = 0
    with source.open("rb") as inf, destination.open("wb") as outf:
        while True:
            chunk = inf.read(1024 * 1024)
            if not chunk:
                break
            outf.write(chunk)
            copied += len(chunk)
    return copied


def _workspace_file_uri(path: Path) -> str:
    posix = path.as_posix()
    if posix.startswith("file:"):
        return posix
    if posix.startswith("/"):
        return f"file:{posix}"
    return f"file:/{posix}"


def _get_dbutils(spark: Any | None) -> Any | None:
    try:
        from pyspark.dbutils import DBUtils  # type: ignore[import-not-found]
    except Exception:
        DBUtils = None  # type: ignore[misc, assignment]
    if DBUtils is not None and spark is not None:
        try:
            return DBUtils(spark)
        except Exception as exc:
            LOGGER.info("DBUtils(spark) unavailable: %s", exc.__class__.__name__)
    try:
        import IPython  # type: ignore[import-not-found]

        ipython = IPython.get_ipython()
        if ipython is not None:
            candidate = ipython.user_ns.get("dbutils")
            if candidate is not None:
                return candidate
    except Exception:
        pass
    return None


def _copy_one_file(
    source: Path,
    destination: Path,
    spark: Any | None,
) -> tuple[int, str]:
    """
    Copy one CSV without recoding. Returns (bytes_copied, mechanism).
    """
    errors: list[str] = []
    try:
        copied = _copy_bytes(source, destination)
        return copied, "posix_direct"
    except OSError as exc:
        errors.append(f"posix_direct:{exc.__class__.__name__}")
        LOGGER.warning(
            "Direct POSIX copy failed for %s -> %s (%s). Trying driver temp.",
            source,
            destination,
            exc.__class__.__name__,
        )

    tmp_path: Path | None = None
    try:
        handle = tempfile.NamedTemporaryFile(prefix="de_c1_src_", suffix=".csv", delete=False)
        tmp_path = Path(handle.name)
        handle.close()
        _copy_bytes(source, tmp_path)
        copied = _copy_bytes(tmp_path, destination)
        return copied, "posix_via_driver_temp"
    except OSError as exc:
        errors.append(f"posix_via_driver_temp:{exc.__class__.__name__}")
        LOGGER.warning(
            "Driver-temp volume copy failed for %s (%s). Trying dbutils.fs.cp.",
            destination,
            exc.__class__.__name__,
        )
    finally:
        if tmp_path is not None:
            try:
                tmp_path.unlink()
            except OSError:
                pass

    dbutils = _get_dbutils(spark)
    if dbutils is not None:
        try:
            dbutils.fs.cp(_workspace_file_uri(source), destination.as_posix(), True)
            size = destination.stat().st_size if destination.is_file() else -1
            if size < 0:
                raise DatabricksBootstrapError(
                    f"dbutils.fs.cp reported success but {destination} is missing."
                )
            return int(size), "dbutils.fs.cp"
        except Exception as exc:
            errors.append(f"dbutils.fs.cp:{exc.__class__.__name__}")
            LOGGER.warning("dbutils.fs.cp failed: %s", exc.__class__.__name__)

    raise DatabricksBootstrapError(
        "Could not copy Git-folder source files into the Unity Catalog volume. "
        "Tried POSIX Python I/O, driver-temp + volume write, and dbutils.fs.cp "
        f"({'; '.join(errors)}). Unavoidable manual action: in the Databricks UI, "
        f"upload {source.name} from the Git-folder data/ directory to "
        f"{destination.parent.as_posix()}/ without renaming or editing the file. "
        "Do not regenerate the CSV."
    )


def copy_git_sources_to_volume(
    git_data_path: str | Path,
    volume_path: str,
    spark: Any | None = None,
) -> list[CopiedSourceFile]:
    """
    Copy the three Stage 2 CSVs from the Git folder to the UC Volume.

    Does not regenerate, rewrite, or trim the files. Destination files are
    verified by size and SHA-256 against the Git-folder originals.
    """
    sources = verify_git_source_files(git_data_path)
    dest_root = Path(volume_path)
    results: list[CopiedSourceFile] = []
    for name, source in sources.items():
        destination = dest_root / name
        LOGGER.info("Copying %s -> %s", source, destination)
        source_hash = sha256_file(source)
        copied, mechanism = _copy_one_file(source, destination, spark)
        if not destination.is_file():
            raise DatabricksBootstrapError(
                f"Destination file missing after copy: {destination}"
            )
        dest_size = destination.stat().st_size
        dest_hash = sha256_file(destination)
        if dest_size != source.stat().st_size or dest_hash != source_hash:
            raise DatabricksBootstrapError(
                f"{name} changed during copy. Git-folder SHA-256 {source_hash}; "
                f"volume SHA-256 {dest_hash}. Refusing to continue. Do not "
                "use a regenerated dataset."
            )
        LOGGER.info(
            "Copied %s (%s bytes, sha256=%s, mechanism=%s)",
            name,
            dest_size,
            dest_hash,
            mechanism,
        )
        results.append(
            CopiedSourceFile(
                filename=name,
                source_path=str(source),
                destination_path=str(destination),
                bytes_copied=copied,
                sha256=dest_hash,
                mechanism=mechanism,
            )
        )
    return results


def _qualified_table(catalog: str, schema: str, table: str) -> str:
    require_sql_identifier(catalog, "catalog")
    require_sql_identifier(schema, "schema")
    require_sql_identifier(table, "table")
    return f"{catalog}.{schema}.{table}"


def plan_reset(runtime: DatabricksRuntimeConfig) -> ResetPlan:
    """
    Describe RESET targets. Does not execute.

    Only ``{catalog}.bronze``, ``{catalog}.silver``, ``{catalog}.gold``, and
    the three CSVs in ``{catalog}.{source_schema}.{source_volume}``.
    Never Git repository data. Never unrelated catalogs/schemas.
    """
    if runtime.bronze_schema not in RESET_LAYER_SCHEMAS:
        raise DatabricksBootstrapError(
            f"RESET refused: bronze schema {runtime.bronze_schema!r} is not a "
            "pipeline layer schema."
        )
    if runtime.silver_schema not in RESET_LAYER_SCHEMAS:
        raise DatabricksBootstrapError(
            f"RESET refused: silver schema {runtime.silver_schema!r} is not a "
            "pipeline layer schema."
        )
    if runtime.gold_schema not in RESET_LAYER_SCHEMAS:
        raise DatabricksBootstrapError(
            f"RESET refused: gold schema {runtime.gold_schema!r} is not a "
            "pipeline layer schema."
        )
    tables = (
        tuple(
            _qualified_table(runtime.catalog, runtime.bronze_schema, name)
            for name in RESET_BRONZE_TABLES
        )
        + tuple(
            _qualified_table(runtime.catalog, runtime.silver_schema, name)
            for name in RESET_SILVER_TABLES
        )
        + tuple(
            _qualified_table(runtime.catalog, runtime.gold_schema, name)
            for name in RESET_GOLD_TABLES
        )
    )
    volume_files = tuple(
        f"{runtime.volume_path}/{name}" for name in SOURCE_FILENAMES
    )
    descriptions = (
        f"DROP TABLE IF EXISTS each of: {', '.join(tables)}",
        "Delete only these volume files: " + ", ".join(volume_files),
        "Do not DROP CATALOG",
        "Do not DROP unrelated schemas",
        "Do not modify Git repository data/",
        "Do not DROP the source schema or volume (CREATE VOLUME IF NOT EXISTS remains)",
    )
    return ResetPlan(
        catalog=runtime.catalog,
        schemas=(
            f"{runtime.catalog}.{runtime.bronze_schema}",
            f"{runtime.catalog}.{runtime.silver_schema}",
            f"{runtime.catalog}.{runtime.gold_schema}",
            runtime.source_schema_qualified,
        ),
        tables=tables,
        volume_files=volume_files,
        descriptions=descriptions,
    )


def execute_reset(spark: Any, runtime: DatabricksRuntimeConfig) -> ResetPlan:
    """
    Remove known evaluation objects. Requires the caller to pass an explicit
    ``--reset`` flag (this function does not infer reset from a normal run).
    """
    plan = plan_reset(runtime)
    LOGGER.warning("RESET plan for catalog %s:", plan.catalog)
    for line in plan.descriptions:
        LOGGER.warning("  %s", line)
    for qualified in plan.tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {qualified}")
            LOGGER.warning("Dropped table if existed: %s", qualified)
        except Exception as exc:
            raise DatabricksBootstrapError(
                f"RESET failed dropping {qualified}: {exc.__class__.__name__}: {exc}"
            ) from exc
    for file_path in plan.volume_files:
        path = Path(file_path)
        try:
            if path.is_file():
                path.unlink()
                LOGGER.warning("Deleted volume file: %s", file_path)
            else:
                LOGGER.info("Volume file already absent: %s", file_path)
        except OSError as exc:
            dbutils = _get_dbutils(spark)
            if dbutils is None:
                raise DatabricksBootstrapError(
                    f"RESET could not delete {file_path}: {exc.__class__.__name__}: {exc}"
                ) from exc
            try:
                dbutils.fs.rm(file_path, True)
                LOGGER.warning("Deleted volume file via dbutils: %s", file_path)
            except Exception as db_exc:
                raise DatabricksBootstrapError(
                    f"RESET could not delete {file_path}: {db_exc.__class__.__name__}: {db_exc}"
                ) from db_exc
    return plan


def bootstrap_environment(
    spark: Any,
    runtime: DatabricksRuntimeConfig,
    *,
    copy_sources: bool = True,
) -> list[CopiedSourceFile]:
    """
    Validate catalog, create schemas/volume if missing, copy Git CSVs to the volume.
    """
    assert_catalog_exists(spark, runtime.catalog)
    ensure_schema(spark, f"{runtime.catalog}.{runtime.bronze_schema}")
    ensure_schema(spark, f"{runtime.catalog}.{runtime.silver_schema}")
    ensure_schema(spark, f"{runtime.catalog}.{runtime.gold_schema}")
    ensure_volume(
        spark, runtime.catalog, runtime.source_schema, runtime.source_volume
    )
    if not copy_sources:
        return []
    return copy_git_sources_to_volume(
        runtime.git_data_path, runtime.volume_path, spark=spark
    )


def format_reset_plan(plan: ResetPlan) -> str:
    lines = ["RESET will remove the following evaluation resources:", ""]
    lines.extend(f"  - {item}" for item in plan.descriptions)
    lines.append("")
    lines.append("Git repository data/ is not modified.")
    return "\n".join(lines)


def known_reset_tables() -> dict[str, tuple[str, ...]]:
    return {
        "bronze": RESET_BRONZE_TABLES,
        "silver": RESET_SILVER_TABLES,
        "gold": RESET_GOLD_TABLES,
    }


def iter_policy_lines(catalog: str) -> Iterable[str]:
    policy = resource_policy()
    for key, values in policy.items():
        yield key
        for value in values:
            yield "  " + value.format(catalog=catalog)


def assert_reset_scope(runtime: DatabricksRuntimeConfig) -> None:
    """Refuse RESET if configured schemas are outside the evaluation set."""
    allowed_layers = set(RESET_LAYER_SCHEMAS)
    for label, name in (
        ("bronze", runtime.bronze_schema),
        ("silver", runtime.silver_schema),
        ("gold", runtime.gold_schema),
    ):
        if name not in allowed_layers:
            raise DatabricksBootstrapError(
                f"RESET refused because {label} schema {name!r} is outside "
                f"{sorted(allowed_layers)}."
            )
    if runtime.source_schema != DEFAULT_SOURCE_SCHEMA:
        raise DatabricksBootstrapError(
            f"RESET refused because source schema {runtime.source_schema!r} "
            f"is not {DEFAULT_SOURCE_SCHEMA!r}."
        )
    if runtime.source_volume != DEFAULT_SOURCE_VOLUME:
        raise DatabricksBootstrapError(
            f"RESET refused because source volume {runtime.source_volume!r} "
            f"is not {DEFAULT_SOURCE_VOLUME!r}."
        )
    plan_reset(runtime)


def is_databricks_runtime() -> bool:
    return bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))


def list_source_filenames() -> Sequence[str]:
    return SOURCE_FILENAMES
