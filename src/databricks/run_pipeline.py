"""
Primary Databricks entry point: bootstrap, execute, validate.

Uses the existing application modules:

- ``src/bronze/ingest_all.py`` / ``ingest_core.ingest_all``
- ``src/silver/create_silver_tables.py``
- ``src/gold/create_gold_tables.py``
- ``src/dashboard/dashboard_queries.sql``

It does not duplicate Bronze, Silver, or Gold transformations.

Intended Databricks command (Git folder = repository root)::

    python src/databricks/run_pipeline.py

Optional stages: ``bootstrap``, ``source``, ``bronze``, ``silver``, ``gold``,
``dashboard``. Optional ``--reset`` is required to tear down evaluation
objects; a normal run never resets.

Do not execute this as a Databricks job from this local environment until
asked. Visual dashboard rendering is not automated.
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from pathlib import Path
from typing import Sequence

_THIS_DIR = Path(__file__).resolve().parent
_SRC_DIR = _THIS_DIR.parent
_BRONZE_DIR = _SRC_DIR / "bronze"
_SILVER_DIR = _SRC_DIR / "silver"
_GOLD_DIR = _SRC_DIR / "gold"
for _path in (
    str(_SRC_DIR),
    str(_BRONZE_DIR),
    str(_SILVER_DIR),
    str(_GOLD_DIR),
    str(_THIS_DIR),
):
    if _path not in sys.path:
        sys.path.insert(0, _path)

from bootstrap import (  # noqa: E402
    DEFAULT_DATABRICKS_APP_NAME,
    DEFAULT_DATABRICKS_CATALOG,
    DEFAULT_SOURCE_SCHEMA,
    DEFAULT_SOURCE_VOLUME,
    DatabricksBootstrapError,
    assert_reset_scope,
    bootstrap_environment,
    execute_reset,
    format_reset_plan,
    is_databricks_runtime,
    load_databricks_runtime_config,
    plan_reset,
    resource_policy,
)
from ingest_core import get_spark_session, ingest_all  # noqa: E402
from validate import (  # noqa: E402
    ValidationError,
    ValidationReport,
    validate_bronze,
    validate_dashboard_sql,
    validate_gold,
    validate_silver,
    validate_source_files,
)

import create_gold_tables as gold_tables  # noqa: E402
import create_silver_tables as silver_tables  # noqa: E402

LOGGER = logging.getLogger("databricks.run_pipeline")

STAGE_ALL = "all"
STAGES: tuple[str, ...] = (
    STAGE_ALL,
    "bootstrap",
    "source",
    "bronze",
    "silver",
    "gold",
    "dashboard",
)


class PipelineWorkflowError(RuntimeError):
    """Workflow failed before or during a stage."""


def _wants(stage: str, names: Sequence[str]) -> bool:
    return stage == STAGE_ALL or stage in names


def run_workflow(args: argparse.Namespace) -> ValidationReport:
    report = ValidationReport()
    runtime = load_databricks_runtime_config(
        catalog=args.catalog,
        source_schema=args.source_schema,
        source_volume=args.source_volume,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
        table_format=args.table_format,
        spark_app_name=args.spark_app_name,
        git_data_path=args.git_data_path,
    )
    pipeline = runtime.pipeline_config()
    LOGGER.info(
        "Databricks workflow catalog=%s volume=%s git_data=%s table_format=%s stage=%s reset=%s",
        runtime.catalog,
        runtime.volume_path,
        runtime.git_data_path,
        pipeline.table_format,
        args.stage,
        args.reset,
    )
    LOGGER.info("Resource policy: %s", resource_policy())

    if args.reset:
        assert_reset_scope(runtime)
        plan = plan_reset(runtime)
        LOGGER.warning(format_reset_plan(plan))

    spark = None
    needs_spark = args.reset or args.stage in {
        STAGE_ALL,
        "bootstrap",
        "bronze",
        "silver",
        "gold",
        "dashboard",
    }
    if needs_spark:
        spark = get_spark_session(pipeline.spark_app_name)

    if args.reset:
        if spark is None:
            raise PipelineWorkflowError("RESET requires a Spark session.")
        execute_reset(spark, runtime)
        report.compare_true(
            "reset.completed",
            True,
            notes="evaluation tables and source volume CSVs only",
        )

    if _wants(args.stage, ("bootstrap",)):
        if spark is None:
            raise PipelineWorkflowError("bootstrap requires a Spark session.")
        copied = bootstrap_environment(spark, runtime, copy_sources=True)
        report.compare(
            "bootstrap.source_files_copied",
            3,
            len(copied),
            notes="; ".join(f"{item.filename}:{item.mechanism}" for item in copied),
        )

    if _wants(args.stage, ("bootstrap", "source")):
        git_report = validate_source_files(runtime.git_data_path, ValidationReport())
        for check in git_report.checks:
            report.add(replace(check, check="git." + check.check))
        volume_report = validate_source_files(runtime.volume_path, ValidationReport())
        for check in volume_report.checks:
            report.add(replace(check, check="volume." + check.check))

    if _wants(args.stage, ("bronze",)):
        if spark is None:
            raise PipelineWorkflowError("Bronze requires a Spark session.")
        results = ingest_all(spark=spark, config=pipeline)
        report.compare(
            "bronze.ingest_all.entities",
            3,
            len(results),
            notes="src/bronze/ingest_core.ingest_all",
        )
        validate_bronze(spark, pipeline, report)

    if _wants(args.stage, ("silver",)):
        if spark is None:
            raise PipelineWorkflowError("Silver requires a Spark session.")
        silver_tables.create_silver_tables(spark=spark, config=pipeline)
        report.compare_true(
            "silver.create_silver_tables.invoked",
            True,
            notes="src/silver/create_silver_tables.py",
        )
        validate_silver(spark, pipeline, report)

    if _wants(args.stage, ("gold",)):
        if spark is None:
            raise PipelineWorkflowError("Gold requires a Spark session.")
        gold_tables.create_gold_tables(spark=spark, config=pipeline)
        report.compare_true(
            "gold.create_gold_tables.invoked",
            True,
            notes="src/gold/create_gold_tables.py executes src/gold/*.sql",
        )
        validate_gold(spark, pipeline, report)

    if _wants(args.stage, ("dashboard",)):
        if spark is None:
            raise PipelineWorkflowError("Dashboard SQL validation requires Spark.")
        validate_dashboard_sql(spark, pipeline, report)

    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Bootstrap the Databricks evaluation workspace, copy Git-folder "
            "CSVs to the UC Volume, run Bronze/Silver/Gold, and validate."
        )
    )
    parser.add_argument("--catalog", default=None, help=f"default {DEFAULT_DATABRICKS_CATALOG}")
    parser.add_argument(
        "--source-schema", default=None, help=f"default {DEFAULT_SOURCE_SCHEMA}"
    )
    parser.add_argument(
        "--source-volume", default=None, help=f"default {DEFAULT_SOURCE_VOLUME}"
    )
    parser.add_argument("--bronze-schema", default=None)
    parser.add_argument("--silver-schema", default=None)
    parser.add_argument("--gold-schema", default=None)
    parser.add_argument(
        "--table-format",
        default=None,
        choices=["delta", "parquet"],
        help="Databricks default is delta. Do not pass parquet on the cluster.",
    )
    parser.add_argument("--spark-app-name", default=DEFAULT_DATABRICKS_APP_NAME)
    parser.add_argument(
        "--git-data-path",
        default=None,
        help="Directory that contains the version-controlled CSVs (default: <repo>/data).",
    )
    parser.add_argument(
        "--stage",
        default=STAGE_ALL,
        choices=list(STAGES),
        help="all = bootstrap through dashboard SQL validation",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help=(
            "Explicit evaluation RESET before the requested stage. "
            "Targets only workspace.bronze / silver / gold tables and the "
            "three CSVs in workspace.de_c1.source_data. Never Git data."
        ),
    )
    parser.add_argument(
        "--allow-local",
        action="store_true",
        help="Allow running outside DATABRICKS_RUNTIME_VERSION (tests / diagnostics).",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.allow_local and not is_databricks_runtime():
        LOGGER.error(
            "This workflow is the Databricks bootstrap/execution/validation path. "
            "DATABRICKS_RUNTIME_VERSION is unset. Local parquet tests continue to "
            "use src/bronze/ingest_all.py --table-format parquet. Pass "
            "--allow-local only for diagnostics. Databricks has not been executed "
            "from this local environment by this command."
        )
        return 2
    try:
        report = run_workflow(args)
    except (DatabricksBootstrapError, ValidationError, PipelineWorkflowError) as exc:
        LOGGER.error("%s", exc)
        print(f"FINAL RESULT: FAIL\nNOTES: {exc}", file=sys.stderr)
        return 1
    print(report.format_table())
    if not report.passed:
        LOGGER.error("Workflow finished with critical failures.")
        return 1
    LOGGER.info("Workflow finished: FINAL RESULT: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
