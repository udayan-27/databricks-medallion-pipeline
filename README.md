# Databricks Medallion Pipeline — E-commerce Sales

This repository is the DE C1 AI Capability Exercise submission. It implements (when complete) a Databricks Medallion Architecture pipeline:

`CSV -> Bronze -> Silver -> Gold -> Dashboard`

Requirements and architecture are written. Stage 2 sample data has been generated. **Bronze ingest code is implemented.** **Silver completeness, uniqueness, type validation, and referential integrity are implemented** (business logic / Silver tables remain stubs). Gold / Dashboard code is still stubs. Bronze tables have **not** been created in a Databricks workspace from this environment.

Canonical requirements: [`DE_C1_REQUIREMENTS.md`](DE_C1_REQUIREMENTS.md).

## Status

| Area | Status |
|---|---|
| Repository structure | Initialized |
| Requirements analysis, architecture, data model, DQ strategy | Written (design stage) |
| Sample CSV data | Generated (10,010 / 100,020 / 500 rows; seed 42) |
| Bronze ingest code | Implemented (`src/bronze/`, `src/config.py`) |
| Local Spark runtime | Python 3.11 `.venv` + Temurin JDK 17 + PySpark 3.5.6 (isolated from system Python 3.12) |
| Local Spark validation | In-memory smoke **passed**. Local parquet Bronze ingest tests **passed** (21/21). Silver completeness/uniqueness/type/RI Spark tests **passed** (35/35). Not Databricks. |
| Bronze Databricks / Delta / Unity Catalog | **Not run** from this environment |
| Silver completeness / uniqueness / type / RI | Implemented (`src/silver/01_quality_completeness.py` through `04_quality_referential_integrity.py`, `quality_common.py`). Local Spark validated. Does not write Silver tables. |
| Silver business logic / Gold / Dashboard | Stubs only |
| Tests | Generator **14/14 OK**; Bronze contract **37/37 OK**; Spark ingest **21/21 OK**; Silver contract **13/13 OK**; Silver Spark **35/35 OK**; combined relevant set **120/120 OK** (0 skipped) |

Do not treat stub business-logic modules, `create_silver_tables.py`, or placeholder Gold SQL as a working pipeline. Completeness, uniqueness, type validation, and RI can be applied to Bronze DataFrames locally. The CSVs in `data/` are real generated inputs.

## What this exercise evaluates

The submission must demonstrate requirement analysis, architecture, AI-assisted implementation, data quality engineering, testing, validation of AI output, debugging, documentation, responsible AI usage, human ownership of technical decisions, meaningful Git history, and complete AI prompt history.

## Layers

- **Bronze:** raw, unchanged CSV ingest into `bronze.customers`, `bronze.orders`, `bronze.products`, plus append-only `bronze.ingest_metadata`. Source columns are not cleaned. `_ingest_row_id` is ingest lineage (unique per physical row of a write; regenerated each run).
- **Silver:** five quality modules (completeness, uniqueness, type validation, referential integrity, business logic). Bad rows are flagged, not deleted. **Completeness, uniqueness, type validation, and RI are implemented** as PySpark transforms; they do not delete rows and do not write combined Silver tables. Business logic / `create_silver_tables.py` are **not implemented yet.**
- **Gold:** business aggregations in SQL (sales by product, revenue by customer, daily/weekly trends, customer segmentation). **Not implemented yet.**
- **Dashboard:** Databricks SQL dashboard with at least three tiles and filters. **Not implemented yet.**

## Repository layout

See `cursor-workflow/spec.md` and `cursor-workflow/task-breakdown.md` for the staged plan. Required paths match `DE_C1_REQUIREMENTS.md`.

## Setup

### Sample data

From the repository root (Python 3.12 used when this was run):

```
python src/data_generation/generate_sample_data.py --output-dir data --seed 42
python -m unittest tests.test_generate_sample_data -v
```

Default seed is **42**. The default `--output-dir` is the repo `data/` directory (resolved from the script path, not a hardcoded Windows or Databricks path). Same seed produces byte-identical UTF-8/LF CSVs. See `src/data_generation/DATA_GENERATION_NOTES.md`.

Do not regenerate these files unless the seed or contract changes.

### Bronze ingest

Configuration (no secrets in git):

| Setting | Purpose | Default |
|---|---|---|
| `MEDALLION_DATA_PATH` / `--data-path` | Directory containing the three CSVs | `<repo>/data` |
| `MEDALLION_CATALOG` / `--catalog` | Unity Catalog name | unset (tables are `bronze.<entity>`) |
| `MEDALLION_BRONZE_SCHEMA` / `--bronze-schema` | Schema name | `bronze` |
| `MEDALLION_TABLE_FORMAT` / `--table-format` | `delta` or `parquet` | `delta` |

**Databricks:** run on a cluster with an active SparkSession. Point `MEDALLION_DATA_PATH` at a UC volume, DBFS, or S3 prefix that contains `customers.csv`, `orders.csv`, and `products.csv`. Set `MEDALLION_CATALOG` if using Unity Catalog. Default format `delta` is the Databricks path.

```
python src/bronze/ingest_all.py --data-path /Volumes/<catalog>/<schema>/<volume>
```

Individual datasets: `python src/bronze/01_ingest_customers.py` (same flags). `ingest_all.py` preflights all three files, then overwrites the three entity tables and appends three metadata rows sharing one `ingest_id`.

**Local Spark (this machine):** an isolated project `.venv` is used so PySpark is not installed into system Python 3.12 (or 3.7). `.gitignore` already ignores `.venv/`.

| Component | Actual |
|---|---|
| Python for Spark | 3.11.9 (`py -3.11`), venv `.venv\Scripts\python.exe` |
| System Python | 3.12.7 (unchanged; no PySpark) |
| JDK | Eclipse Temurin 17.0.20.1 (locate the installed `jdk-17*-hotspot` directory; do not assume a patch path) |
| PySpark | 3.5.6 inside `.venv` only (`py4j==0.10.9.7` came in as its dependency) |

Create and use the venv from the repo root:

```
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install "pyspark==3.5.6"
$env:JAVA_HOME = (Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Directory | Where-Object { $_.Name -like "jdk-17*" } | Select-Object -First 1).FullName
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
$env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe).Path
python -m unittest tests.test_bronze_ingest -v
```

Do **not** install delta-spark, pandas, pyarrow, Jupyter, standalone Spark, or Hadoop/winutils for this stack. Default `--table-format` is still `delta` (Databricks). Local validation uses `--table-format parquet`. On Windows, locally created SparkSessions compile `src/local_runtime/NoWinutilsRawLocalFileSystem.java` so parquet `saveAsTable` does not need winutils; Databricks uses the cluster session and never loads that class.

**What this environment actually proved (2026-08-31):**

- `createDataFrame` + `count()` + `collect()` + `stop()` in `local[2]` **succeeded**.
- `python -m unittest tests.test_bronze_contract -v` → **37 tests OK**.
- `python -m unittest tests.test_bronze_ingest -v` → **21 tests OK** (full source vs Bronze: 10010 / 100020 / 500; duplicates, NULLs, and orphans retained).
- Combined: `python -m unittest tests.test_bronze_contract tests.test_bronze_ingest -v` → **58 tests OK**.
- Stage 2 CSV SHA-256 unchanged vs `DATA_GENERATION_NOTES.md`.
- None of the above is Databricks, Delta, DBFS, or Unity Catalog validation. See `debugging-notes.md`.

**Rerun:** entity tables overwrite from the current CSVs; `_ingest_row_id` values change; `bronze.ingest_metadata` appends. Do not run overlapping jobs.

### Silver completeness, uniqueness, type validation, and RI

These modules read Bronze DataFrames (or already-written Bronze tables) and **add** quality columns. They do not delete rows, do not write `silver.*` tables, and do not run business-logic checks.

| Setting | Purpose | Default |
|---|---|---|
| `--bronze-schema` | Schema of the Bronze tables to read | `bronze` |
| `--table-format` | Must match the Bronze write format | `delta` (use `parquet` locally) |

After a local Bronze parquet ingest:

```
python src/silver/01_quality_completeness.py --table-format parquet
python src/silver/02_quality_uniqueness.py --table-format parquet
python src/silver/03_quality_type_validation.py --table-format parquet
python src/silver/04_quality_referential_integrity.py --table-format parquet
```

That logs physical-row pass/fail metrics. Combined `quality_check_result` and `silver.quality_metrics` are later (`create_silver_tables.py`).

Tests (same local Spark stack as Bronze):

```
python -m unittest tests.test_silver_contract -v
python -m unittest tests.test_silver_quality -v
```

Observed local counts on seed-42 data: 50 NULL emails, 100 NULL order `customer_id`, 200 NULL order `product_id`, 20 customer uniqueness-fail rows, 40 order uniqueness-fail rows, **0 type failures**, **50 orphan customer_id**, **30 orphan product_id**. NULL FKs are not counted as orphans. Rows remain (10,010 / 100,020 / 500). Malformed type cases use `tests/fixtures/silver/type_validation/`, not Stage 2 CSVs.

### Tests that run without Spark

```
python -m unittest tests.test_bronze_contract tests.test_silver_contract -v
```

No real PII, credentials, secrets, tokens, or private production connection details belong in this repository.

## Working rules

Every meaningful change must be derived from the written spec, tested, reviewed against requirements, recorded in `ai-prompts/`, and committed with a descriptive message.

## Documentation map

- `requirements-analysis.md` — problem, requirements, ambiguities, decisions
- `design-notes.md` — architecture decisions
- `data-model.md` — Bronze / Silver / Gold contracts
- `data-quality-strategy.md` — quality checks and metrics
- `cursor-workflow/` — persistent context for Cursor-assisted work
- `ai-prompts/` — actual prompt history (not fabricated)
- `src/bronze/ingest_core.py` — Bronze CSV options, lineage, local vs Databricks, rerun behaviour
- `src/silver/quality_common.py` — shared Silver accumulation and physical-row metrics helpers
