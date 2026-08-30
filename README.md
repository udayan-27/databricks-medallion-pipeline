# Databricks Medallion Pipeline — E-commerce Sales

This repository is the DE C1 AI Capability Exercise submission. It implements (when complete) a Databricks Medallion Architecture pipeline:

`CSV -> Bronze -> Silver -> Gold -> Dashboard`

Requirements and architecture are written. Stage 2 sample data has been generated. **Bronze ingest code is implemented.** Silver / Gold / Dashboard code is still stubs. Bronze tables have **not** been created in a Databricks workspace from this environment.

Canonical requirements: [`DE_C1_REQUIREMENTS.md`](DE_C1_REQUIREMENTS.md).

## Status

| Area | Status |
|---|---|
| Repository structure | Initialized |
| Requirements analysis, architecture, data model, DQ strategy | Written (design stage) |
| Sample CSV data | Generated (10,010 / 100,020 / 500 rows; seed 42) |
| Bronze ingest code | Implemented (`src/bronze/`, `src/config.py`) |
| Local Spark runtime | Python 3.11 `.venv` + Temurin JDK 17 + PySpark 3.5.6 (isolated from system Python 3.12) |
| Local Spark validation | In-memory smoke test **passed**. Bronze parquet ingest tests **failed** (Windows Hadoop path encoding + missing winutils). Not Databricks. |
| Bronze Databricks / Delta / Unity Catalog | **Not run** from this environment |
| Silver / Gold / Dashboard code | Stubs only |
| Tests | Generator tests run; Bronze contract tests 33/33 OK; Spark ingest tests ran (not skipped) and failed |

Do not treat stub Silver/Gold modules or placeholder SQL as a working pipeline. The CSVs in `data/` are real generated inputs.

## What this exercise evaluates

The submission must demonstrate requirement analysis, architecture, AI-assisted implementation, data quality engineering, testing, validation of AI output, debugging, documentation, responsible AI usage, human ownership of technical decisions, meaningful Git history, and complete AI prompt history.

## Layers

- **Bronze:** raw, unchanged CSV ingest into `bronze.customers`, `bronze.orders`, `bronze.products`, plus append-only `bronze.ingest_metadata`. Source columns are not cleaned. `_ingest_row_id` is ingest lineage (unique per physical row of a write; regenerated each run).
- **Silver:** five quality modules (completeness, uniqueness, type validation, referential integrity, business logic). Bad rows are flagged, not deleted. **Not implemented yet.**
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

Do **not** install delta-spark, pandas, pyarrow, Jupyter, standalone Spark, or Hadoop/winutils for this stack. Default `--table-format` is still `delta` (Databricks). Local attempts use `--table-format parquet`.

**What this environment actually proved (2026-08-31):**

- `createDataFrame` + `count()` + `collect()` + `stop()` in `local[2]` **succeeded**.
- `python -m unittest tests.test_bronze_contract -v` → **33 tests OK**.
- `python -m unittest tests.test_bronze_ingest -v` → **FAILED** (1 failure, 2 errors). Spark could not read `file:/...%20...` URIs produced by `Path.as_uri()`, and Hadoop `mkdirs`/`saveAsTable` need winutils on Windows. Bronze logic was not changed to hide that. See `debugging-notes.md`.
- `python src/bronze/ingest_all.py --table-format parquet` was **not** run, because the ingest test suite did not succeed.
- None of the above is Databricks, Delta, DBFS, or Unity Catalog validation.

**Rerun:** entity tables overwrite from the current CSVs; `_ingest_row_id` values change; `bronze.ingest_metadata` appends. Do not run overlapping jobs.

### Tests that run without Spark

```
python -m unittest tests.test_bronze_contract -v
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
