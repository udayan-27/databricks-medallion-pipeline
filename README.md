# Databricks Medallion Pipeline — E-commerce Sales

This repository is the DE C1 AI Capability Exercise submission. It implements a Databricks Medallion Architecture pipeline for **synthetic** e-commerce sales:

`CSV → Bronze (PySpark, raw) → Silver (PySpark, five quality modules) → Gold (SQL) → Dashboard queries`

Local Spark / parquet tests have been run. The official Databricks path is the repository workflow in `src/databricks/`. **That workflow has not been executed in a Databricks workspace from this environment.** Do not read local results as Databricks results.

Canonical requirements: [`DE_C1_REQUIREMENTS.md`](DE_C1_REQUIREMENTS.md).

## Reviewer navigation

| Topic | Where |
|---|---|
| Assignment requirements | [`DE_C1_REQUIREMENTS.md`](DE_C1_REQUIREMENTS.md) |
| Ambiguities and decisions | [`requirements-analysis.md`](requirements-analysis.md) |
| Architecture | [`design-notes.md`](design-notes.md) |
| Column contracts | [`data-model.md`](data-model.md) |
| Silver quality rules | [`data-quality-strategy.md`](data-quality-strategy.md) |
| Prompt history | [`ai-prompts/prompt-index.md`](ai-prompts/prompt-index.md) |
| Tests | [`tests/`](tests/) |
| Debugging | [`debugging-notes.md`](debugging-notes.md) |
| Databricks bootstrap / run / validate | [`database/setup-notes.md`](database/setup-notes.md), [`src/databricks/run_pipeline.py`](src/databricks/run_pipeline.py) |
| Dashboard workspace steps | [`src/dashboard/DASHBOARD_GUIDE.md`](src/dashboard/DASHBOARD_GUIDE.md) |
| Public-repo audit | [`FINAL_AUDIT.md`](FINAL_AUDIT.md) |
| Reflection / AI usage | [`reflection.md`](reflection.md), [`final-ai-usage-summary.md`](final-ai-usage-summary.md) |

## Status

| Area | Status |
|---|---|
| Repository structure | Required assignment tree plus tests, config, and local Spark helpers |
| Requirements, architecture, data model, DQ strategy | Written and frozen |
| Sample CSV data | Generated (seed **42**). Physical rows **10,010 / 100,020 / 500**. Unique keys **10,000 / 100,000 / 500**. Do not regenerate. |
| Bronze ingest | Implemented. Local parquet tests passed. Databricks / Delta / UC **not** run. |
| Silver (all five modules + combiner) | Implemented. Local parquet tests passed. Databricks Silver **not** run. |
| Gold SQL aggregations | Implemented. Local parquet tests passed. Databricks Gold **not** run. |
| Dashboard | Queries + guide implemented and locally tested. Databricks SQL Dashboard UI **not** rendered. |
| Databricks workflow | Repository-owned bootstrap + pipeline + validation in `src/databricks/`. Uses existing Bronze/Silver/Gold modules. **Not executed** in a workspace from this environment. |

## What this exercise evaluates

Requirement analysis, architecture, AI-assisted implementation with recorded prompts, data-quality engineering, testing, validation of AI output, debugging, documentation, responsible AI, human ownership of decisions, and meaningful Git history.

## Architecture

```
data/*.csv  (or MEDALLION_DATA_PATH on DBFS / S3 / UC Volume)
    → Bronze  bronze.customers | bronze.orders | bronze.products  + bronze.ingest_metadata
    → Silver  five quality modules → silver.* + silver.quality_metrics
    → Gold    sales_by_product, revenue_by_customer, daily_trends, weekly_trends, customer_segmentation
    → Dashboard queries against Gold only
```

- **Bronze:** raw CSV ingest. Source columns are not cleaned. `_ingest_row_id` is ingest lineage (unique per physical row of a write; regenerated each run).
- **Silver:** completeness, uniqueness, type validation, referential integrity, business logic. Bad rows are **flagged, not deleted**. Combined `quality_check_result` / `failed_checks`.
- **Gold:** qualifying orders are `order_status = 'Completed' AND quality_check_result = 'PASS'`. `lifetime_value_actual` is qualifying order revenue, **not** source `customers.lifetime_value`.
- **Dashboard:** Top 10 products (bar), customer revenue distribution (histogram), customer segmentation (pie), plus two Gold-field filters.

## Frozen requirement interpretations

- Listed defects total **460 issue instances**. Do not pad to ~700.
- Implement **all five** Silver modules (the narrative says four; the required tree lists five).
- Physical customers **10,010** (10,000 unique + 10 extra duplicate rows). Physical orders **100,020** (100,000 unique + 20 extra duplicate rows). Products **500**.
- **30 future signup** records are optional business-logic defects, documented separately from the 460.
- Local Spark writes **parquet**. Databricks default is **Delta**.

## Repository layout

Required assignment paths are present. Supporting modules that the assignment omitted but the work needs:

| Path | Role |
|---|---|
| `src/config.py` | `MEDALLION_DATA_PATH`, `MEDALLION_CATALOG`, schema names, `MEDALLION_TABLE_FORMAT` |
| `src/bronze/ingest_core.py`, `contracts.py` | Shared ingest (numbered `01_` scripts are thin CLIs) |
| `src/silver/quality_common.py` | Shared quality accumulation |
| `src/spark_local.py`, `src/local_runtime/` | Local Windows Spark only; not used on Databricks |
| `src/databricks/` | Databricks bootstrap, source copy, orchestration, validation |
| `tests/` | Contract tests (no Spark) and Spark integration tests |
| `requirements.txt` | Pin `pyspark==3.5.6` for local setup |

## Local vs Databricks

| | Local (this machine / a fresh clone) | Databricks |
|---|---|---|
| Python | 3.11 virtualenv | Databricks Runtime Python |
| JDK | 17 (Temurin or equivalent) | Cluster JDK |
| Spark | PySpark **3.5.6** from `requirements.txt` | Cluster Spark |
| Table format | `--table-format parquet` | omit the flag (default **delta**) |
| Data path | repo `data/` | `/Volumes/<catalog>/de_c1/source_data` after `src/databricks` copies Git-folder `data/` |
| Catalog | unset (Hive metastore names `bronze.customers`) | `MEDALLION_CATALOG` at runtime |
| SparkSession | created locally; Windows FileSystem adapter may apply | **reuse the cluster session** |
| Do not copy | `.venv`, winutils, laptop `JAVA_HOME`, parquet flag | — |

Databricks execution from this repository: **not started**. The command to run there is documented below; do not treat that documentation as a completed workspace run.

## Prerequisites (fresh clone)

### Generator and Spark-free tests

Python 3.11 or 3.12 is enough. No extra packages.

```
python src/data_generation/generate_sample_data.py --output-dir data --seed 42
python -m unittest tests.test_generate_sample_data -v
```

**Do not regenerate** the committed `data/*.csv` files unless the seed or contract changes. They are required submission artifacts (seed 42, as-of 2026-08-31, ROUND_HALF_EVEN money rounding). SHA-256 values are in [`src/data_generation/DATA_GENERATION_NOTES.md`](src/data_generation/DATA_GENERATION_NOTES.md).

### Local Spark (Bronze / Silver / Gold / Dashboard query tests)

From the repository root:

```
py -3.11 -m venv .venv
```

Windows:

```
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
$env:JAVA_HOME = (Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Directory | Where-Object { $_.Name -like "jdk-17*" } | Select-Object -First 1).FullName
$env:Path = "$env:JAVA_HOME\bin;" + $env:Path
$env:PYSPARK_PYTHON = (Resolve-Path .\.venv\Scripts\python.exe).Path
```

macOS / Linux: create the venv with Python 3.11, `pip install -r requirements.txt`, point `JAVA_HOME` at a JDK 17 install, then use `export PYSPARK_PYTHON=$(pwd)/.venv/bin/python`.

Do **not** install delta-spark, pandas, winutils, or PySpark into system Python. Default `--table-format` remains `delta` for Databricks. Local validation passes `--table-format parquet`. On Windows, locally created SparkSessions compile `src/local_runtime/NoWinutilsRawLocalFileSystem.java` so parquet `saveAsTable` does not need winutils.

## Configuration

No secrets in git. `.venv/` and `.env` are gitignored.

| Setting | Purpose | Default |
|---|---|---|
| `MEDALLION_DATA_PATH` / `--data-path` | Directory containing the three CSVs | `<repo>/data` |
| `MEDALLION_CATALOG` / `--catalog` | Unity Catalog name | unset |
| `MEDALLION_BRONZE_SCHEMA` / `--bronze-schema` | Bronze schema | `bronze` |
| `MEDALLION_SILVER_SCHEMA` / `--silver-schema` | Silver schema | `silver` |
| `MEDALLION_GOLD_SCHEMA` / `--gold-schema` | Gold schema | `gold` |
| `MEDALLION_TABLE_FORMAT` / `--table-format` | `delta` or `parquet` | `delta` |
| `MEDALLION_SOURCE_SCHEMA` | UC schema that owns the source volume (Databricks workflow) | `de_c1` |
| `MEDALLION_SOURCE_VOLUME` | UC volume name (Databricks workflow) | `source_data` |
| `MEDALLION_GIT_DATA_PATH` | Git-folder directory that contains the three CSVs | `<repo>/data` |

## Execution

Run **one** Spark job or unittest process at a time. Concurrent Spark processes are unsupported on Windows.

### Bronze

```
python src/bronze/ingest_all.py --table-format parquet
```

Databricks official path: `python src/databricks/run_pipeline.py` from the Git-folder repository root (see below). That command copies `data/*.csv` to the UC Volume and then calls `ingest_all.py`. Individual datasets remain available: `python src/bronze/01_ingest_customers.py`. `ingest_all.py` preflights all three files, overwrites the three entity tables, and appends three metadata rows sharing one `ingest_id`.

Rerun: entity tables overwrite; `_ingest_row_id` values change; `bronze.ingest_metadata` appends.

### Silver

Modules add quality columns. They do not delete rows. Then write combined Silver tables:

```
python src/silver/01_quality_completeness.py --table-format parquet
python src/silver/02_quality_uniqueness.py --table-format parquet
python src/silver/03_quality_type_validation.py --table-format parquet
python src/silver/04_quality_referential_integrity.py --table-format parquet
python src/silver/05_quality_business_logic.py --table-format parquet
python src/silver/create_silver_tables.py --table-format parquet
```

Observed local counts on seed-42 data: 50 NULL emails, 100 NULL order `customer_id`, 200 NULL order `product_id`, 20 customer uniqueness-fail rows, 40 order uniqueness-fail rows, **0 type failures**, **50 orphan customer_id**, **30 orphan product_id**, **30 future signup** business-logic failures (optional; not in the 460). Combined customer FAIL rows = 100; order FAIL rows = 420. Physical rows remain 10,010 / 100,020 / 500.

### Gold

```
python src/gold/create_gold_tables.py --table-format parquet
```

Eligibility is in each SQL file: `order_status = 'Completed' AND quality_check_result = 'PASS'`. Segmentation: Inactive → High-Value (>= 1000.00) → Repeat → One-Time. `revenue_by_customer` includes every canonical customer, including zeros.

### Dashboard queries

SQL: [`src/dashboard/dashboard_queries.sql`](src/dashboard/dashboard_queries.sql). Workspace click-path: [`src/dashboard/DASHBOARD_GUIDE.md`](src/dashboard/DASHBOARD_GUIDE.md).

| Tile | Gold table | Visualization |
|---|---|---|
| Top 10 products by revenue | `gold.sales_by_product` | bar |
| Customer revenue distribution | `gold.revenue_by_customer.lifetime_value_actual` (all canonical customers, including zeros) | histogram (bins in the Databricks viz, not SQL) |
| Customer segmentation | `gold.customer_segmentation` | pie |

Filters: `category` on Tile 1 **before** `LIMIT 10`; `customer_segment` on Tile 2. Date range is not a filter on these tiles.

## Databricks (official workspace workflow)

The supported Databricks process is a single repository command. It does **not** depend on pasted exploratory notebook cells. Bronze/Silver/Gold logic stays in the existing application modules.

This workflow has **not** been executed in Databricks from this environment.

### MANUAL — genuinely unavoidable

1. Databricks login / account authorization
2. GitHub authorization if the workspace Git folder requires it
3. Git-folder creation/connection if the Databricks UI/API requires it (connect this repository so `data/*.csv` and `src/` are workspace files)
4. Visual Dashboard creation/rendering in the Databricks SQL UI after Gold exists (`DASHBOARD_GUIDE.md`). SQL validation of those queries **is** automated; tile rendering is not.

### AUTOMATED (`python src/databricks/run_pipeline.py`)

Schema creation, volume creation, copy of Git-folder `data/*.csv` into `/Volumes/workspace/de_c1/source_data/`, source validation, Bronze, Bronze validation, Silver, Silver validation, Gold, Gold validation, dashboard SQL validation, structured PASS/FAIL report.

From the Git-folder repository root, on Databricks serverless / cluster Python:

```
python src/databricks/run_pipeline.py
```

Defaults: catalog `workspace`, source schema `de_c1`, volume `source_data`, table format `delta`, app name `DE_C1_Databricks`. Optional: `--stage bootstrap|source|bronze|silver|gold|dashboard`. Optional evaluation reset (not used on a normal run): `--reset`.

Details, resource policy, and the source-copy mechanism: [`database/setup-notes.md`](database/setup-notes.md).

## Tests

Spark-free:

```
python -m unittest tests.test_bronze_contract tests.test_silver_contract tests.test_gold_contract tests.test_dashboard_contract tests.test_databricks_workflow -v
```

Full relevant suite (**one process**; do not start a second copy):

```
python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality tests.test_gold_contract tests.test_gold_aggregations tests.test_dashboard_contract tests.test_dashboard_queries tests.test_databricks_workflow -v
```

| Suite | Tests |
|---|---|
| Generator | 14 |
| Bronze contract | 38 |
| Bronze Spark ingest | 21 |
| Silver contract | 20 |
| Silver Spark | 55 |
| Gold contract | 11 |
| Gold Spark | 16 |
| Dashboard contract | 15 |
| Dashboard Spark | 13 |
| Databricks workflow (Spark-free) | 20 |
| **Total relevant** | **223** |

Latest sequential full-suite result (this Databricks-workflow increment): **Ran 223 tests in 534.573s OK**. The prior public-repo audit recorded 203 tests in [`FINAL_AUDIT.md`](FINAL_AUDIT.md). Do not treat an older prompt-file timing as the current result.

A second concurrent full suite can fail during Spark JVM gateway launch on Windows. That is an environment issue, not a pipeline logic failure. See [`debugging-notes.md`](debugging-notes.md).

## Local limitations

- No winutils. Windows parquet writes use a project Java FileSystem **only** for locally created sessions (`os.name == "nt"`).
- Repo path spaces produce a non-fatal `'C1' is not recognized` warning from `spark-submit.cmd`. Tests still run.
- Local warehouse / `metastore_db` / parquet files are gitignored. They are not submission artifacts.
- Histogram binning, dashboard widgets, and Unity Catalog grants require a Databricks workspace.

## Expected quality checks (seed 42)

Mandatory listed issues (460 instances): 50 NULL emails; 10 extra duplicate customers; 100 NULL order `customer_id`; 200 NULL order `product_id`; 50 orphan `customer_id`; 30 orphan `product_id`; 20 extra duplicate orders.

Uniqueness flags **all copies** (20 customer rows, 40 order rows). Optional 30 future signups are separate. Type failures on committed data: 0.

## AI workflow artifacts

Prompt history lives under [`ai-prompts/`](ai-prompts/). Start at [`ai-prompts/prompt-index.md`](ai-prompts/prompt-index.md). Entries record prompt, response summary, accept/change/reject, validation, and final decision. They are not fabricated.

## Responsible AI

Data is synthetic. No real customer PII, credentials, tokens, or production connection details belong in this repository or in AI prompts. See [`tool-workflow.md`](tool-workflow.md).

## Working rules

Every meaningful change must be derived from the written spec, tested, reviewed against requirements, recorded in `ai-prompts/`, and committed with a descriptive message.
