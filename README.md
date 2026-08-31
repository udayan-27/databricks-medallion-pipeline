# Databricks Medallion Pipeline — E-commerce Sales

This repository is the DE C1 AI Capability Exercise submission. It implements (when complete) a Databricks Medallion Architecture pipeline:

`CSV -> Bronze -> Silver -> Gold -> Dashboard`

Requirements and architecture are written. Stage 2 sample data has been generated. **Bronze ingest code is implemented.** **All five Silver quality modules and Silver table orchestration are implemented** (local Spark / parquet). **Gold SQL aggregations and `create_gold_tables.py` are implemented** (local Spark / parquet). **Dashboard SQL queries and `DASHBOARD_GUIDE.md` are implemented** (local Spark / parquet). A Databricks SQL dashboard has **not** been rendered. Bronze/Silver/Gold tables have **not** been created in a Databricks workspace from this environment.

Canonical requirements: [`DE_C1_REQUIREMENTS.md`](DE_C1_REQUIREMENTS.md).

## Status

| Area | Status |
|---|---|
| Repository structure | Initialized |
| Requirements analysis, architecture, data model, DQ strategy | Written (design stage) |
| Sample CSV data | Generated (10,010 / 100,020 / 500 rows; seed 42) |
| Bronze ingest code | Implemented (`src/bronze/`, `src/config.py`) |
| Local Spark runtime | Python 3.11 `.venv` + Temurin JDK 17 + PySpark 3.5.6 (isolated from system Python 3.12) |
| Local Spark validation | In-memory smoke **passed**. Local parquet Bronze ingest tests **passed** (21/21). Silver Spark tests **passed** (55/55). Gold Spark tests **passed** (16/16 aggregations + 11/11 contract). Not Databricks. |
| Bronze Databricks / Delta / Unity Catalog | **Not run** from this environment |
| Silver completeness / uniqueness / type / RI / business logic | Implemented (`src/silver/01_quality_completeness.py` through `05_quality_business_logic.py`, `quality_common.py`, `create_silver_tables.py`). Local Spark validated. Combined Silver tables written in tests as parquet. |
| Gold aggregations | Implemented (`src/gold/01_sales_by_product.sql` through `04_customer_segmentation.sql`, `create_gold_tables.py`). Local Spark / parquet validated. Databricks Gold **not** run. |
| Dashboard | Queries + guide implemented (`src/dashboard/dashboard_queries.sql`, `DASHBOARD_GUIDE.md`). Local Spark query tests against Gold parquet. Databricks SQL Dashboard UI **not** rendered. |
| Tests | Generator **14/14 OK**; Bronze contract **38/38 OK**; Spark ingest **21/21 OK**; Silver contract **20/20 OK**; Silver Spark **55/55 OK**; Gold contract **11/11 OK**; Gold Spark **16/16 OK**; Dashboard contract **15/15 OK**; Dashboard Spark **13/13 OK**. Sequential generator/Bronze/Silver **148/148 OK**; sequential Gold **27/27 OK**; sequential Dashboard **28/28 OK**; combined relevant **203/203 OK** (0 failed, 0 errors, 0 skipped) in **548.539s**. |

Completeness, uniqueness, type validation, RI, and business logic run on Bronze DataFrames locally. `create_silver_tables.py` writes combined Silver tables and `silver.quality_metrics` (local parquet in tests). `create_gold_tables.py` executes the Gold SQL files against Silver and overwrites Gold tables (local parquet in tests). The CSVs in `data/` are real generated inputs.

## What this exercise evaluates

The submission must demonstrate requirement analysis, architecture, AI-assisted implementation, data quality engineering, testing, validation of AI output, debugging, documentation, responsible AI usage, human ownership of technical decisions, meaningful Git history, and complete AI prompt history.

## Layers

- **Bronze:** raw, unchanged CSV ingest into `bronze.customers`, `bronze.orders`, `bronze.products`, plus append-only `bronze.ingest_metadata`. Source columns are not cleaned. `_ingest_row_id` is ingest lineage (unique per physical row of a write; regenerated each run).
- **Silver:** five quality modules (completeness, uniqueness, type validation, referential integrity, business logic). Bad rows are flagged, not deleted. **All five modules plus `create_silver_tables.py` are implemented.** They preserve every Bronze physical row and write combined `quality_check_result` / `failed_checks` plus `silver.quality_metrics`. Local parquet validated; Databricks Silver is **not** run.
- **Gold:** business aggregations in SQL (sales by product, revenue by customer, daily/weekly trends, customer segmentation). **Implemented.** Qualifying orders: `order_status = 'Completed' AND quality_check_result = 'PASS'`. Local parquet validated; Databricks Gold is **not** run.
- **Dashboard:** Databricks SQL dashboard queries for three required tiles (bar / histogram / pie) plus two Gold-field filters. **Queries implemented and locally tested.** Databricks SQL Dashboard UI has **not** been rendered from this environment. See `src/dashboard/DASHBOARD_GUIDE.md`.

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

### Silver quality modules and tables

These modules read Bronze DataFrames (or already-written Bronze tables) and **add** quality columns. They do not delete rows. `create_silver_tables.py` runs all five modules, combines flags on `_ingest_row_id`, and writes `silver.customers` / `orders` / `products` plus `silver.quality_metrics`.

| Setting | Purpose | Default |
|---|---|---|
| `--bronze-schema` | Schema of the Bronze tables to read | `bronze` |
| `--silver-schema` | Schema of the Silver tables to write | `silver` |
| `--table-format` | Must match the Bronze write format | `delta` (use `parquet` locally) |

After a local Bronze parquet ingest:

```
python src/silver/01_quality_completeness.py --table-format parquet
python src/silver/02_quality_uniqueness.py --table-format parquet
python src/silver/03_quality_type_validation.py --table-format parquet
python src/silver/04_quality_referential_integrity.py --table-format parquet
python src/silver/05_quality_business_logic.py --table-format parquet
python src/silver/create_silver_tables.py --table-format parquet
```

Observed local counts on seed-42 data: 50 NULL emails, 100 NULL order `customer_id`, 200 NULL order `product_id`, 20 customer uniqueness-fail rows, 40 order uniqueness-fail rows, **0 type failures**, **50 orphan customer_id**, **30 orphan product_id**, **30 future signup** business-logic failures (optional; not in the 460), **0** other frozen business-rule failures (`order_not_before_signup` = 0). Rows remain (10,010 / 100,020 / 500). Combined customer FAIL rows = 100; order FAIL rows = 420 (disjoint classes on this seed). Malformed type cases use `tests/fixtures/silver/type_validation/`. Business-rule cases use `tests/fixtures/silver/business_logic/`.

Tests (same local Spark stack as Bronze):

```
python -m unittest tests.test_silver_contract -v
python -m unittest tests.test_silver_quality -v
```

Run **one Spark unittest process at a time**. A second concurrent full suite can fail during Spark JVM gateway launch on Windows (`PermissionError` on a Py4J temp connection-info file). That is an environment/concurrency issue, not a Bronze/Silver/Gold logic failure. Sequential isolation uses a unique warehouse and `spark.local.dir` per test class; it does not make overlapping suites supported. See `debugging-notes.md`.

### Gold aggregations

Gold reads Silver only. Eligibility is explicit in each SQL file:

`order_status = 'Completed' AND quality_check_result = 'PASS'`

`lifetime_value_actual` is the customer's qualifying order revenue, not source `customers.lifetime_value`. `revenue_by_customer` includes every canonical customer (including zero qualifying orders). Segmentation uses Inactive → High-Value (>= 1000.00) → Repeat → One-Time.

| Setting | Purpose | Default |
|---|---|---|
| `--silver-schema` | Schema of the Silver tables to read | `silver` |
| `--gold-schema` | Schema of the Gold tables to write | `gold` |
| `--table-format` | Must match the Silver write format | `delta` (use `parquet` locally) |
| `--catalog` | Unity Catalog name | unset |

After a local Bronze + Silver parquet run:

```
python src/gold/create_gold_tables.py --table-format parquet
```

Tests (same local Spark stack):

```
python -m unittest tests.test_gold_contract -v
python -m unittest tests.test_gold_aggregations -v
```

Or one sequential relevant suite (do not start a second copy while this is running):

```
python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality tests.test_gold_contract tests.test_gold_aggregations tests.test_dashboard_contract tests.test_dashboard_queries -v
```

Observed local fixture reconciliation: 17 qualifying orders, revenue 3330.00; duplicate order copies, NULL/orphan FKs, and FAIL rows excluded. Seed-42: Gold product/customer/daily/weekly/segment totals match eligible Silver; 10,000 canonical customers. Databricks Gold tables have **not** been written.

### Dashboard queries

Dashboard datasets read Gold only. Queries live in `src/dashboard/dashboard_queries.sql`. Workspace click-path, viz types, histogram binning, and filters are in `src/dashboard/DASHBOARD_GUIDE.md`.

| Tile | Gold table | Visualization |
|---|---|---|
| Top 10 products by revenue | `gold.sales_by_product` | bar |
| Customer revenue distribution | `gold.revenue_by_customer` (`lifetime_value_actual`, all canonical customers including zeros) | histogram (bins in the Databricks viz, not SQL) |
| Customer segmentation | `gold.customer_segmentation` | pie |

Filters (Gold fields only): `category` on Tile 1 as a query parameter **before** `LIMIT 10`; `customer_segment` on Tile 2. Date range is not a filter on these tiles (no date grain on those Gold tables).

Local tests:

```
python -m unittest tests.test_dashboard_contract -v
python -m unittest tests.test_dashboard_queries -v
```

Or one sequential relevant suite (do not start a second copy while this is running):

```
python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality tests.test_gold_contract tests.test_gold_aggregations tests.test_dashboard_contract tests.test_dashboard_queries -v
```

A Databricks SQL dashboard has **not** been created or rendered from this environment. Local `spark.sql` against parquet is not Databricks SQL / Delta / Unity Catalog validation.

This cycle’s sequential full suite: **Ran 203 tests in 548.539s OK**.

### Tests that run without Spark

```
python -m unittest tests.test_bronze_contract tests.test_silver_contract tests.test_gold_contract tests.test_dashboard_contract -v
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
- `src/dashboard/dashboard_queries.sql` — Gold-only tile queries and filter-value lookups
- `src/dashboard/DASHBOARD_GUIDE.md` — workspace steps; local vs Databricks
