# Bronze-layer prompts

Stage 3 (user numbering) / Stage 4 (task-breakdown numbering): Bronze ingest implementation.

---

## Prompt 1 — Stage 3 Bronze ingestion

### PROMPT SENT

The user asked to start Stage 3 — Bronze Layer. Stages 1–2 are complete. Instructions: do not restart Git; do not regenerate sample data unless a concrete defect is found; do not change frozen requirements/design without a documented justified reason.

Required reading: `DE_C1_REQUIREMENTS.md`, `requirements-analysis.md`, `design-notes.md`, `data-model.md`, `data-quality-strategy.md`, `cursor-workflow/project-context.md`, `cursor-workflow/spec.md`, `cursor-workflow/cursor-rules-or-instructions.md`, `cursor-workflow/task-breakdown.md`, the Stage 2 generator and notes, generator tests, committed CSVs, and the four Bronze stubs.

Objective: implement Databricks/PySpark Bronze ingest for customers, orders, and products in:

- `src/bronze/01_ingest_customers.py`
- `src/bronze/02_ingest_orders.py`
- `src/bronze/03_ingest_products.py`
- `src/bronze/ingest_all.py`

Core rules: explicit schema from `data-model.md`; preserve source values; no cleaning; no dropping duplicates/NULLs/orphans; no Silver filters; capture ingest metadata (source path, timestamp, row count); `_ingest_row_id` as lineage only; configurable paths (no machine-specific hard-coding); clear errors; orchestrate without duplicating logic; tests for retention/schema/metadata/orchestration; adversarial review; record this prompt file; update task-breakdown and README with supported facts; then one commit `feat: add Bronze ingestion pipeline`. Do not start Silver.

### AI RESPONSE SUMMARY

Implemented a shared PySpark ingest core (`src/bronze/ingest_core.py`) plus Spark-free contracts (`src/bronze/contracts.py`) and `src/config.py`. The numbered scripts and `ingest_all.py` are thin CLIs over reusable functions. Explicit schemas match `data-model.md` (`DECIMAL(18,2)`, `DATE`, nullable source fields). CSV read is `PERMISSIVE` with `nullValue=""`, `inferSchema=false`, no drop-malformed mode. Entity tables are `{catalog?}{bronze_schema}.customers|orders|products` (not flattened `bronze_customers` names). `_ingest_row_id` is `monotonically_increasing_id()` per execution so exact duplicate rows stay distinguishable. `bronze.ingest_metadata` is append-only. Local vs Databricks is parameterized (`MEDALLION_DATA_PATH`, optional catalog, `delta` default / `parquet` for local Spark without Delta). Tests split into always-on contract tests and Spark integration tests that skip when PySpark is missing.

### ACCEPTED

- Frozen table names from design: `bronze.customers`, `bronze.orders`, `bronze.products`, `bronze.ingest_metadata`.
- Explicit schema on read; PERMISSIVE; empty CSV fields become SQL NULL; no trim.
- Source columns unchanged; no drop/dedupe/fill/FK repair; no Silver flags on Bronze.
- `_ingest_row_id` as technical lineage, unique per physical row of a write, not a business key.
- Full-refresh overwrite of entity tables; append-only metadata; one `ingest_id` per `ingest_all` run.
- Preflight of all three sources before any overwrite.
- Config/env/CLI for paths and schema names; SQL identifiers validated.
- `tests/` split: unit-testable contracts vs Spark runtime tests.
- Stage 2 CSVs left untouched.

### CHANGED

- Did **not** default `MEDALLION_CATALOG` to `main`. That name is environment-specific; unset catalog uses HMS-style `bronze.customers`. Set the env var on Unity Catalog workspaces.
- Shared logic lives in `ingest_core.py` because `01_ingest_*.py` cannot be imported as Python modules (leading digits). The required files remain the CLI entry points.
- `_ingest_row_id` is generated **per ingestion execution**, not as a deterministic hash of business columns. Exact Stage 2 duplicate rows would collide under a content hash and hide duplicates.
- Local Spark write format is configurable (`parquet`) because `delta` requires Databricks or `delta-spark`. Default remains `delta` for Databricks.
- FAILED metadata records the last observed row count instead of always writing `0` (review fix before any Spark run).
- Write errors for missing Delta mention `--table-format parquet` for local Spark.

### REJECTED

- Flattened names `bronze_customers` in the default schema: the frozen design uses layer schema + entity table.
- `DROPMALFORMED` / `dropna` / `dropDuplicates` / quality filters in Bronze.
- Deterministic `_ingest_row_id` from hashed business keys (would collapse exact duplicates).
- Hard-coded Windows paths, `/Workspace/Users/...`, workspace URLs, or credentials.
- Streaming, Autoloader, or extra orchestration frameworks.
- Installing PySpark/JDK into this environment just to force a local Spark run (no Java on the machine; a failed install would not be a Databricks pass).
- Starting Silver.
- Regenerating `data/*.csv`.
- Claiming Spark/Databricks ingest passed when it was not executed.

### VALIDATION

Commands actually run (2026-08-31):

1. `python -c "import pyspark"` — **blocked**. `ModuleNotFoundError: No module named 'pyspark'`.

2. `java -version` — **blocked**. Java is not installed (`JAVA_HOME` empty, `java` not on PATH).

3. `python -m unittest tests.test_bronze_contract tests.test_bronze_ingest tests.test_generate_sample_data -v`

   First run: `Ran 66 tests in 67.080s` → **OK (skipped=21)**. All 21 skips were Spark ingest tests (`tests.test_bronze_ingest`) with reason PySpark missing. Generator tests still 14 OK. Bronze contract tests OK.

4. After review fixes (FAILED metadata `row_count`, fixture contract tests): `python -m unittest tests.test_bronze_contract tests.test_bronze_ingest -v`

   `Ran 54 tests in 0.039s` → **OK (skipped=21)**. 33 Bronze contract tests passed, including fixture defect-shape tests and stub-replacement checks.

5. Databricks cluster / Unity Catalog / Delta / DBFS-or-volume read: **not executed**. Recorded as BLOCKED.

No Spark row-count comparison against Bronze tables was obtained in this environment. Source CSV physical counts (header excluded) remain 10,010 / 100,020 / 500; those are the expected Bronze counts if ingest is faithful. They are not claimed as Spark-measured Bronze counts.

No test was marked PASS without being executed. No validation was fabricated.

### FINAL DECISION

Keep this Bronze implementation. Do not start Silver until requested. On a Databricks cluster, run `src/bronze/ingest_all.py` with `MEDALLION_DATA_PATH` (and `MEDALLION_CATALOG` if using UC) and execute `tests.test_bronze_ingest` (or the same ingest against `data/`) before treating runtime ingest as PASS. Local contract tests are the supported evidence in this environment.

---

## Prompt 2 — Local Windows Spark/parquet validation (cross-platform path + no winutils)

### PROMPT SENT

Continue DE C1. Bronze is committed (`a581cbc`). Local Spark (Python 3.11.9, JDK 17, PySpark 3.5.6) is installed. In-memory smoke passes. File-based Bronze integration tests fail on Windows: percent-encoded `file:` URIs from `Path.as_uri()`, and Hadoop `HADOOP_HOME` / winutils unset. Contract tests 33/33. Ingest: 38 run, 4 pass, 1 failure, 2 setUpClass errors. Do not start Silver; do not change Stage 2 CSVs; do not weaken Bronze; do not skip tests; do not change the raw-data contract. Diagnose whether the two problems are independent; design a cross-platform local input path helper; investigate whether parquet tests can run without winutils via configuration; preserve Databricks design; fix local test infrastructure only as needed; validate Bronze behaviour; document the debugging cycle; commit `fix: make local Bronze Spark validation cross-platform` if source/test/config changes are required.

### AI RESPONSE SUMMARY

Diagnosed two independent Windows Hadoop issues from stack traces and isolated Spark experiments. Fixed local input paths (`spark_input_path` now uses `as_posix()`, remote URIs pass through). Hadoop 3.3.4 has no skip-permissions config; dummy winutils is not a valid exe; pre-creating warehouse dirs is not enough for `saveAsTable`. A tiny Java `RawLocalFileSystem` subclass compiled with the project JDK lets local Windows Spark mkdir/list/commit parquet without native binaries. Databricks still uses the existing cluster session. Winutils was not installed. Stage 2 CSVs unchanged. Silver was not started.

### ACCEPTED

- Cross-platform `spark_input_path` in `contracts.py` (posix local paths; dbfs/s3/abfss/Volumes passthrough).
- Windows-only `fs.file.impl` adapter for locally created SparkSessions.
- Path helper unit tests; Spark ingest tests executed rather than skipped.
- Honest local vs Databricks distinction.

### CHANGED

- Moved `spark_input_path` from `ingest_core.py` into `contracts.py` so it can be tested without Spark.
- `get_spark_session` and ingest tests call `apply_local_spark_config`.

### REJECTED

- Installing winutils/hadoop.dll from third-party GitHub trees.
- Empty dummy `winutils.exe`.
- Adding `hadoop-bare-naked-local-fs` as a Maven dependency.
- Skipping or weakening ingest tests.
- Percent-encoded `file:` URIs.
- Hard-coded Windows user paths.
- Starting Silver.
- Regenerating `data/*.csv`.

### VALIDATION

1. Isolated reads: `as_uri()` FAIL; posix/str/unquoted file URI OK. Zero-space `as_uri()` OK.
2. `CREATE SCHEMA` fails on `setPermission`/winutils. Dummy HADOOP_HOME does not help. Empty exe → error 193.
3. Custom FileSystem: `CREATE SCHEMA` OK, then `listStatus` NativeIO failure; after `listStatus` override, `saveAsTable` count=4 and overwrite OK.
4. `python -m unittest tests.test_bronze_contract -v` → 37 OK.
5. `python -m unittest tests.test_bronze_ingest -v` → 21 OK (47.830s).
6. Combined → 58 OK (46.761s).
7. CSV SHA-256 matches `DATA_GENERATION_NOTES.md`.
8. Databricks / Delta / UC: **not executed**.

### FINAL DECISION

Keep the local path helper and Windows-only FileSystem adapter. Do not install winutils. Do not start Silver. Databricks Bronze remains unproven until a workspace job is run.

---

## Prompt 3 — 2026-08-31 — Databricks workflow reuses existing Bronze ingest

### PROMPT SENT

Same interaction as `ai-prompts/documentation.md` Prompt 7 / P016: automate Databricks bootstrap and validation. Bronze must run via existing `src/bronze/ingest_all.py`, not a duplicated notebook transform.

### AI RESPONSE SUMMARY

`src/databricks/run_pipeline.py` calls `ingest_core.ingest_all` with `PipelineConfig` pointing at `/Volumes/workspace/de_c1/source_data`, catalog `workspace`, `table_format=delta`. Bronze validation queries actual tables for existence, Delta format, row counts, NULL/duplicate/orphan preservation, `_ingest_row_id` uniqueness, and ingest metadata. Ingest logic was not copied into `src/databricks/`.

### ACCEPTED

Reuse `ingest_all`. Keep entity overwrite + append-only metadata.

### CHANGED

None of the Bronze transform modules.

### REJECTED

Notebook-duplicated ingest; regenerating CSVs; claiming Databricks Bronze was executed.

### VALIDATION

Covered by the P016 sequential suite (**223 tests in 534.573s OK**). Bronze Spark tests remain part of that suite.

### FINAL DECISION

Bronze application code stays the source of truth. Databricks execution still not run.

---

## Prompt 4 — 2026-08-31 — Closeout: Databricks Bronze PASS (no Bronze code change)

### PROMPT SENT

Same interaction as `ai-prompts/documentation.md` Prompt 8 / P017. Record the actual workspace Bronze PASS. Do not modify ingest logic.

### AI RESPONSE SUMMARY

No Bronze source change. Documented Databricks Bronze: customers/orders/products 10,010 / 100,020 / 500; all Delta; `_ingest_row_id` unique; NULL/duplicates/orphans preserved; metadata SUCCESS.

### ACCEPTED

Reuse existing `ingest_all`. Treat the workspace run as Databricks evidence, distinct from local parquet.

### CHANGED

Documentation/current-status only.

### REJECTED

Rewriting ingest; claiming Cursor ran Bronze in this closeout chat; regenerating CSVs.

### VALIDATION

Local closeout suite **223/223 OK**. Stage 2 SHA-256 unchanged. Databricks Bronze PASS is the candidate’s workspace result.

### FINAL DECISION

Bronze application code stays the source of truth. Databricks Bronze is complete.
