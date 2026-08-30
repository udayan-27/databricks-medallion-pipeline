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
