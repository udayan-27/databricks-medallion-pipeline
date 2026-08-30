# Debugging notes

Stage 2 (data generation) produced no runtime defect/debug cycle.

The generator’s built-in validation passed on the first seed-42 run. Unit tests (`python -m unittest tests.test_generate_sample_data -v`) were 14/14 OK. A tautological validation check (`customer_id is None and False`) was removed in a senior review pass; it did not hide a real data bug and did not require regenerating CSVs.

## Stage 3 / Bronze — review findings (no Spark runtime)

PySpark and a JDK are not installed in the implementation environment, so these items were found by code review, not by a failed Spark job.

### 1. FAILED ingest metadata always recorded `row_count = 0`

- **Symptom:** `_record_failure_metadata` hard-coded `row_count=0` even if the entity table had already been written and a later verification step failed.
- **Expected vs actual:** FAILED metadata should record the last observed count when one exists; SUCCESS is still refused.
- **Root cause:** Failure handler ignored the count already computed in `ingest_entity`.
- **Files changed:** `src/bronze/ingest_core.py`
- **Tests re-run:** `python -m unittest tests.test_bronze_contract tests.test_bronze_ingest -v` → `Ran 54 tests in 0.039s` **OK (skipped=21)**.
- **AI suggestion:** accepted the review catch and applied the fix; did not invent a Spark failure.

### 2. Default catalog name `main` would be environment-specific

- **Symptom (prevented):** `design-notes.md` listed `main` as a possible catalog default. Hard-coding it would fail local HMS and non-`main` UC workspaces.
- **Decision:** `MEDALLION_CATALOG` is unset by default; tables qualify as `bronze.customers` until a catalog is provided.
- **Not a runtime defect.** Documented as a justified config choice.

### 3. Local Spark / Databricks execution was BLOCKED (pre-setup)

- **Symptom:** `import pyspark` fails; `java` is not on PATH.
- **Expected vs actual:** Bronze code is present; Bronze tables were not created in this environment.
- **Root cause:** environment, not ingest logic.
- **Action:** Spark integration tests skip with an explicit BLOCKED message. Do not treat skips as PASS.

## Local Spark environment setup (2026-08-31)

Goal: isolated Python 3.11 + JDK 17 + PySpark 3.5.6 for local Spark testing. System Python 3.12.7 and 3.7.9 were not changed. No Bronze ingest logic was edited.

### 4. Temurin JDK 17 has no winget user-scope installer

- **Symptom:** `winget install --id EclipseAdoptium.Temurin.17.JDK --source winget --scope user --disable-interactivity` failed with "No applicable installer found".
- **Expected vs actual:** user-scope JDK 17; package only publishes a machine-scope WiX MSI.
- **Root cause:** winget installer scope, not a project bug.
- **Action:** installed with `--scope machine` after approval. Actual path: `C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot`. The MSI also set machine `JAVA_HOME`. Session `PATH` still prepends `%JAVA_HOME%\bin`.
- **Tests re-run:** `java -version` / `javac -version` → OpenJDK / javac 17.0.20.1.

### 5. In-memory Spark smoke test passed; Bronze parquet ingest did not

- **Symptom:** `SparkSession` `local[2]` `createDataFrame` / `count()` / `collect()` / `stop()` succeeded with `PYSPARK_PYTHON` = `.venv\Scripts\python.exe`. Full Bronze ingest tests failed.
- **Expected vs actual:** PySpark 3.5.6 imported as 3.5.6. Ingest tests were expected to run once PySpark+JDK existed; they ran (not skipped) and failed.
- **Root cause (two environment issues, not Databricks):**
  1. `spark_input_path()` uses `Path.resolve().as_uri()`, which percent-encodes spaces (`file:/D:/DE%20C1%20Project-Udayan%20Mahajan/...`). Hadoop on this Windows runtime cannot open that URI. The same files **are** readable via the unquoted URI, POSIX path, or Windows path. A `subst` drive did not help because `Path.resolve()` expands it back to `D:\DE C1 ...`.
  2. `CREATE SCHEMA` / parquet `saveAsTable` / `mkdirs` call Hadoop `winutils`. Hadoop/winutils was **not** installed (explicitly out of this stack). Writes fail with `HADOOP_HOME and hadoop.home.dir are unset`.
- **Files changed:** none of `src/bronze/` (not hiding an environment failure as an ingest bug).
- **Tests re-run:**
  - `python -m unittest tests.test_bronze_ingest -v` → `Ran 5 tests in 7.553s` **FAILED (failures=1, errors=2)**. `TestFixtureIngest` and `TestCommittedSourceIngest` errored in `setUpClass` (`PATH_NOT_FOUND` on `%20` URIs). `test_header_only_fails` failed because the exception was path-not-found instead of `0 data rows`. Four other `TestIngestErrors` cases passed. 0 skipped.
  - `python -m unittest tests.test_bronze_contract -v` → `Ran 33 tests in 0.033s` **OK**.
  - Combined: `Ran 38 tests in 7.636s` **FAILED (failures=1, errors=2)**.
- **Optional local job:** `python src/bronze/ingest_all.py --table-format parquet` was **not** run because the Spark ingest suite did not succeed.
- **Not claimed:** Databricks runtime, Delta, DBFS, Unity Catalog, or a passing local parquet Bronze write.

Use this file during later stages to capture:

- Symptom
- Expected vs actual
- Root cause
- Files changed
- Tests re-run and **actual** results
- Whether the AI suggestion was accepted, changed, or rejected

Do not invent debugging sessions.
