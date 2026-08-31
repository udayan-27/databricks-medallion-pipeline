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

## Local Windows Spark/parquet validation (2026-08-31)

Two independent defects blocked file-based Bronze tests. In-memory Spark already worked. Winutils was **not** installed.

### 6. Percent-encoded `file:` URIs (`PATH_NOT_FOUND`)

- **Symptom:** `reader.load(spark_path)` in `read_source_csv` (`src/bronze/ingest_core.py`) failed with `[PATH_NOT_FOUND] Path does not exist: file:/D:/DE%20C1%20Project-Udayan%20Mahajan/...`. `TestFixtureIngest` / `TestCommittedSourceIngest` errored in `setUpClass`. `test_header_only_fails` failed because it received path-not-found instead of `0 data rows`.
- **Expected vs actual:** Spark should read the local CSV. The file exists; Python `csv` header reads succeed. Hadoop cannot open the percent-encoded URI.
- **Evidence (isolated):** same fixture CSV:
  - `Path.as_uri()` → `file:///D:/DE%20C1%20Project-Udayan%20Mahajan/...` → **FAIL**
  - `Path.as_posix()` / `str(Path)` / unquoted `file:///D:/DE C1 ...` → **OK, count=4**
  - A zero-space path (`C:/dec1_spark_wh/customers.csv`) **succeeds** even as `as_uri()`, so the bug is encoding, not `file:` itself.
  - CSV **read** does not require winutils (warning only).
- **Root cause:** `spark_input_path()` used `Path.resolve().as_uri()`. Hadoop 3.3.4 on this Windows runtime treats `%20` as literal path characters.
- **Independent of Hadoop writes:** yes.

### 7. Hadoop `mkdirs` / parquet commit need native Windows binaries

- **Symptom:** `ensure_bronze_schema` → `CREATE SCHEMA` → `RawLocalFileSystem.setPermission` → `Shell.getWinUtilsPath` → `HADOOP_HOME and hadoop.home.dir are unset`. After URI fix, `saveAsTable` / `write.parquet` still fail.
- **Exact trigger:** `RawLocalFileSystem.mkOneDirWithMode` → `setPermission` → winutils `chmod`. After a no-op `setPermission`, parquet commit then failed on `FileUtil.list` → `NativeIO$Windows.access0` (`hadoop.dll`).
- **Evidence:**
  - Pre-creating `{warehouse}/{schema}.db` made `CREATE SCHEMA` succeed (`exists` skips `mkdirs`) but `saveAsTable` still failed on table-dir `mkdirs`.
  - Dummy `HADOOP_HOME` with empty `bin/` → "Could not locate Hadoop executable: ...\winutils.exe".
  - Empty `winutils.exe` file → `CreateProcess error=193` (not a valid Win32 application).
  - Hadoop 3.3.4 `LocalFileSystemConfigKeys` has no skip-permissions flag. HADOOP-17839 (disable local permission get/set) is unresolved.
- **Root cause:** Spark uses Hadoop for local files. On Windows, Hadoop 3.3.4 shells out to winutils for chmod and uses NativeIO for directory listing during `FileOutputCommitter.commitJob`. Configuration-only workarounds do not exist in this distribution.
- **Affects:** local Windows testing / local CLI parquet only. Databricks uses the cluster session (DBFS/S3/UC), not this FileSystem.
- **Rejected:** installing third-party winutils/hadoop.dll; empty dummy exe; skipping tests; changing Bronze to temp-views; redesigning around Windows.
- **Accepted:** compile a tiny Java `RawLocalFileSystem` subclass (`setPermission`/`setOwner` no-op; `listStatus`/`getFileStatus` via `java.io.File`) with the project JDK and set it as `fs.file.impl` **only** for locally created SparkSessions (`os.name == "nt"`). Diagnostic `saveAsTable` + overwrite then returned count=4 without winutils.

### 8. Implementation and re-test

- **Files changed:** `src/bronze/contracts.py` (`spark_input_path` → `as_posix()`; remote URIs pass through), `src/spark_local.py`, `src/local_runtime/NoWinutilsRawLocalFileSystem.java`, `src/bronze/ingest_core.py` (`get_spark_session` applies the helper), `tests/test_bronze_ingest.py`, `tests/test_bronze_contract.py`.
- **Tests re-run:**
  - `python -m unittest tests.test_bronze_contract -v` → `Ran 37 tests in 0.040s` **OK**.
  - `python -m unittest tests.test_bronze_ingest -v` → `Ran 21 tests in 47.830s` **OK**.
  - Combined → `Ran 58 tests in 46.761s` **OK**.
- **Bronze local result:** customers 10010, orders 100020, products 500; duplicate keys, NULLs, orphans retained; `_ingest_row_id` unique; metadata SUCCESS row counts match; missing/empty/header-only/header-mismatch/preflight failures still fail.
- **Winutils:** not installed, not committed, not required for this local path.
- **Still not claimed:** Databricks, Delta, DBFS, Unity Catalog.

## Stage 4 Silver business logic and orchestration (2026-08-31)

### 9. Canonical parent window used pre-alias column names

- **Symptom (caught in review before Spark tests):** `canonical_customer_signup` selected aliased `_bl_customer_id` then partitioned the window by `customer_id`, which no longer existed on that projection.
- **Expected vs actual:** lookup must be unique per `customer_id` using `min(_ingest_row_id)` so order joins cannot fan out.
- **Root cause:** window defined on original names after `select` aliases.
- **Files changed:** `src/silver/05_quality_business_logic.py` (partition/order by `_bl_customer_id` / `_bl_parent_ingest_id`).
- **Tests:** subsequent Spark suite exercised the lookup (duplicate parent signup disagreement; seed-42 `order_not_before_signup` = 0).

### 10. Fixture metric test compared the wrong populations

- **Symptom:** `test_ri_does_not_fan_out_and_metrics_reconcile` failed with `AssertionError: 7 not greater than 12`.
- **Expected vs actual:** the test assumed three business-rule fail counts would exceed table-outcome FAIL rows. On the small BL fixture, completeness/RI also fail, so outcome FAIL = 12 and those three BL rules sum to 7.
- **Root cause:** test assertion, not Silver implementation. Rule-level sums are issue instances; table-outcome is distinct physical FAIL rows.
- **Files changed:** `tests/test_silver_quality.py` only.
- **Tests re-run:** `python -m unittest tests.test_silver_quality -v` → **55/55 OK**. Combined relevant set **147/147 OK**.

### 11. `current_date()` string in a docstring

- **Symptom:** Spark-free contract test `assertNotIn("current_date()", bl_src)` failed because the module docstring said not to use that function.
- **Root cause:** static string match, not a Spark `current_date()` call. As-of date is the frozen `date(2026, 8, 31)` literal.
- **Files changed:** docstring/comment wording in `05_quality_business_logic.py`.
- **Not a runtime defect.**

## Stage 5 Gold (2026-08-31)

Local Spark / parquet. Databricks Gold was not run.

### 12. Contract tests matched documentation comments, not executable SQL

- **Symptom:** `test_sales_by_product_columns_and_eligibility` failed on comment text `(not SUM(quantity))`. `test_orchestrator_does_not_reimplement_aggregations` failed because the orchestrator docstring mentioned ``F.sum``.
- **Expected vs actual:** executable Gold SQL must not `SUM(quantity)`; the orchestrator must not call PySpark `groupBy`/`F.sum`. Comments explaining those prohibitions are not violations.
- **Root cause:** naive substring checks over the whole file, including headers.
- **Files changed:** `src/gold/01_sales_by_product.sql` comment wording; `src/gold/create_gold_tables.py` docstring; `tests/test_gold_contract.py` strips `--` comments before the quantity-sum check. AST test already proved no `groupBy`.
- **Not a Gold aggregation defect.** Tests were not weakened: executable SQL is still checked.

### 13. Holding `spark.table()` DataFrames across a Gold overwrite

- **Symptom:** `test_repeated_gold_execution_is_stable` overwrote Gold parquet; later fixture tests (and the exceptAll in that test) raised `SparkFileNotFoundException` on the previous `part-*.snappy.parquet`.
- **Expected vs actual:** a second Gold run must replace files; readers must use the new snapshot. Holding a `spark.table()` scan from `setUpClass` keeps the old file list.
- **Root cause:** test harness, not double-counting in SQL. Gold overwrite is the documented full-refresh behaviour.
- **Files changed:** `tests/test_gold_aggregations.py` re-reads tables via `_gold()` after each write; repeated-run test snapshots rows with `collect()` before overwrite.
- **Tests re-run:** Gold suite **27/27 OK**, then combined **174/174 OK**.

### 14. Overwriting Silver orders while reading the same table

- **Symptom:** `TestGoldZeroEligibleOrders.setUpClass` failed with `UNSUPPORTED_OVERWRITE.TABLE` on `gzero_silver.orders`.
- **Expected vs actual:** the zero-eligible case needs an empty orders table; Spark refuses `saveAsTable` of a DataFrame that still reads that table (`limit(0)` is still a scan of the target).
- **Root cause:** overwrite-while-reading. Not a Gold SQL defect.
- **Files changed:** `tests/test_gold_aggregations.py` builds `createDataFrame([], schema=...)` so the write does not read the target.
- **Rejected:** deleting Silver FAIL rows to create a zero-eligible population.

## Stage 5 Gold QA — concurrent Spark suite / Windows temp PermissionError (2026-08-31)

Not a Gold aggregation defect. Production Gold SQL and `create_gold_tables.py` were not changed.

### Symptom

A full relevant unittest process failed during Bronze Spark startup:

`ERROR: setUpClass (tests.test_bronze_ingest.TestCommittedSourceIngest)`

The class never reached ingest. The traceback ended in PySpark JVM gateway launch:

```
pyspark/java_gateway.py launch_gateway
  with open(conn_info_file, "rb") as info:
PermissionError: [Errno 13] Permission denied:
  C:\Users\UDAYAN~1\AppData\Local\Temp\tmp38ammwfx\tmpwmg8qj9e
```

Unittest continued. `TestFixtureIngest` started a new Spark session ~3 seconds later and passed. Silver and Gold tests in that same process also passed. Result: **Ran 170 tests in 642.476s FAILED (errors=1)**. The four `TestCommittedSourceIngest` methods were not collected after `setUpClass` failed (174 − 4 = 170).

The same command in a second process completed **Ran 174 tests in 628.243s OK**.

### Trigger

Two `python -m unittest` full-suite processes were started close together:

| Terminal | Started (UTC) | Ended (UTC) | Result |
|---|---|---|---|
| First process | 2026-08-31T01:10:49Z | 2026-08-31T01:21:33Z | 170 run, 1 error |
| Second process | 2026-08-31T01:13:50Z | 2026-08-31T01:24:21Z | 174 OK |

They overlapped for about eight minutes. That overlap is an unsupported operating mode.

Clock time of the PermissionError (Spark log `26/08/31 06:41:58` local, ≈ 01:11:58Z) is **about two minutes before** the second process started. The failing `setUpClass` is therefore not “the second suite’s Spark collided with the first suite’s warehouse files at that instant.” It is the first process’s first Spark gateway launch on Windows.

### Evidence

- Stack is `SparkSession.getOrCreate` → `SparkContext.__init__` → `launch_gateway` → `open(conn_info_file)`. No Gold SQL, no ingest, no assertion on revenue.
- PySpark 3.5.6 `java_gateway.py` creates a unique `tempfile.mkdtemp()` / `mkstemp()`, `os.unlink`s the empty file, waits for the JVM to rewrite it, then opens it. The failing path (`tmp38ammwfx\tmpwmg8qj9e`) is unique, not a shared hardcoded name.
- Immediately after the error: `ResourceWarning: subprocess ... is still running` — the failed gateway left a Java/`spark-submit` child.
- `'C1' is not recognized as an internal or external command` appears because the repo path contains a space (`DE C1 Project-...`) and `spark-submit.cmd` splits it. The **same warning** printed on the successful `TestFixtureIngest` launch. It is not the PermissionError.
- Sequential isolation already used unique `tempfile.mkdtemp(prefix="de_c1_*_wh_")` warehouse dirs per test class. `spark.local.dir` was **not** set (default = process `%TEMP%`). Warehouse dirs were not deleted in `tearDownClass`.
- Default Hive/Derby metastore still lives at repo-root `metastore_db/` (gitignored). Two overlapping Spark processes can lock that database; that is a real concurrent risk, but this traceback is the Py4J connection-info file, not Derby `XSDB6`.

### Root cause

**Spark-on-Windows JVM gateway startup + temporary-file locking**, not Gold logic and not a sequential test-assertion bug.

Py4J’s connection-info handshake is racy on Windows: Python deletes a temp file, the JVM recreates it, Python opens it. `PermissionError` means the file existed (`os.path.isfile` passed) but could not be opened — typical when the JVM still holds the handle, or when `%TEMP%` is under heavy I/O. Concurrent Spark suites make that class of failure more likely (shared `%TEMP%`, leftover JVMs, Derby). This specific stack trace fired on the **first** Spark launch of the first process, then a later launch in the same process succeeded.

Classification: Spark environment / Windows temp-file race, with concurrency as an unsupported aggravating operating mode. Not Gold SQL. Not Silver/Bronze business logic. Not a weakened or wrong unittest assertion.

### Why it is not a Gold logic defect

- Failure is Bronze `setUpClass` before `ingest_all`.
- Gold tests in the failed process still ran and passed (`test_zero_eligible_orders_keep_customers_and_empty_facts` ok).
- Clean sequential process: 174/174 OK, 0 failed, 0 skipped (Gold included).
- No change to `src/gold/*.sql` or aggregation formulas was required or made for this incident.

### Correct operating procedure

1. Run **one** Spark test process at a time. Do not start a second full suite while another Spark suite is running.
2. Sequential command (from repo root, project `.venv`, `JAVA_HOME` set):

```
python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality tests.test_gold_contract tests.test_gold_aggregations tests.test_dashboard_contract tests.test_dashboard_queries -v
```

3. Test infrastructure now isolates `spark.local.dir` (and Py4J temp) per Spark test class, retries **only** gateway `PermissionError`, and deletes the warehouse on teardown. That does not authorize concurrent suites.

### Final validation

Sequential re-run after the isolation helper (this cycle; not the previous 174):

1. `python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality -v`
   → **Ran 148 tests in 385.673s OK** (0 failed, 0 skipped). Count is 147 prior relevant tests plus 1 new Spark-free helper contract test.

2. `python -m unittest tests.test_gold_contract tests.test_gold_aggregations -v`
   → **Ran 27 tests in 122.986s OK** (0 failed, 0 skipped). Gold reconciliation remaining green.

Stage 2 CSV SHA-256 unchanged vs `DATA_GENERATION_NOTES.md`. No Gold SQL change. Databricks not run.

## Stage 6 / Dashboard — local query validation (2026-08-31)

### 11. Fresh shell had no JAVA_HOME

- **Symptom:** `python -m unittest tests.test_dashboard_queries -v` failed in `setUpClass` with `Java not found and JAVA_HOME environment variable is not set` / `JAVA_GATEWAY_EXITED`.
- **Expected vs actual:** Same local Spark stack as Bronze/Silver/Gold (Temurin 17 + `.venv` PySpark). A new agent shell does not inherit the previous session `JAVA_HOME`.
- **Root cause:** Environment, not dashboard SQL.
- **Files changed:** none of `src/dashboard/` for this item.
- **Action:** Set `JAVA_HOME` to `C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot` and prepend `%JAVA_HOME%\bin` as documented in README.
- **AI suggestion:** rejected as a dashboard logic bug.

### 12. Histogram test `AMBIGUOUS_REFERENCE` on `lifetime_value_actual`

- **Symptom:** `test_histogram_uses_gold_customer_population` errored: `Reference lifetime_value_actual is ambiguous` after joining the dashboard result to `gold.revenue_by_customer`.
- **Expected vs actual:** Dashboard query ran; Top 10, segmentation, and edge tests passed (12 OK). The join compared `lifetime_value_actual` to `total_revenue` without aliases, so Spark saw two Gold `lifetime_value_actual` columns.
- **Root cause:** Test join, not dashboard SQL (the histogram SELECT is a single-table Gold read).
- **Files changed:** `tests/test_dashboard_queries.py` (alias `dash_ltv` / `gold_rev` / `gold_ltv`).
- **Dashboard SQL:** unchanged.
- **AI suggestion:** accepted the alias fix; rejected rewriting the histogram query.
- **Tests re-run:** `python -m unittest tests.test_dashboard_queries -v` → **Ran 13 tests in 70.184s OK**. Combined dashboard: **Ran 28 tests in 69.924s OK**. Combined relevant suite: **Ran 203 tests in 548.539s OK**.

Use this file during later stages to capture:

- Symptom
- Expected vs actual
- Root cause
- Files changed
- Tests re-run and **actual** results
- Whether the AI suggestion was accepted, changed, or rejected

Do not invent debugging sessions.
