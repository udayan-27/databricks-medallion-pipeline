# Debugging prompts

## Prompt 1 — 2026-08-31 — Local Windows Spark/parquet validation

### PROMPT SENT

Continue DE C1 after Bronze ingest (`a581cbc`) and local Spark install. File-based Bronze integration tests fail on Windows (`Path.as_uri()` percent-encoded URIs; Hadoop `HADOOP_HOME` / winutils unset). Diagnose whether the two problems are independent. Design a cross-platform local input path helper. Investigate whether parquet tests can run without installing winutils. Do not start Silver, change Stage 2 CSVs, weaken Bronze, skip tests, or change the raw-data contract. Document the real debugging cycle. Commit only if repository changes are required.

### AI RESPONSE SUMMARY

Two independent Hadoop-on-Windows issues. Path helper switched from `as_uri()` to `as_posix()` with remote URI passthrough. Winutils is required by Hadoop 3.3.4 `setPermission` and NativeIO `listStatus` unless a Java FileSystem replaces those calls; no configuration-only skip exists in this Spark distribution. A project-compiled `NoWinutilsRawLocalFileSystem` is used only for locally created Windows SparkSessions. Tests then passed. Winutils was not downloaded.

### ACCEPTED

- Local posix path helper; Databricks URIs unchanged.
- Compile-at-session-start Java FileSystem for local Windows only.
- Execute Spark tests instead of skipping.

### CHANGED

- Path helper lives in `contracts.py` for Spark-free tests.
- Ingest tests and `get_spark_session` share `apply_local_spark_config`.

### REJECTED

- Third-party winutils binaries; dummy exe; Maven Hadoop-FS dependency; test skips; Silver.

### VALIDATION

Contract 37 OK. Ingest 21 OK. Combined 58 OK. Full Bronze row counts 10010 / 100020 / 500 with defects retained. CSV hashes unchanged. Databricks not run.

### FINAL DECISION

Record this as a real local-runtime debug cycle. Do not claim Databricks validation. Do not start Silver.

## Prompt 2 — 2026-08-31 — Gold QA: concurrent Spark suite Windows temp PermissionError

### PROMPT SENT

Close the Gold QA/debugging checkpoint before Dashboard. Gold is committed (`eb25da1`). A clean sequential full relevant suite was 174 OK / 0 failed / 0 skipped. A second full-suite process started concurrently and failed during Bronze `setUpClass` with a Windows temporary-file `PermissionError` while starting Spark. Diagnose from the actual stack trace and test setup. Do not start Dashboard. Do not change Gold business logic unless a Gold defect is proven. Choose the smallest robust fix (document sequential, isolate Spark temp, concurrency guard, or no code change). Reproduce only with a minimal controlled test if needed. Re-run a clean sequential suite and record actual counts. Update debugging notes and prompt evidence. Commit only if documentation or a genuine test-isolation change is required.

### AI RESPONSE SUMMARY

The failing stack is PySpark `java_gateway.launch_gateway` opening a unique `%TEMP%` connection-info file (`PermissionError: [Errno 13]` on `tmp38ammwfx\tmpwmg8qj9e`). That is JVM gateway startup on Windows, not Gold SQL and not a Bronze ingest assertion.

Terminal timestamps: the error in the first process is at ≈01:11:58Z; the second process started at 01:13:50Z. The PermissionError therefore fired on the **first process's first Spark launch** (`TestCommittedSourceIngest.setUpClass`, alphabetical before `TestFixtureIngest`), about two minutes before the second suite existed. The same process then started Spark successfully ~3 seconds later (`TestFixtureIngest`) and ran Silver/Gold. Result: 170 tests, 1 error (the four committed-source ingest methods never ran). The overlapping second process completed 174/174 OK.

`'C1' is not recognized` is `spark-submit.cmd` splitting the repo path `DE C1 Project-...`. Non-fatal (same warning on the successful launch). A leftover `spark-submit` child was still running after the failed gateway.

Sequential tests already used unique warehouse dirs; they did not set `spark.local.dir` and did not delete warehouses on teardown. Concurrent suites remain unsupported (shared CWD Derby `metastore_db`, Windows TEMP). No lockfile/xdist guard was added.

### ACCEPTED

- Classification: Spark environment / Windows Py4J temp-file race; concurrency is an unsupported aggravating mode, not a Gold defect.
- Document sequential-only Spark unittest execution in README, debugging notes, workflow files.
- Small test-infrastructure helper: unique `spark.local.dir`, redirect Python `TEMP`/`TMP`/`TMPDIR` only during `getOrCreate`, retry **only** gateway `PermissionError`, `rmtree` warehouse on teardown.
- Leave Gold SQL / `create_gold_tables.py` unchanged.
- Do not start two destructive full suites to "re-break" the machine; diagnosis used the recorded traceback, PySpark source, and terminal timestamps.

### CHANGED

- Shared `start_local_test_spark` / `stop_local_test_spark` in `src/spark_local.py` instead of copying builder config in three test modules.
- Retry is limited to `PermissionError` on session start (the next class in the failed process already proved a later launch can succeed). Assertions are unchanged.

### REJECTED

- Changing Gold aggregations, eligibility, or DECIMAL handling — no evidence of a Gold defect.
- Weakening or skipping `TestCommittedSourceIngest`.
- pytest-xdist / file-lock concurrency guard — extra infrastructure; the operating rule is sequential.
- Installing winutils or renaming the repo to remove the space in `DE C1 Project`.
- Starting Dashboard.
- Intentionally launching two full Spark suites again.

### VALIDATION

Environment: Python 3.11.9 `.venv`, Temurin JDK 17.0.20.1, PySpark 3.5.6. One process at a time. **Not** Databricks.

Stage 2 CSV SHA-256 still matches `DATA_GENERATION_NOTES.md` (customers / orders / products).

Spark-free contracts (including the new helper test): generator + Bronze/Silver/Gold contract → **83 OK** in 52.144s.

Sequential command 1:
`python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality -v`
→ **Ran 148 tests in 385.673s OK** (0 failed, 0 skipped).

Sequential command 2:
`python -m unittest tests.test_gold_contract tests.test_gold_aggregations -v`
→ **Ran 27 tests in 122.986s OK** (0 failed, 0 skipped).

Combined relevant count this cycle: **175** (prior 174 plus one Spark-free contract test for the isolation helper). No test was skipped or weakened. Gold SQL files were not modified.

Did not launch a second concurrent full suite.

### FINAL DECISION

Treat the incident as local Spark-on-Windows gateway/temp isolation, not Gold. Run Spark tests sequentially. Isolation helper is test/runtime infrastructure only. Databricks not claimed. Dashboard not started.
