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
