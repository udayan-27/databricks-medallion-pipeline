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

### 3. Local Spark / Databricks execution BLOCKED

- **Symptom:** `import pyspark` fails; `java` is not on PATH.
- **Expected vs actual:** Bronze code is present; Bronze tables were not created in this environment.
- **Root cause:** environment, not ingest logic.
- **Action:** Spark integration tests skip with an explicit BLOCKED message. Do not treat skips as PASS.

Use this file during later stages to capture:

- Symptom
- Expected vs actual
- Root cause
- Files changed
- Tests re-run and **actual** results
- Whether the AI suggestion was accepted, changed, or rejected

Do not invent debugging sessions.
