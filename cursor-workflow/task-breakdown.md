# Task breakdown

Staged plan. Later stages must not be marked complete until that work is actually done.

A new Cursor Agent should start at the first incomplete stage the **user** requested, not at Stage 0.

## Stage 0 — Understand requirements (done)

- Read `DE_C1_REQUIREMENTS.md` fully.
- Identify ambiguities (700-row figure, four-vs-five checks, future signup dates).
- Confirm AI workflow evidence is part of the submission.

## Stage 1 — Initialize structure (done)

Commit: `16ee902 chore: initialize project structure and engineering spec`

- Required directories and files created.
- Stubs and header-only CSVs.
- `.gitignore`
- **Did not** generate data. **Did not** implement the pipeline.

## Stage 1.5 — Requirements, architecture, data quality design (done in the design commit)

- Traceability matrix in `requirements-analysis.md`.
- Ambiguities challenged and frozen (including the original three).
- Architecture in `design-notes.md` plus senior failure-mode review.
- `data-model.md` and `database/schema.sql` aligned to three sources + Bronze/Silver/Gold contracts.
- `data-quality-strategy.md` frozen for the five modules.
- Cursor workflow files updated so a new chat can continue.

**Did not** generate data. **Did not** implement pipeline code.

## Stage 2 — Data generation (done)

Commit message (this stage): `feat: add deterministic e-commerce sample data generator`

- Generator: `src/data_generation/generate_sample_data.py` (seed **42**, as-of **2026-08-31**).
- CSVs: `data/customers.csv` 10,010 rows / 10,000 unique IDs; `data/orders.csv` 100,020 / 100,000; `data/products.csv` 500 / 500.
- Mandatory defects at listed counts only (460 instances). Optional 30 future signups documented in `DATA_GENERATION_NOTES.md`.
- Built-in validation passed on the generating run.
- Tests: `python -m unittest tests.test_generate_sample_data -v` → 14 tests OK.
- `tests/` added because the assignment requires meaningful tests but omitted the directory (`requirements-analysis.md` §6.15).

Do not regenerate CSVs unless the seed or contract changes. Bronze ingest code exists; do not regenerate data to "help" Bronze.

## Stage 3 — Database contracts (DDL already aligned; not applied)

- `database/schema.sql` already includes `_ingest_row_id` and `bronze.ingest_metadata` matching the implemented Bronze code.
- Setup/seed notes describe Databricks apply steps. Objects have **not** been created in a workspace.

## Stage 4 — Bronze (done as the user-requested "Stage 3 Bronze Layer")

Commit message (this stage): `feat: add Bronze ingestion pipeline`

- Shared PySpark ingest in `src/bronze/ingest_core.py`; CLIs in `01_ingest_customers.py`, `02_ingest_orders.py`, `03_ingest_products.py`, `ingest_all.py`.
- Config: `src/config.py` (env/CLI; no personal paths).
- Explicit schema, PERMISSIVE, raw write, `_ingest_row_id` (per-execution), append-only `bronze.ingest_metadata`.
- Tests: `tests/test_bronze_contract.py` (always run); `tests/test_bronze_ingest.py` (requires local PySpark+JDK).
- Local Spark environment (2026-08-31): Python 3.11.9 `.venv`, Temurin JDK 17.0.20.1, PySpark 3.5.6. In-memory smoke test passed. Spark ingest tests **passed** after a local-Windows path/Hadoop adapter (`spark_input_path` uses `as_posix()`; `NoWinutilsRawLocalFileSystem` for local sessions only). Contract tests 37/37 OK. Databricks / Delta / UC still **not** executed.
- `ai-prompts/bronze-layer.md` updated with the Bronze implementation interaction; environment setup is in `ai-prompts/documentation.md`.

Do not start Silver until that stage is requested.

## Stage 5 — Silver

Implement and test in order, using frozen `data-quality-strategy.md`:

1. Completeness — **done**. `01_quality_completeness.py` + `quality_common.py`. Local Spark: 50 NULL emails, 100 NULL order customer_ids, 200 NULL order product_ids; rows retained.
2. Uniqueness — **done**. `02_quality_uniqueness.py`. Local Spark: 20 customer uniqueness-fail rows, 40 order uniqueness-fail rows; all copies flagged; no survivor.
3. Type validation — **done** (this increment). `03_quality_type_validation.py`. Local Spark: 0 type failures on seed-42 committed data. Malformed INT/DATE/DECIMAL and domain fixtures fail without deleting rows. Completeness-owned NULLs and NULL `payment_date` are not type failures.
4. Referential integrity (NULL ≠ orphan) — **done** (this increment). `04_quality_referential_integrity.py`. Local Spark: 50 customer orphans, 30 product orphans; 100/200 NULL FKs not orphans; distinct parent-key left join (no fan-out).
5. Business logic — **done** (this increment). `05_quality_business_logic.py`. Frozen rules only. Local Spark: 30 `signup_not_future`; 0 other BL on seed-42; future-signup customers have no orders so `order_not_before_signup` stays 0. DECIMAL amount check uses 0.01 tolerance.
6. `create_silver_tables.py` combining flags on `_ingest_row_id` and metrics — **done** (this increment). Combined `quality_check_result` / `failed_checks`. Local parquet Silver writes in tests. Physical Bronze = Silver counts.

Assert Silver row counts equal Bronze. Multi-failure rows keep multiple per-module codes. `ai-prompts/silver-layer.md` Prompts 1–3 record these increments.

Do not start Gold until requested.

## Stage 6 — Gold (done as the user-requested "Stage 5 — Gold Layer")

Commit message (this stage): `feat: add Gold analytical aggregations`

- SQL: `01_sales_by_product.sql`, `02_revenue_by_customer.sql`, `03_daily_weekly_trends.sql` (daily + weekly), `04_customer_segmentation.sql`.
- Orchestrator: `create_gold_tables.py` executes those files; does not replace them with PySpark aggregations.
- Eligibility: Completed + PASS. `lifetime_value_actual` from orders. All canonical customers including zeros. Segmentation exclusive priority; threshold 1000.00 unchanged.
- Tests: `tests/test_gold_contract.py`, `tests/test_gold_aggregations.py`. Sequential Gold **27/27 OK**. Sequential generator/Bronze/Silver **148/148 OK** after Spark test-isolation hardening (combined relevant **175**, including one new Spark-free helper contract test). Databricks Gold **not** written.
- Gold QA checkpoint: a second concurrent full-suite process failed in Bronze `setUpClass` with a Windows Py4J temp-file `PermissionError`. That is Spark-on-Windows gateway startup, not Gold SQL. Production Gold was not changed. Local Spark tests must be run **sequentially** (one process). See `debugging-notes.md`.

Do not start Dashboard until that stage is requested.

## Stage 7 — Dashboard (done as the user-requested "Stage 6 — Dashboard")

Commit message (this stage): `feat: add Databricks SQL dashboard queries and guide`

- SQL: `src/dashboard/dashboard_queries.sql` — Top 10 products, customer revenue distribution (raw `lifetime_value_actual` for a Databricks histogram), customer segmentation (`segment_type` + `customer_count`), plus filter-value queries.
- Guide: `src/dashboard/DASHBOARD_GUIDE.md` — prerequisites, catalog/schema, Gold tables, tile mapping, viz/axis/histogram/filter configuration, QA checklist, local vs Databricks.
- Filters: `category` before LIMIT on Tile 1; `customer_segment` on Tile 2. Date range rejected for these tiles (no date grain on those Gold tables).
- Tests: `tests/test_dashboard_contract.py`, `tests/test_dashboard_queries.py`. Local Spark against Gold parquet. Databricks SQL Dashboard UI **not** rendered.
- Gold production SQL/orchestrator unchanged. Stage 2 CSVs unchanged.

Do not start final submission audit until requested.

## Stage 8 — Debugging and hardening

- Record **real** issues in `debugging-notes.md`.
- Fix against spec; re-test; record results.
- Update `ai-prompts/debugging.md`.

Gold QA (prior increment): concurrent/overlapping Spark suites vs sequential. Root cause is PySpark `launch_gateway` Windows `PermissionError` on a unique temp connection-info file — not Gold logic. Test helper isolates `spark.local.dir` / Py4J temp per class and retries that gateway error only.

Databricks compatibility audit (this increment): **code review done**. Local Windows Spark (winutils adapter, parquet, `.venv`, warehouse/`TEMP` isolation) is already gated off the Databricks path. Default table format is `delta`. Catalog/data-path are runtime parameters, not committed names. No `src/` change required. Databricks jobs have **not** been started.

## Stage 9 — Documentation closeout

- README setup that matches reality.
- `reflection.md` and `final-ai-usage-summary.md` from real experience.
- Update `ai-prompts/documentation.md`.

## Stage 10 — Submission review

- Verify required file tree.
- Verify prompt history is complete and non-fabricated.
- Verify Git history is meaningful.
- Confirm no secrets.
- Organizational Git account/email process (not stored as a secret in-repo).

## Explicitly not started after compatibility audit

Databricks cluster/SQL warehouse execution. Databricks SQL Dashboard UI rendering. Generator, Bronze, Silver, Gold, and Dashboard query tests exist and were locally validated. Databricks tables have not been created.
