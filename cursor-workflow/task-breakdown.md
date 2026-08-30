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

1. Completeness — **done** (this increment). `01_quality_completeness.py` + `quality_common.py`. Local Spark: 50 NULL emails, 100 NULL order customer_ids, 200 NULL order product_ids; rows retained.
2. Uniqueness — **done** (this increment). `02_quality_uniqueness.py`. Local Spark: 20 customer uniqueness-fail rows, 40 order uniqueness-fail rows; all copies flagged; no survivor.
3. Type validation — **not started**
4. Referential integrity (NULL ≠ orphan) — **not started**
5. Business logic — **not started**
6. `create_silver_tables.py` combining flags on `_ingest_row_id` and metrics — **not started**

Assert Silver row counts equal Bronze (completeness/uniqueness transforms already do this in tests). Multi-failure rows keep multiple per-module codes. `ai-prompts/silver-layer.md` Prompt 1 records this increment.

Do not start Gold until remaining Silver modules and the combiner are requested.

## Stage 6 — Gold

- Implement the four SQL files with documented filters (Completed + PASS).
- Prevent join fan-out; NULLIF averages; segmentation priority list.
- Orchestrate with `create_gold_tables.py` without replacing SQL logic.
- Tests: columns present; queries execute; duplicate orders do not double revenue.
- Update `ai-prompts/gold-layer.md` and commit.

## Stage 7 — Dashboard

- Write `dashboard_queries.sql` for the three required tiles plus filters.
- Write `DASHBOARD_GUIDE.md` with actual workspace steps (no fake screenshots).
- Update `ai-prompts/dashboard.md` and commit.

## Stage 8 — Debugging and hardening

- Record **real** issues in `debugging-notes.md`.
- Fix against spec; re-test; record results.
- Update `ai-prompts/debugging.md`.

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

## Explicitly not started after completeness/uniqueness

Type validation, referential integrity, business logic, `create_silver_tables.py`, Gold, Dashboard. Generator tests, Bronze tests, and Silver completeness/uniqueness tests exist and passed locally. Databricks tables have not been created.
