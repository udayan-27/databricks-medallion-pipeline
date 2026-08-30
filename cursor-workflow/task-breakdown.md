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

Do not start Bronze until that stage is requested. Do not regenerate CSVs unless the seed or contract changes.

## Stage 3 — Database contracts

- Align `database/schema.sql` with implemented tables (`_ingest_row_id`, quality columns, Gold tables).
- Complete seed and setup notes for Databricks (no secrets).
- Update `ai-prompts/documentation.md` if this is a distinct docs pass.

## Stage 4 — Bronze

- Implement customer, order, product ingest and `ingest_all.py`.
- Explicit schema, PERMISSIVE, raw write, `_ingest_row_id`, ingestion metadata, config/paths.
- Tests: row counts vs files; source columns unchanged; missing file fails.
- Update `ai-prompts/bronze-layer.md` and commit.

## Stage 5 — Silver

Implement and test in order, using frozen `data-quality-strategy.md`:

1. Completeness
2. Uniqueness
3. Type validation
4. Referential integrity (NULL ≠ orphan)
5. Business logic
6. `create_silver_tables.py` combining flags on `_ingest_row_id` and metrics

Assert Silver row counts equal Bronze. Multi-failure rows keep multiple codes. Update `ai-prompts/silver-layer.md` and commit per meaningful slice if needed.

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

## Explicitly not started after Stage 2

Bronze/Silver/Gold/Dashboard implementation. Generator tests exist; pipeline tests do not.
