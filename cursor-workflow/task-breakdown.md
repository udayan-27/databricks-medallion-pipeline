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

## Stage 2 — Data generation (next; not started)

Only when the user requests it:

- Document generation rules, seed, disjoint defect injection, exact line counts (10,010 / 100,020 / 500) in `DATA_GENERATION_NOTES.md`.
- Decide whether to inject 30 future signup dates; record the decision.
- Implement `generate_sample_data.py` with a frozen seed.
- Produce `data/customers.csv`, `data/orders.csv`, `data/products.csv`.
- Validate counts and injected issues with **actual** script output (unique keys vs file rows; uniqueness extras vs FAIL-row math).
- Update `ai-prompts/data-generation.md` and commit when asked.

Do not start Bronze until generation is validated.

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

## Explicitly not started after Stage 1.5

Data generation, Bronze/Silver/Gold/Dashboard implementation, tests, and runtime validation evidence.
