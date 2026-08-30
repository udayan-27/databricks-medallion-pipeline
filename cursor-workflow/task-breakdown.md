# Task breakdown

Staged plan. Later stages must not be marked complete until that work is actually done.

## Stage 0 — Understand requirements (done as analysis)

- Read `DE_C1_REQUIREMENTS.md` fully.
- Identify ambiguities (700-row figure, four-vs-five checks, future signup dates).
- Confirm AI workflow evidence is part of the submission.

## Stage 1 — Initialize structure (this stage)

- Create required directories and files.
- Write requirements analysis, design, data model, quality strategy, Cursor context.
- Stub source modules; header-only CSVs.
- `.gitignore`
- Initial Git commit.
- **Do not generate data. Do not implement the pipeline.**

## Stage 2 — Data generation

- Document generation rules and defect injection in `DATA_GENERATION_NOTES.md`.
- Decide whether to inject 30 future signup dates; record the decision.
- Implement `generate_sample_data.py`.
- Produce `data/customers.csv`, `data/orders.csv`, `data/products.csv`.
- Validate counts and injected issues with actual script output.
- Update `ai-prompts/data-generation.md` and commit.

## Stage 3 — Database contracts

- Finalize `database/schema.sql` to match the implemented tables.
- Complete seed and setup notes for Databricks.
- Update `ai-prompts/documentation.md` if this is a distinct docs pass.

## Stage 4 — Bronze

- Implement customer, order, product ingest and `ingest_all.py`.
- Explicit schema, raw write, ingestion metadata.
- Tests: row counts vs files; no value changes.
- Update `ai-prompts/bronze-layer.md` and commit.

## Stage 5 — Silver

Implement and test in order:

1. Completeness
2. Uniqueness
3. Type validation
4. Referential integrity
5. Business logic (rules frozen in `data-quality-strategy.md` first)
6. `create_silver_tables.py` combining flags and metrics

Assert Silver row counts equal Bronze. Update `ai-prompts/silver-layer.md` and commit per meaningful slice if needed.

## Stage 6 — Gold

- Implement the four SQL files.
- Orchestrate with `create_gold_tables.py` without replacing SQL logic.
- Tests: columns present; queries execute; documented filters applied.
- Update `ai-prompts/gold-layer.md` and commit.

## Stage 7 — Dashboard

- Write `dashboard_queries.sql` for the three required tiles plus filters.
- Write `DASHBOARD_GUIDE.md` with actual workspace steps (no fake screenshots).
- Update `ai-prompts/dashboard.md` and commit.

## Stage 8 — Debugging and hardening

- Record real issues in `debugging-notes.md`.
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

## Explicitly not started after Stage 1

Data generation, Bronze/Silver/Gold/Dashboard implementation, tests, and validation evidence.
