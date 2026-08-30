# Project context (Cursor)

Use this file as the **first** persistent context in a new Cursor Agent chat. Do not wait for the old conversation. Then read the files in the reading order below.

## What this project is

DE C1 AI Capability Exercise: Databricks Medallion pipeline for **synthetic** e-commerce sales.

`CSV -> Bronze (PySpark, raw) -> Silver (PySpark, five quality modules, flag not delete) -> Gold (SQL) -> Databricks SQL dashboard`

Candidate: Udayan Mahajan. Project root is this folder (it fulfils the required `databricks-medallion-pipeline/` tree). Canonical requirements file: `DE_C1_REQUIREMENTS.md`.

## Reading order for a new chat

1. `DE_C1_REQUIREMENTS.md`
2. `requirements-analysis.md` (ambiguities, decisions, **traceability matrix**)
3. This file
4. `cursor-workflow/spec.md`
5. `cursor-workflow/cursor-rules-or-instructions.md`
6. `cursor-workflow/task-breakdown.md`
7. `design-notes.md`
8. `data-model.md`
9. `data-quality-strategy.md`
10. Matching `ai-prompts/<area>.md` before changing that area

## Current stage (as of Stage 4 Silver completeness/uniqueness)

| Stage | Status |
|---|---|
| 0 Requirements read | Done |
| 1 Repository structure + engineering spec | Done (`16ee902`) |
| 1.5 Requirements traceability, architecture, DQ strategy | Done (`3fa1c57`) |
| 2 Data generation | **Done** (seed 42; `data/*.csv` populated; tests run) |
| 3 Bronze ingest | **Code complete.** Local Python 3.11 `.venv` + JDK 17 + PySpark 3.5.6. Local parquet ingest tests **passed**. Databricks / Delta / UC **not** run. |
| 4 Silver completeness + uniqueness | **Code complete** for this increment. Local Spark tests **passed** (50/100/200 completeness; 20/40 uniqueness-fail rows). Type / RI / business logic / `create_silver_tables.py` **not** started. |
| 5–10 Remaining Silver modules through submission | **Not started** |

`data/*.csv` are generated synthetic files. Bronze modules under `src/bronze/` are implemented (PySpark). Silver completeness and uniqueness are implemented as transforms; they do not write Silver tables. Type / RI / business logic / Gold / Dashboard remain stubs.

**Next requested stage should be the next Silver module (type validation) only when the user asks.** Do not skip ahead. Do not regenerate sample data unless asked. Do not claim Databricks Bronze/Silver tables exist until that runtime is executed.

## Frozen decisions (do not reopen unless the user or official spec contradicts them)

1. **~700 vs 460:** Generate the listed issue counts (sum **460** instances). Do not pad to 700. Document the gap.
2. **Four vs five Silver modules:** Implement **all five** files.
3. **30 future signup dates:** Optional business-logic defect. **Stage 2 decision: inject 30** (`2026-09-01`..`2026-09-30`), disjoint from mandatory customer issues, excluded from the order pool. Not part of the 460. Silver still has the `signup_not_future` rule.
4. **Duplicate extra rows:** 10,000 unique customers + 10 extra rows = **10,010** customer lines. 100,000 unique orders + 20 extra = **100,020** order lines. Products **500**.
5. **NULL FK ≠ orphan:** Completeness owns NULLs. RI owns non-null missing parents (50 / 30).
6. **Uniqueness:** Flag **all** copies of a duplicated key. Combiner joins on `_ingest_row_id`, never on duplicated PKs alone.
7. **Gold facts:** `order_status = 'Completed' AND quality_check_result = 'PASS'`. `lifetime_value_actual` is sum of qualifying order amounts, not source `lifetime_value`.
8. **Segmentation:** Inactive → High-Value (>= 1000.00) → Repeat (>= 2 orders) → One-Time. Mutually exclusive.
9. **Bronze:** Explicit schema, `PERMISSIVE`, no `DROPMALFORMED`, no source-value repair. `_ingest_row_id` is allowed lineage.
10. **Money:** `DECIMAL(18,2)`.
11. **Scope:** Batch full refresh, ~20–25 hour core. No streaming, dbt, GE, SCD2.
12. **PySpark** for Bronze/Silver; **SQL files** for Gold and dashboard.

## Mandatory injected issues (do not pad to 700)

- 50 NULL emails
- 10 duplicate customer_id rows (extra rows; uniqueness FAIL rows will be 20)
- 100 NULL order customer_id
- 200 NULL order product_id
- 50 orphan customer_id
- 30 orphan product_id
- 20 duplicate order_id rows (uniqueness FAIL rows will be 40)

Keep mandatory classes on **disjoint rows** so counts are independently testable.

## Silver modules (implement all five)

1. completeness
2. uniqueness
3. type validation
4. referential integrity
5. business logic

Rule details: `data-quality-strategy.md`. Multiple failures concatenate into `failed_checks`. Never delete bad rows.

## Non-negotiable rules

- Do not implement work ahead of the current requested stage unless asked.
- Inspect relevant files; compare to spec; explain approach; list assumptions and edge cases.
- Implement; run tests; record **actual** results; update docs; update the matching `ai-prompts/*.md`; commit when the user asks (or when the user already requested a commit for that stage).
- Never claim a test passed unless executed.
- Never fabricate prompt history or validation results.
- Never silently resolve contradictory requirements.
- Never delete bad source records merely to make quality checks pass.
- Bronze remains raw (source columns).
- No real PII, credentials, secrets, passwords, tokens, or private production connection details in the repo or prompts.
- No hard-coded personal workspace/DBFS/S3 paths.

## Prompt log format (every meaningful interaction)

In the matching `ai-prompts/*.md` file:

### PROMPT SENT
### AI RESPONSE SUMMARY
### ACCEPTED
### CHANGED
### REJECTED
### VALIDATION
### FINAL DECISION

## Config (Bronze)

`src/config.py`: `MEDALLION_DATA_PATH`, `MEDALLION_CATALOG`, `MEDALLION_BRONZE_SCHEMA`, `MEDALLION_TABLE_FORMAT`. Default data path = repo `data/`. Catalog default is unset (not `main`).

## Tests

Required by the assignment but **missing from the official file tree**. `tests/` holds Stage 2 generator tests, Stage 3 Bronze tests, and Stage 4 Silver completeness/uniqueness tests:

- `tests/test_bronze_contract.py` — always runnable (schema, CSV options, no-drop AST, config, fixtures, path helper)
- `tests/test_bronze_ingest.py` — PySpark runtime; skipped when Spark is unavailable
- `tests/test_silver_contract.py` — always runnable (code format, field lists, no-drop AST, physical-row metrics)
- `tests/test_silver_quality.py` — PySpark runtime; skipped when Spark is unavailable
