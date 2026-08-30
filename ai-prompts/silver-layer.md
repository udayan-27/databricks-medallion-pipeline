# Silver-layer prompts

## Prompt 1 — Stage 4 Silver completeness and uniqueness (first increment)

### PROMPT SENT

Implement the first Stage 4 Silver increment only: `src/silver/01_quality_completeness.py` and `src/silver/02_quality_uniqueness.py`.

Do not start Gold or Dashboard. Do not modify Stage 2 CSVs. Do not redesign Bronze unless a concrete defect is found. Do not reinitialize Git.

Silver must preserve the full Bronze validation population. Bad records must not be deleted. Do not use `dropna`, `dropDuplicates`, destructive filters, or keep-one survivor logic for the core validation dataset. Quality validation means the source row remains and quality information is added.

Physical-row identity is Bronze `_ingest_row_id`. Do not use it as a substitute for `customer_id` / `order_id` / `product_id`.

Completeness: detect NULL on customers.email, orders.customer_id, orders.product_id. Preserve affected rows. Produce row-level failure information and pass/fail metrics. Do not silently classify non-null values as NULL. Do not classify NULL FKs as referential-integrity failures. Empty-string email only per the existing quality strategy (empty string is not NULL).

Uniqueness: customers.customer_id and orders.order_id. If a business key occurs more than once, every physical row in that duplicate group fails. 10 extra customer duplicate rows → 20 uniqueness-fail rows. 20 extra order duplicate rows → 40 uniqueness-fail rows. Do not deduplicate, choose a survivor, or alter business-key values.

Multiple checks must accumulate on the same physical row. Do not overwrite an earlier failure with a later one. Design a reusable helper if needed.

Metrics (physical population, not distinct keys): total evaluated, passed, failed, pass percentage, fail percentage.

Tests under `tests/` using the existing local Spark environment. Minimum: 50 NULL emails, 100 NULL order customer_ids, 200 NULL order product_ids, 20 customer duplicate-participating rows, 40 order duplicate-participating rows, rows remain, NULL FKs not classified as orphans, business keys unmodified, `_ingest_row_id` remains identity.

No unnecessary UDFs, `collect()`, or driver-side loops over full datasets.

Adversarial self-review before finalize. Update `ai-prompts/silver-layer.md` with this actual interaction. Run Bronze tests, then new Silver tests, then the relevant complete set. Commit only after implementation, tests, review, and docs: `feat: add Silver completeness and uniqueness validation`.

Do not implement Type Validation, Referential Integrity, Business Logic, Gold, or Dashboard.

### AI RESPONSE SUMMARY

Inspected the frozen spec (`DE_C1_REQUIREMENTS.md`, `data-quality-strategy.md`, design/data-model, Bronze ingest, Stage 2 notes, Silver stubs) and implemented completeness and uniqueness as PySpark transforms over Bronze DataFrames.

Added `src/silver/quality_common.py` so later modules can attach compatible per-module `*_failed_checks` arrays without last-writer overwrite. Completeness uses Spark `IS NULL` (empty string is not NULL; PK NULL checks from the frozen strategy are included; expected PK fail_count 0). Uniqueness uses a window count over the non-null business key and flags every participating physical row. No join-back on duplicated keys. Modules do not write `quality_check_result` and do not emit `ri:` codes.

Tests: `tests/test_silver_contract.py` (Spark-free) and `tests/test_silver_quality.py` (fixtures, synthetic edges, committed CSVs via Bronze ingest). Silver tables are not written.

### ACCEPTED

- Flag, do not delete. Full Bronze row counts remain after both checks.
- `_ingest_row_id` as physical-row identity; business keys remain the evaluated keys and are not rewritten.
- Completeness: `IS NULL` only. Empty string is not NULL (strategy, not a new rule).
- NULL FKs are completeness failures only. No `ri:` / orphan codes from these modules.
- Uniqueness: every participating physical row fails (20 customer rows, 40 order rows), not extra-row issue-instance counts (10 / 20) and not distinct-key metrics as the fail_count.
- Per-module arrays plus a reusable accumulator so completeness then uniqueness cannot overwrite each other.
- Metrics denominators are physical table row counts.
- PK completeness (`customers.customer_id`, `orders.order_id`, `products.product_id`) and products uniqueness from the frozen `data-quality-strategy.md` (expected 0 fails on generated data).

### CHANGED

- Added supporting utility `src/silver/quality_common.py` (required for compatible multi-check representation; documented). Logic for the two checks still lives in the numbered Silver modules.
- Completeness field list includes the documented PK extension, not only the three assignment-named critical fields. The increment’s injected-defect tests still assert email / order `customer_id` / order `product_id`.
- Modules return DataFrames with quality columns; they do not write `silver.*` tables or run `create_silver_tables.py`.
- CLI entry points read existing Bronze tables and log metrics; they do not orchestrate Silver writes.

### REJECTED

- Type validation, referential integrity, business logic, Gold, Dashboard, Silver table combiner — out of this increment (conflicts with the prompt scope).
- `dropna` / `dropDuplicates` / survivor selection / rewriting duplicate keys — conflicts with “flag all copies” and “never delete bad rows”.
- Treating `""` as NULL — conflicts with `data-quality-strategy.md`.
- Classifying NULL FKs as orphans — conflicts with NULL ≠ orphan (RI not implemented yet).
- Joining uniqueness results back on `customer_id` / `order_id` — would fan out duplicate keys.
- Writing last-writer `quality_check_result` from these modules — conflicts with accumulation.
- Padding defects to ~700 or modifying Stage 2 CSVs.
- Reinitializing Git.

### VALIDATION

Environment: Python 3.11.9 `.venv`, Temurin JDK 17.0.20.1, PySpark 3.5.6. Local parquet, not Databricks.

Commands actually run:

1. `python -m unittest tests.test_silver_contract -v` → **Ran 8 tests in 0.008s OK**
2. `python -m unittest tests.test_silver_quality -v` → **Ran 16 tests in 66.653s OK** (0 skipped)
3. `python -m unittest tests.test_bronze_contract tests.test_bronze_ingest -v` → **Ran 58 tests in 47.268s OK**
4. `python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality -v` → **Ran 96 tests in 153.298s OK** (0 failed, 0 skipped)

Observed physical-row failure counts on seed-42 committed data:

| Check | failed | total_evaluated | passed |
|---|---|---|---|
| `completeness:customers.email` | 50 | 10010 | 9960 |
| `completeness:orders.customer_id` | 100 | 100020 | 99920 |
| `completeness:orders.product_id` | 200 | 100020 | 99820 |
| orders completeness module rollup | 300 | 100020 | 99720 |
| uniqueness customers (all copies) | 20 | 10010 | 9990 |
| uniqueness orders (all copies) | 40 | 100020 | 99980 |
| products completeness / uniqueness | 0 | 500 | 500 |

Generator tests confirmed committed CSVs still match seed 42. Bronze Spark tests were not skipped.

No Silver runtime defect required a code fix after the first passing run. A pre-existing Windows `PYSPARK_PYTHON` warning (`'C1' is not recognized` because the repo path contains a space) appeared; tests still passed. Not treated as a Silver logic defect.

### FINAL DECISION

Accept this increment. Completeness and uniqueness are implemented and locally validated. Type / RI / business logic / Silver orchestration / Gold / Dashboard remain stubs. Commit: `feat: add Silver completeness and uniqueness validation`.
