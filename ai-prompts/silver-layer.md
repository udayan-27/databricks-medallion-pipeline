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

## Prompt 2 — Stage 4 Silver type validation and referential integrity

### PROMPT SENT

Continue Stage 4 Silver. Implement only type validation (`src/silver/03_quality_type_validation.py`) and referential integrity (`src/silver/04_quality_referential_integrity.py`).

Do not start business logic, Silver final orchestration, Gold, or Dashboard. Do not modify Stage 2 CSVs.

Type validation: use Bronze schema contracts. Cover declared INT/STRING/DATE/DECIMAL fields and closed domains (`customer_segment`, `order_status`). Do not silently coerce malformed values. Do not drop malformed rows. If PERMISSIVE ingest represents malformed CSV tokens as NULL, do not call a completeness-owned NULL a type failure. Distinguish null/missing, valid typed value, and invalid/malformed. Use the existing quality accumulator. Do not overwrite completeness or uniqueness. If a malformed-type fixture is needed, keep it small and separate from Stage 2 data.

Required type tests: valid rows pass; malformed integer; malformed date; malformed decimal; invalid domain; nullable `payment_date` NULL remains valid; type validation does not delete rows.

RI: `orders.customer_id` → `customers.customer_id` and `orders.product_id` → `products.product_id`. NULL FKs are not orphans (completeness). Non-null absences are RI failures. Do not alter FKs, delete orphans, or deduplicate parent tables as tables. Do not allow parent duplicates to multiply child rows.

Expected: 50 orphan customer IDs, 30 orphan product IDs. Completeness already established 100 NULL `customer_id` and 200 NULL `product_id` — those must not be classified as orphans.

Join safety: a naïve `orders JOIN customers ON customer_id` can fan out because duplicate customer IDs exist. Prefer a semi-join / existence check / distinct existence set so input physical row count equals output. Use `_ingest_row_id`. Explicit regression test for no RI join fan-out.

Semantic tests for both FKs, NULL-vs-orphan, valid FKs, row-count preservation, accumulation, unchanged business keys and `_ingest_row_id`.

Reuse the shared accumulator. Avoid collect/UDF/Python loops over Spark rows. Run Silver contract, Silver quality, Bronze, then the combined suite. Record this interaction in `ai-prompts/silver-layer.md`. Commit: `feat: add Silver type and referential integrity validation`.

### AI RESPONSE SUMMARY

Inspected frozen spec, Bronze contracts, existing completeness/uniqueness modules, and Silver tests. Implemented type validation as Spark predicates over typed Bronze columns: closed-domain `NOT IN` allowlists (case-sensitive, untrimmed) plus `IS NULL` on INT/DATE/DECIMAL fields this module owns. Completeness-critical NULLs and `payment_date` NULL are excluded. Malformed CSV tokens become NULL under PERMISSIVE ingest; a dedicated fixture (`tests/fixtures/silver/type_validation/`) covers `xyz` quantity, `13/01/2024` date, `12.34.56` decimal, `Shipped` / `premium` domains.

RI left-joins child FKs to a **broadcast distinct existence set** of non-null parent keys. That is not a naïve many-to-one join against the full parent table, so duplicate `customer_id` rows cannot multiply orders. `assert_no_row_loss` refuses a count increase. NULL FKs fail the orphan predicate (`fk IS NOT NULL AND parent_key IS NULL`). Shared `quality_common.py` gained `codes_for_domain_violation`, `codes_for_orphan_fk`, and `concat_code_arrays`; `attach_module_result` is unchanged.

### ACCEPTED

- Flag, do not delete. Physical counts remain 10,010 / 100,020 / 500 after type and RI.
- Completeness-owned NULLs are not type failures. `payment_date` NULL is type-valid.
- Domain checks: `Premium`/`Standard`/`Basic` and `Pending`/`Completed`/`Cancelled` only; country/category are free STRING.
- Type fail_count 0 on seed-42 committed data (no extra malformed injection into Stage 2).
- NULL FK ≠ orphan. Observed 50 / 30 orphans; 100 / 200 NULL FKs are RI pass.
- Distinct parent-key left join (broadcast) so duplicate parents cannot fan out child rows.
- Per-module arrays accumulate with completeness and uniqueness; no last-writer `quality_check_result`.
- Malformed-type fixture separate from Stage 2 CSVs.

### CHANGED

- Small helpers in `quality_common.py` for domain, orphan, and concatenating code arrays (compatible with the existing accumulator; not a second status system).
- Type-null ownership derived from Bronze contracts minus completeness fields minus `payment_date`; contract test asserts the lists stay aligned.
- Modules still do not write `silver.*` tables or run `create_silver_tables.py`.

### REJECTED

- Business logic, `create_silver_tables.py`, Gold, Dashboard — out of this increment.
- Naïve `orders JOIN customers ON customer_id` against the full parent table — would multiply rows where `customer_id` is duplicated.
- Dropping orphan or malformed rows / `dropna` / `dropDuplicates` on the validation dataset.
- Silently coercing or trimming malformed/domain values into valid values.
- Last-writer `quality_check_result` from these modules.
- Classifying NULL FKs as orphans (would make fail_count 150 / 230).
- Double-counting completeness-critical NULLs as type failures.
- Treating NULL `payment_date` as malformed.
- Inventing extra malformed tokens in Stage 2 CSVs merely to produce type failures.
- Collecting parent IDs to the driver / Python loops / UDFs for existence checks.
- Deduplicating parent tables as tables (existence uses a derived distinct key set only).

### VALIDATION

Environment: Python 3.11.9 `.venv`, Temurin JDK 17.0.20.1, PySpark 3.5.6. Local parquet, not Databricks.

Commands actually run:

1. `python -m unittest tests.test_silver_contract -v` → **Ran 13 tests in 0.015s OK**
2. `python -m unittest tests.test_silver_quality -v` → **Ran 35 tests in 149.265s OK** (0 skipped)
3. `python -m unittest tests.test_bronze_contract tests.test_bronze_ingest -v` → **Ran 58 tests in 46.922s OK**
4. `python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality -v` → **Ran 120 tests in 232.745s OK** (0 failed, 0 skipped)

Observed physical-row counts on seed-42 committed data (after completeness + uniqueness + type + RI):

| Check | failed | total_evaluated |
|---|---|---|
| type customers / orders / products | 0 | 10010 / 100020 / 500 |
| `ri:orders.customer_id_orphan` | 50 | 100020 |
| `ri:orders.product_id_orphan` | 30 | 100020 |
| orders RI module rollup | 80 | 100020 |
| NULL `customer_id` rows with customer-orphan code | 0 (of 100) | |
| NULL `product_id` rows with product-orphan code | 0 (of 200) | |
| physical rows after type+RI | 10010 / 100020 / 500 | unchanged |

Fan-out regression: parent `customer_id=1` duplicated three times; two matching child orders plus NULL and orphan rows → **exactly one output row per original physical order row** (4 in, 4 out).

Adversarial review (duplicate parents, NULL FKs, orphans, empty child dataset, all-valid committed data, all-invalid orphans, repeated RI execution, physical identity, row-count preservation, accumulation, malformed type fixture, domain violations): **no implementation defect required a code fix** after the first passing run. Pre-existing Windows `PYSPARK_PYTHON` warning (`'C1' is not recognized` because the repo path contains a space) still appears; tests still passed. Not treated as a Silver logic defect.

### FINAL DECISION

Accept this increment. Type validation and referential integrity are implemented and locally validated. Business logic / Silver orchestration / Gold / Dashboard remain stubs. Commit: `feat: add Silver type and referential integrity validation`.

## Prompt 3 — Stage 4 Silver business logic, orchestration, and metrics

### PROMPT SENT

Final Silver increment: freeze documented business rules, implement `05_quality_business_logic.py`, orchestrate all five modules in `create_silver_tables.py`, write quality metrics, reconcile Bronze=Silver physical rows, test against seed-42 and focused fixtures. Do not start Gold or Dashboard. Do not regenerate Stage 2 CSVs. Do not redesign Bronze. Do not reinitialize Git. Commit: `feat: complete Silver quality validation pipeline`.

### AI RESPONSE SUMMARY

Read frozen DQ strategy / design / requirements. Implemented only the 14 rules already listed in `data-quality-strategy.md`. Did not add product cost-vs-price. Future signup uses frozen as-of `2026-08-31`; Stage 2’s 30 future-signup customers remain excluded from orders so `order_not_before_signup` stays 0 and the 460 mandatory count is unchanged. Orchestrator applies completeness → uniqueness → type → RI → business logic, concatenates per-module arrays, writes `quality_check_result`, and overwrites `silver.quality_metrics` with `total_evaluated`, pass/fail counts and percentages, expected vs observed, and `population_kind` (physical_row / distinct_key / table_outcome).

### ACCEPTED

- Frozen business rules only (quantity > 0, non-negative money/stock, amount ±0.01 DECIMAL, Completed/Cancelled payment consistency, payment ≥ order_date, order ≥ signup via min `_ingest_row_id` parent, signup ≤ as-of, LTV ≥ 0).
- Flag, do not delete. Bronze physical rows = Silver physical rows (10010 / 100020 / 500).
- `_ingest_row_id` identity; business keys not rewritten.
- NULL fields skip BL (completeness/type own them). NULL/orphan FKs skip `order_not_before_signup`.
- Combined `failed_checks` + `quality_check_result`; modules never last-writer-overwrite.
- Metrics distinguish physical-row failures, distinct duplicate keys, and table-outcome FAIL rows.
- 30 future signups reported separately from the 460.

### CHANGED

- `quality_metrics` schema gained `total_evaluated`, `expected_fail_count`, `population_kind` so uniqueness keys are not mixed with participating-row counts.
- Empty-table percentages are 0.0000 / 0.0000 (documented) instead of raising.
- `PipelineConfig.silver_table()` added next to `bronze_table()`.
- Focused fixtures under `tests/fixtures/silver/business_logic/` (not Stage 2 CSVs).

### REJECTED

- Gold / Dashboard / regenerating Stage 2 CSVs / redesigning Bronze / Git reinit.
- Product cost vs list price — not in the frozen rule table.
- Extra pending-payment rule.
- Using Spark job-clock date for “future” signup.
- Padding defects to 700.
- Treating the sum of rule-level failures as distinct FAIL rows.
- Weakening tests to obtain a green result (the one Spark failure was a test assertion comparing three BL rules to all-module FAIL rows on a small fixture; the implementation was correct).

### VALIDATION

Environment: Python 3.11.9 `.venv`, Temurin JDK 17.0.20.1, PySpark 3.5.6. Local parquet, not Databricks. Session `JAVA_HOME` must be set.

Commands actually run:

1. `python -m unittest tests.test_silver_contract -v` → **Ran 20 tests in 0.034s OK**
2. `python -m unittest tests.test_silver_quality -v` → first Spark run **54/55** (1 test assertion); after test fix **Ran 55 tests in 305.812s OK** (0 skipped)
3. Combined: `python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality -v` → **Ran 147 tests in 393.422s OK** (0 failed, 0 skipped)

Observed seed-42 physical-row counts after all five modules + combine:

| Check | failed | total_evaluated |
|---|---|---|
| completeness email / order customer_id / order product_id | 50 / 100 / 200 | 10010 / 100020 / 100020 |
| uniqueness customer/order participating rows | 20 / 40 | 10010 / 100020 |
| uniqueness distinct duplicate keys | 10 / 20 | 10000 / 100000 |
| type | 0 | 10010 / 100020 / 500 |
| RI orphans | 50 / 30 | 100020 |
| `signup_not_future` | 30 | 10010 |
| other frozen BL including `order_not_before_signup` | 0 | |
| customers / orders table_outcome FAIL rows | 100 / 420 | 10010 / 100020 |

Physical rows unchanged: 10010 / 100020 / 500.

Debugging cycle (real): (1) canonical-parent window originally partitioned on pre-alias column names — fixed before Spark tests. (2) contract test matched `current_date()` in a docstring; docstring wording changed. (3) fixture orchestration test compared sum of three BL rules (7) to all-module FAIL rows (12); assertion fixed, implementation unchanged. (4) this shell needed `JAVA_HOME` set — environment, not Silver logic.

### FINAL DECISION

Accept this increment. Silver is complete locally. Gold / Dashboard remain stubs. Databricks / Delta / UC still not run. Commit: `feat: complete Silver quality validation pipeline`.

---

## Prompt 4 — 2026-08-31 — Databricks workflow reuses existing Silver modules

### PROMPT SENT

Same interaction as `ai-prompts/documentation.md` Prompt 7 / P016. Silver must run via `src/silver/create_silver_tables.py` (all five quality modules). Do not duplicate Silver logic.

### AI RESPONSE SUMMARY

The Databricks orchestrator calls `create_silver_tables.create_silver_tables`. Validation queries `silver.quality_metrics` and Silver tables for completeness, uniqueness (keys and participating rows), type (`type` module name), RI orphans, future-signup business logic, combined `quality_check_result`, and Bronze=Silver row counts. Eligibility for later Gold remains Completed + PASS.

### ACCEPTED

Reuse the combiner and frozen seed-42 expected fail counts. Flag, do not delete.

### CHANGED

None of the five Silver quality modules.

### REJECTED

Notebook-duplicated quality logic; padding to 700; treating NULL FKs as orphans; claiming Databricks Silver was executed.

### VALIDATION

P016 sequential suite **223/223 OK**. Silver Spark tests unchanged.

### FINAL DECISION

Silver application code stays the source of truth.

---

## Prompt 5 — 2026-08-31 — Closeout: Databricks Silver PASS (no Silver code change)

### PROMPT SENT

Same interaction as `ai-prompts/documentation.md` Prompt 8 / P017. Record the actual workspace Silver PASS. Do not change quality-module semantics.

### AI RESPONSE SUMMARY

No Silver source change. Documented Databricks Silver: physical rows 10,010 / 100,020 / 500; customers FAIL 100; orders FAIL 420; products FAIL 0; completeness 50 / 100 / 200; uniqueness participating rows 20 / 40; RI orphans 50 / 30; future signups 30; type failures 0.

### ACCEPTED

Flag, do not delete. Keep listed 460 issue instances and optional 30 future signups as separate classes.

### CHANGED

Documentation/current-status only.

### REJECTED

Changing Silver rules; padding to 700; treating NULL FKs as orphans.

### VALIDATION

Local closeout suite **223/223 OK**. Databricks Silver PASS is the candidate’s workspace result.

### FINAL DECISION

Silver application code stays the source of truth. Databricks Silver is complete.
