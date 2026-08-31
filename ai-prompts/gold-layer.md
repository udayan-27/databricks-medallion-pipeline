# Gold-layer prompts

## Prompt 1 — Stage 5 Gold analytical aggregations

### PROMPT SENT

Start Stage 5 — Gold Layer. Silver is complete and locally validated (commit `99a5ad7`). Do not start Dashboard. Do not redesign Bronze or Silver unless a concrete Gold dependency defect is discovered. Do not regenerate Stage 2 CSVs. Do not reinitialize Git.

First read the frozen requirements/design/DQ/workflow files and inspect the complete Silver implementation and tests.

**Part 1 — Gold contract.** Before writing Gold SQL, state the established analytical contract. Current design: Gold order facts are eligible only when `order_status = 'Completed'` and `quality_check_result = 'PASS'`. Do not silently change this policy. Document why failed-quality rows are excluded from analytical facts, why Bronze/Silver still retain them, how this affects order counts and revenue, and how reconciliation proves correctness. Do not confuse source `lifetime_value` with `lifetime_value_actual`. `lifetime_value_actual` must be actual aggregated order revenue. Do not modify source `lifetime_value` semantics.

**Part 2 — Sales by product.** Implement `src/gold/01_sales_by_product.sql` with product_id, product_name, category, total_orders, total_revenue, avg_order_value. Eligible completed Silver orders only. Avoid double counting from duplicate source orders. Preserve DECIMAL monetary precision. Define `total_orders` explicitly (a count of physical source rows is not always equivalent to valid completed orders). `avg_order_value` = total_revenue / total_orders. Handle division-by-zero safely.

**Part 3 — Revenue by customer.** Implement `src/gold/02_revenue_by_customer.sql`. Eligible completed orders only. No duplicate amplification. `lifetime_value_actual` from aggregated order revenue, not source `customers.lifetime_value`. Define zero-eligible-order customer behavior. Be explicit whether output includes all customers or only those with eligible completed orders. Use the documented design.

**Part 4 — Daily/weekly trends.** Implement `src/gold/03_daily_weekly_trends.sql`. Daily and weekly revenue/order trends from eligible completed orders. Follow the documented week-start convention. Explicit date truncation. DECIMAL for revenue.

**Part 5 — Customer segmentation.** Implement `src/gold/04_customer_segmentation.sql`. Segment types High-Value / Repeat / One-Time / Inactive. Exclusive priority: Inactive, High-Value, Repeat, One-Time. High-Value threshold revenue >= 1000.00. Mutually exclusive. Test boundaries including exactly 1000.00, 999.99, multiple/one/zero eligible orders, only invalid/failed orders.

**Part 6 — Orchestration.** Implement `src/gold/create_gold_tables.py` for sales_by_product, revenue_by_customer, daily_weekly_trends, customer_segmentation. Use existing configuration. Do not hard-code personal paths, usernames, secrets, or production catalog names. Keep local parquet behavior. Do not make Gold depend on Windows-specific hacks.

**Part 7–8 — Reconciliation tests.** Prove Gold vs eligible Silver revenue, customer revenue, order counts, product/customer consistency, duplicate protection, failed-row exclusion, zero-order customers, segmentation population and empty intersections, threshold boundary.

**Part 9–11 — DECIMAL correctness, quality-policy interaction, SQL performance review.** Do not convert DECIMAL to DOUBLE/FLOAT. Do not clean Bronze, change Silver results, or count invalid rows as facts. No dbt/streaming/SCD2.

**Part 12 — Testing.** Run existing suite first (Silver remains green). Then Gold tests. Then the complete relevant suite. No failures, no unexpected skips. Do not weaken a test to make it pass.

**Part 13 — Adversarial Gold review.** Duplicate IDs, NULL FKs, orphans, FAIL rows, multiple failures, zero/one/many eligible orders, 1000/999.99 boundaries, decimal rounding, weekly boundaries, empty product groups, repeated Gold execution.

**Part 14–16.** Update `ai-prompts/gold-layer.md` with this actual interaction. Update docs where justified. Git: status, diff --stat, diff --check. One commit: `feat: add Gold analytical aggregations`. Stop. Do not start Dashboard.

### AI RESPONSE SUMMARY

Stated the frozen Gold contract (Completed + PASS; Bronze/Silver retain FAIL rows; `lifetime_value_actual` is order-sum, not source LTV; revenue_by_customer includes all canonical customers including zeros; segmentation exclusive priority with 1000.00 threshold; Monday weeks via Spark `date_trunc('WEEK')`).

Implemented four SQL files plus `create_gold_tables.py`. SQL uses `{silver_schema}` placeholders, aggregates qualifying orders before joining canonical dimensions (`ROW_NUMBER` on `_ingest_row_id`), `NULLIF` for averages, and `DECIMAL(18,2)` casts. Orchestrator executes those SELECTs via Spark SQL and overwrites Gold tables (parquet locally, delta on Databricks). Did not rewrite aggregations as PySpark `groupBy`.

Tests: `tests/test_gold_contract.py` (Spark-free) and `tests/test_gold_aggregations.py` (fixture Bronze→Silver→Gold, zero-eligible, seed-42 reconciliation).

### ACCEPTED

- Eligibility: `order_status = 'Completed' AND quality_check_result = 'PASS'`. FAIL / Pending / Cancelled / duplicate-key copies are excluded from facts.
- Bronze/Silver retain every physical row; Gold filters explicitly.
- `total_orders` = COUNT of qualifying orders, not `SUM(quantity)` and not physical source-row count.
- `avg_order_value` = `total_revenue / NULLIF(total_orders, 0)`; CAST to DECIMAL(18,2); NULL when orders = 0.
- `lifetime_value_actual` = that customer's qualifying `SUM(total_amount)`, never source `lifetime_value`.
- `revenue_by_customer` includes **all canonical customers** (one row per distinct non-null `customer_id`), including zero qualifying orders (`total_orders = 0`, `total_revenue = 0`, `avg_order_value` NULL, `lifetime_value_actual = 0`).
- Sales by product omits products with zero qualifying orders (documented `data-model.md` decision).
- Segmentation exclusive CASE: Inactive → High-Value (>= 1000.00) → Repeat (>= 2) → One-Time (1). Threshold unchanged.
- Canonical dimension joins (min `_ingest_row_id`) so duplicate parent keys cannot fan out.
- Aggregate facts first, then join attributes.
- Config/env for catalog/schema/format; no personal paths or secrets.
- Local parquet + Databricks delta; Gold does not import the Windows FileSystem adapter.

### CHANGED

- SQL files use `-- GOLD_OUTPUT: <table>` markers so `03_daily_weekly_trends.sql` can emit both `daily_trends` and `weekly_trends`.
- Schema names are `{silver_schema}` placeholders substituted from `PipelineConfig.qualified_schema` (supports optional catalog).
- Added `PipelineConfig.gold_table()`.
- High-Value threshold kept at 1000.00; fixture and seed-42 both have High-Value customers, so no documented revision.
- Tests re-read Gold tables after overwrite instead of holding `spark.table()` DataFrames across a second Gold write (stale parquet paths).

### REJECTED

- Using source `customers.lifetime_value` as `lifetime_value_actual` — would ignore order facts and the quality contract.
- Counting all physical order rows, or treating uniqueness-FAIL duplicate copies as facts — would double-count injected duplicates.
- Including Pending/Cancelled or `quality_check_result = FAIL` in revenue — would inflate Gold vs eligible Silver.
- Overlapping segmentation labels (High-Value also Repeat) — pie chart requires one label; frozen priority list.
- Changing the 1000.00 threshold after seeing seed-42 distributions — documentation allows a change only if the bucket is empty or nearly universal; it is neither; frozen default kept.
- Replacing Gold SQL with PySpark `groupBy`/`F.sum` in the orchestrator — conflicts with “logic stays in `.sql` files”.
- Blind Silver/Bronze dedupe or deleting FAIL rows so Gold is “cleaner” — conflicts with flag-not-delete.
- Regenerating Stage 2 CSVs, starting Dashboard, dbt/streaming/SCD2, Windows-specific Gold hacks, hard-coded workspace paths.
- Weakening tests that failed because comments contained `SUM(quantity)` / `F.sum` — reworded comments instead; executable SQL still forbids summing quantity.

### VALIDATION

Environment: Python 3.11.9 `.venv`, Temurin JDK 17.0.20.1, PySpark 3.5.6. Local parquet warehouse. **Not** Databricks / Delta / Unity Catalog.

Commands actually run:

1. Existing suite (Silver remains green):
   `python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality -v`
   → **Ran 147 tests in 417.044s OK** (0 failed, 0 skipped)

2. Gold only (after fixing test-harness overwrite issues):
   `python -m unittest tests.test_gold_contract tests.test_gold_aggregations -v`
   → **Ran 27 tests in 136.510s OK**

3. Complete relevant suite:
   `python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality tests.test_gold_contract tests.test_gold_aggregations -v`
   → **Ran 174 tests in 628.243s OK** (0 failed, 0 skipped)

Observed on the adversarial fixture (Bronze→Silver→Gold): 17 qualifying orders, revenue 3330.00; duplicate `order_id` 99 (5000×2) excluded; NULL/orphan FKs excluded; Eve (only pending/cancelled/FAIL) Inactive with null AOV; Iris `lifetime_value_actual` 30.00 not source 99999.00; unused product 102 omitted; 1000.00 High-Value and 999.99 One-Time; Sunday 2026-08-30 week-start Monday 2026-08-24.

Observed on seed-42: Gold product/customer/daily/weekly/segment revenue and order counts match eligible Silver; `revenue_by_customer` has 10,000 canonical customers; `lifetime_value_actual` differs from source LTV for some customers; uniqueness-FAIL order copies (40) excluded.

First Gold Spark run had 6 errors (stale parquet readers after overwrite; overwrite-while-reading empty orders). Those were test-harness defects, not Gold SQL double-counting. Fixed and re-run. See `debugging-notes.md`.

### FINAL DECISION

Gold SQL aggregations and `create_gold_tables.py` are implemented against the frozen Completed+PASS contract. Local Spark/parquet tests pass. Databricks/Delta/UC Gold tables have **not** been written. Dashboard is not started.

## Prompt 2 — Gold QA: concurrent Spark suite vs sequential (not a Gold SQL change)

A later debugging cycle investigated a concurrent full-suite `PermissionError` during Bronze `setUpClass`. Gold production SQL was not modified. See `ai-prompts/debugging.md` Prompt 2 and `debugging-notes.md`. Operating rule: run Spark unittest suites sequentially.

---

## Prompt 3 — 2026-08-31 — Databricks workflow reuses existing Gold SQL

### PROMPT SENT

Same interaction as `ai-prompts/documentation.md` Prompt 7 / P016. Gold must run via `src/gold/create_gold_tables.py`. Reconcile eligible Silver revenue with Gold product/customer revenue and order counts. Eligibility remains Completed AND `quality_check_result = PASS`.

### AI RESPONSE SUMMARY

The Databricks orchestrator calls `create_gold_tables.create_gold_tables`, which still executes `src/gold/*.sql`. Validation queries Gold tables and compares `SUM(total_amount)` / `COUNT(*)` of eligible Silver orders to Gold product, customer, daily, and weekly totals, and checks segmentation coverage vs `revenue_by_customer`.

### ACCEPTED

Keep Gold logic in SQL files. Keep `lifetime_value_actual` from orders.

### CHANGED

None of the Gold SQL files or `create_gold_tables.py` aggregation path.

### REJECTED

Reimplementing Gold as PySpark `groupBy` in the Databricks wrapper; claiming Databricks Gold was executed.

### VALIDATION

P016 sequential suite **223/223 OK**. Gold Spark tests unchanged.

### FINAL DECISION

Gold application SQL stays the source of truth.

---

## Prompt 4 — 2026-08-31 — Closeout: Databricks Gold PASS (no Gold SQL change)

### PROMPT SENT

Same interaction as `ai-prompts/documentation.md` Prompt 8 / P017. Record the actual workspace Gold PASS. Do not change Gold SQL semantics.

### AI RESPONSE SUMMARY

No Gold SQL or orchestrator change. Documented Databricks Gold: sales_by_product 500; revenue_by_customer 10,000; daily_trends 1,339; weekly_trends 193; customer_segmentation 4; eligible revenue 46,083,475.86; eligible order count 74,587; segmentation population 10,000.

### ACCEPTED

Keep Completed + PASS eligibility. Keep `lifetime_value_actual` from orders.

### CHANGED

Documentation/current-status only.

### REJECTED

Rewriting Gold aggregations; claiming Cursor computed Gold in this closeout chat.

### VALIDATION

Local closeout suite **223/223 OK**. Databricks Gold PASS is the candidate’s workspace result.

### FINAL DECISION

Gold application SQL stays the source of truth. Databricks Gold is complete.
