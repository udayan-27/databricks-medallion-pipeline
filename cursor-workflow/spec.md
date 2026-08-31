# Technical specification

Canonical human-readable requirements: `DE_C1_REQUIREMENTS.md`.  
Ambiguity decisions: `requirements-analysis.md`.  
Architecture: `design-notes.md`.  
Column contracts: `data-model.md`.  
Silver rules: `data-quality-strategy.md`.  
This file is the canonical **technical** spec for implementation.

A new agent should treat frozen decisions here as binding for later stages.

## 1. Scope

Build, in later stages, a Databricks Medallion pipeline:

1. Generate synthetic CSVs with documented quality issues.
2. Ingest to Bronze without cleaning source values.
3. Build Silver tables with five quality modules; flag failures; emit metrics.
4. Build Gold aggregations in SQL.
5. Provide dashboard SQL and setup guide.
6. Document, test, and record AI usage.

Those stages are now implemented and locally tested. This spec remains the binding contract. Databricks runtime execution is still out of scope until requested.

Out of scope: streaming, Autoloader, SCD2, dbt, Great Expectations, quarantine tables, production deployment.

## 2. Runtime

| Layer | Technology |
|---|---|
| Data generation | Python (local is acceptable); **frozen RNG seed** |
| Bronze | PySpark |
| Silver | PySpark |
| Gold | Spark SQL / Databricks SQL files |
| Dashboard | Databricks SQL |
| Orchestration helpers | Python that runs SQL files is allowed; Gold logic must remain in `.sql` files |

Catalog, schema, and volume names must be configuration, not hardcoded personal secrets.

Spark CSV: **explicit schema**, mode **PERMISSIVE**. Never DROPMALFORMED.

## 3. Input contracts

See `data-model.md`.

After generation (frozen):

| File | Unique keys | File rows | Mandatory defects |
|---|---|---|---|
| customers.csv | 10,000 | 10,010 | 50 NULL email; 10 extra duplicate customer_id rows |
| orders.csv | 100,000 | 100,020 | 100 NULL customer_id; 200 NULL product_id; 50 orphan customer_id; 30 orphan product_id; 20 extra duplicate order_id rows |
| products.csv | 500 | 500 | none mandatory |

Do not pad issue instances to 700.

Optional: 30 future signup dates — **Stage 2 injected 30** (`2026-09-01`..`2026-09-30`); documented in `DATA_GENERATION_NOTES.md`; not part of the 460.

## 4. Bronze behavior

- Read `data/customers.csv`, `data/orders.csv`, `data/products.csv` (or configured DBFS/S3/volume equivalents).
- Apply explicit schema from `data-model.md`.
- Write raw Delta tables `bronze.customers|orders|products`.
- Add `_ingest_row_id BIGINT` (lineage). Do not change source field values.
- Append `bronze.ingest_metadata`: ingest_id, source_file, table_name, row_count, ingested_at, status, error_message.
- Full refresh overwrite of entity tables; append-only metadata.
- Forbidden: dropping rows, filling nulls, deduplicating, repairing FKs, UNIQUE/FK/NOT NULL constraints that reject defects.
- Missing file or missing header: fail the job.

## 5. Silver behavior

Process:

1. Load Bronze.
2. Run completeness, uniqueness, type validation, referential integrity, business logic **independently**.
3. Combine on `_ingest_row_id` in `create_silver_tables.py`.
4. Write Silver tables including every Bronze row.
5. Write `silver.quality_metrics`.

Flags:

- Per-module booleans
- `failed_checks ARRAY<STRING>` concatenated, never last-writer-wins
- `quality_check_result` = `FAIL` if any required check fails; otherwise `PASS`

NULL FK = completeness only. Orphan = RI only. Duplicate = uniqueness (all copies). Malformed/enum = type. Cross-field = business logic.

Business-logic rules are copied from `data-quality-strategy.md`; that file is frozen until a documented change. Product cost-vs-price is not a frozen rule and is not implemented. As-of date is `2026-08-31`.

Products still run all five modules; RI passes for all product rows.

## 6. Gold behavior

SQL files must produce:

- `01_sales_by_product.sql` — product_id, product_name, category, total_orders, total_revenue, avg_order_value
- `02_revenue_by_customer.sql` — customer_id, customer_name, customer_segment, total_orders, total_revenue, avg_order_value, lifetime_value_actual
- `03_daily_weekly_trends.sql` — populate `gold.daily_trends` and `gold.weekly_trends` (columns in `data-model.md`)
- `04_customer_segmentation.sql` — segment_type (High-Value / Repeat / One-Time / Inactive), customer_count, avg_revenue, total_revenue

Default qualifying order:

`order_status = 'Completed' AND quality_check_result = 'PASS'`

Formulas:

- total_orders = COUNT of qualifying orders
- total_revenue = SUM(total_amount)
- avg_order_value = total_revenue / NULLIF(total_orders, 0)
- lifetime_value_actual = customer total_revenue (not source lifetime_value)

Segmentation priority: Inactive (0 qualifying orders) → High-Value (revenue >= 1000.00) → Repeat (>= 2) → One-Time (1).

Dimension joins must not fan out on duplicate parent keys (`row_number` canonical row). Each query header must state filters and grain.

`create_gold_tables.py` executes SQL files; it must not replace them with undocumented PySpark aggregations.

**Implemented locally** (parquet). Databricks / Delta / UC Gold tables have not been written from this environment.

## 7. Dashboard

`dashboard_queries.sql` includes:

- Top 10 products by revenue (bar) — `gold.sales_by_product`, `ORDER BY total_revenue DESC, product_id ASC LIMIT 10`
- Customer revenue distribution (histogram) — one Gold customer row with `lifetime_value_actual`; Databricks viz bins
- Customer segmentation (pie) — `segment_type`, `customer_count` from `gold.customer_segmentation`

Filters: `category` on Tile 1 as a query parameter before LIMIT; `customer_segment` on Tile 2. Date range is not a filter on these tiles. Guide: `src/dashboard/DASHBOARD_GUIDE.md`. No fabricated screenshots. Databricks UI not rendered from this environment.

**Implemented locally** (Spark SQL against parquet Gold). Databricks SQL Dashboard product has not been used.

## 8. Testing (later)

Tests must be real executions. Add `tests/` at implementation (not in the official required tree, but required by the assignment). Minimum coverage:

- Generated row counts and injected issue counts (including uniqueness **row** vs **extra-row** distinction)
- Generator determinism (seed)
- Bronze row counts match source files
- Bronze source values unchanged vs CSV
- Bronze unchanged after Silver
- Each Silver module flags known injected defects
- NULL FK not classified as orphan
- Multi-failure `failed_checks` length > 1
- Silver row counts equal Bronze (no deletes)
- Gold queries run, expected columns, no duplicate amplification
- Segmentation exclusive and exhaustive
- No secrets in repo

Never claim PASS unless executed.

## 9. Data and security

Synthetic data only. No real customer PII. No credentials in code, prompts, or notes. Connection strings, if any, via environment variables that are gitignored.

## 10. Requirement conflict handling

| Conflict | Spec decision |
|---|---|
| ~700 rows vs listed 460 | Generate listed counts; document gap |
| Four checks vs five modules | Implement five |
| 30 future signup dates | Optional business-logic; document at generation |
| 10,000 rows vs extra duplicates | Unique targets + extra duplicate rows |
| Inference vs types | Explicit schema; inference diagnostic only |
| Raw Bronze vs lineage column | Source columns raw; `_ingest_row_id` allowed |

## 11. Definition of done for a change

A meaningful AI-assisted change is done only when: files inspected, spec compared, approach and edge cases stated, code written (if that stage), tests/validation run, actual results recorded, docs and `ai-prompts/` updated, Git commit created when requested.
