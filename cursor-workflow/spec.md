# Technical specification

Canonical human-readable requirements: `DE_C1_REQUIREMENTS.md`.  
Ambiguity decisions: `requirements-analysis.md`.  
This file is the canonical **technical** spec for implementation.

## 1. Scope

Build, in later stages, a Databricks Medallion pipeline:

1. Generate synthetic CSVs with documented quality issues.
2. Ingest to Bronze without cleaning.
3. Build Silver tables with five quality modules; flag failures; emit metrics.
4. Build Gold aggregations in SQL.
5. Provide dashboard SQL and setup guide.
6. Document, test, and record AI usage.

**This specification commit does not implement those stages.**

## 2. Runtime

| Layer | Technology |
|---|---|
| Data generation | Python (local is acceptable) |
| Bronze | PySpark |
| Silver | PySpark |
| Gold | Spark SQL / Databricks SQL files |
| Dashboard | Databricks SQL |
| Orchestration helpers | Python that runs SQL files is allowed; Gold logic must remain in `.sql` files |

Catalog, schema, and volume names must be configuration, not hardcoded personal secrets.

## 3. Input contracts

See `data-model.md`. Row targets after generation: 10,000 customers, 100,000 orders, 500 products, plus extra duplicate rows as specified (duplicate rows increase file length above the “base” unique-key counts; generation notes must state exact output row counts).

## 4. Bronze behavior

- Read `data/customers.csv`, `data/orders.csv`, `data/products.csv` (or DBFS/S3 equivalents).
- Apply explicit schema.
- Write raw Delta tables.
- Log metadata: row counts, timestamp, source path.
- Forbidden: dropping rows, filling nulls, deduplicating, repairing FKs.

## 5. Silver behavior

Process:

1. Load Bronze.
2. Run completeness, uniqueness, type validation, referential integrity, business logic.
3. Combine results onto each row.
4. Write Silver tables including every Bronze row.
5. Write quality metrics.

`quality_check_result` is `FAIL` if any required check fails; otherwise `PASS`.

Business-logic rules must be copied from `data-quality-strategy.md` at implementation time; if the strategy is still “intended candidates,” finish documenting rules first.

## 6. Gold behavior

SQL files must produce:

- `01_sales_by_product.sql` — product_id, product_name, category, total_orders, total_revenue, avg_order_value
- `02_revenue_by_customer.sql` — customer_id, customer_name, customer_segment, total_orders, total_revenue, avg_order_value, lifetime_value_actual
- `03_daily_weekly_trends.sql` — daily and weekly trend measures (exact columns documented in the SQL header when written)
- `04_customer_segmentation.sql` — segment_type (High-Value / Repeat / One-Time / Inactive), customer_count, avg_revenue, total_revenue

Each query header must state which Silver quality filters apply.

## 7. Dashboard

`dashboard_queries.sql` must include at least:

- Top 10 products by revenue (bar)
- Customer revenue distribution (histogram)
- Customer segmentation (pie)

Plus filter-supporting queries or WHERE placeholders. Guide describes how to attach them in Databricks SQL.

## 8. Testing (later)

Tests must be real executions. Minimum intended coverage (not yet present):

- Generated row counts and injected issue counts
- Bronze row counts match source files
- Bronze values unchanged vs CSV for a sample of rows
- Each Silver module flags known injected defects
- Silver row counts equal Bronze (no deletes)
- Gold queries run and return expected columns
- No secrets in repo

## 9. Data and security

Synthetic data only. No real customer PII. No credentials in code, prompts, or notes. Connection strings, if any, via environment variables that are gitignored.

## 10. Requirement conflict handling

| Conflict | Spec decision |
|---|---|
| ~700 rows vs listed 460 | Generate listed counts; document gap |
| Four checks vs five modules | Implement five |
| 30 future signup dates | Optional business-logic; document at generation |

## 11. Definition of done for a change

A meaningful AI-assisted change is done only when: files inspected, spec compared, approach and edge cases stated, code written, tests/validation run, actual results recorded, docs and `ai-prompts/` updated, Git commit created.
