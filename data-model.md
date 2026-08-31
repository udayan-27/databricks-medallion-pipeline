# Data model

Status: **Bronze contracts implemented in code** (`src/bronze/`, `src/config.py`). All five Silver quality modules and combined Silver tables are produced locally (`src/silver/`, including `create_silver_tables.py`). Gold SQL aggregations and `create_gold_tables.py` produce Gold tables locally. Dashboard queries read those Gold tables (`src/dashboard/`). Tables have not been created in a Databricks workspace from this environment. CSV files are generated (Stage 2). No columns were added beyond source fields, required quality flags, ingest lineage, and Gold measures named by the assignment.

Logical layers:

- **Source CSV** — files on disk / S3 / DBFS
- **Bronze** — raw ingest, source columns unchanged, plus `_ingest_row_id`
- **Silver** — all Bronze rows plus quality attributes
- **Gold** — aggregations only

Bronze DDL must **not** enforce PK/FK/NOT NULL so required defects can land.

---

## Type conventions

| Kind | Spark / SQL type | Precision / scale | Date / numeric notes |
|---|---|---|---|
| Identifiers | `INT` | integer, expected positive when present | NULL allowed in Bronze for defective FKs |
| Counts / stock | `INT` | integer | Negative values are business-logic failures, not type failures if they parse as INT |
| Money | `DECIMAL(18,2)` | precision 18, scale 2 | Guide said only `DECIMAL` |
| Civil dates | `DATE` | — | CSV `yyyy-MM-dd`; no timezone |
| Ingest time | `TIMESTAMP` | — | Job clock |
| Names / email / country / category | `STRING` | — | UTF-8; email NULL is allowed |
| Enums | `STRING` | — | Closed sets below |
| Lineage id | `BIGINT` | — | Technical, not a business key |
| Arrays of check codes | `ARRAY<STRING>` | — | Silver only |

Allowed domains (named by the spec):

| Field | Allowed values |
|---|---|
| `customer_segment` | `Premium`, `Standard`, `Basic` |
| `order_status` | `Pending`, `Completed`, `Cancelled` |
| Gold `segment_type` | `High-Value`, `Repeat`, `One-Time`, `Inactive` |

Country and product category are free `STRING` domains at validation time (generation will use a small synthetic list; unknown values are not type failures).

---

## Source files (CSV grain = Bronze source columns)

### customers

| Column | Type | Nullable in Bronze | PK/FK | Business meaning | Allowed domain | Generation expectation |
|---|---|---|---|---|---|---|
| customer_id | INT | physically nullable (no DDL NOT NULL) | PK (logical; duplicates injected) | Customer identifier | Positive INT when present | Present on all generated rows; 10 IDs appear twice |
| customer_name | STRING | nullable at DDL; expected populated | | Synthetic display name | any string | NOT NULL in generator |
| email | STRING | **YES** | | Synthetic contact | any string or NULL | 50 NULLs; others populated |
| country | STRING | nullable at DDL; expected populated | | Synthetic country label | free string | NOT NULL in generator |
| signup_date | DATE | nullable at DDL; expected populated | | Account start date | valid date | NOT NULL; optional 30 future vs as-of date |
| customer_segment | STRING | nullable at DDL; expected populated | | Pricing/service tier | Premium / Standard / Basic | NOT NULL and in domain |
| lifetime_value | DECIMAL(18,2) | nullable at DDL; expected populated | | Source-provided LTV (not Gold actuals) | >= 0 intended | NOT NULL in generator |

Logical PK: `customer_id` **should** be unique and NOT NULL. Silver uniqueness/completeness flag violations. After generation: **10,000 unique ids**, **10,010 rows**.

### orders

| Column | Type | Nullable in Bronze | PK/FK | Business meaning | Allowed domain | Generation expectation |
|---|---|---|---|---|---|---|
| order_id | INT | physically nullable | PK (logical; duplicates injected) | Order identifier | Positive INT when present | Present; 20 IDs appear twice |
| customer_id | INT | **YES** | FK → customers.customer_id | Purchaser | Positive INT or NULL | 100 NULL; 50 non-null orphans; rest valid |
| order_date | DATE | expected populated | | Order civil date | valid date | NOT NULL in generator |
| product_id | INT | **YES** | FK → products.product_id | Item ordered | Positive INT or NULL | 200 NULL; 30 non-null orphans; rest valid |
| quantity | INT | expected populated | | Units ordered | INT | NOT NULL; BL requires > 0 |
| unit_price | DECIMAL(18,2) | expected populated | | Price per unit | decimal | NOT NULL; BL requires >= 0 |
| total_amount | DECIMAL(18,2) | expected populated | | Line total | decimal | NOT NULL; BL requires = quantity * unit_price ± 0.01 |
| order_status | STRING | expected populated | | Fulfilment state | Pending / Completed / Cancelled | NOT NULL and in domain |
| payment_date | DATE | **YES** | | Payment civil date | valid date or NULL | NULL allowed; BL vs status/order_date |

Logical PK: `order_id`. After generation: **100,000 unique ids**, **100,020 rows**.

FK rules (Silver, not DDL):

- NULL `customer_id` / `product_id`: completeness failure, **not** RI failure.
- Non-null value absent from parent: RI failure (orphan).
- Non-null value present on any parent row (even if that parent id is duplicated): RI pass.

### products

| Column | Type | Nullable in Bronze | PK/FK | Business meaning | Allowed domain | Generation expectation |
|---|---|---|---|---|---|---|
| product_id | INT | physically nullable | PK (logical) | Product identifier | Positive INT when present | Unique; 500 rows; no mandatory duplicates |
| product_name | STRING | expected populated | | Product label | any string | NOT NULL |
| category | STRING | expected populated | | Merchandise category | free string | NOT NULL |
| price | DECIMAL(18,2) | expected populated | | List price | decimal | NOT NULL; BL >= 0 |
| cost | DECIMAL(18,2) | expected populated | | Unit cost | decimal | NOT NULL; BL >= 0 |
| stock_quantity | INT | expected populated | | On-hand units | INT | NOT NULL; BL >= 0 |
| reorder_level | INT | expected populated | | Reorder threshold | INT | NOT NULL; BL >= 0 |

No foreign keys. No mandatory injected defects.

---

## Relationships

```
products (1) ----< orders >---- (1) customers
```

Referential integrity is a Silver **check**, not a database constraint that would reject Bronze loads.

Orphan namespaces (generation): unused negative or high INT ids that are not in the parent key set. Exact ranges go in `DATA_GENERATION_NOTES.md` at Stage 2.

---

## Bronze logical contract

Tables: `bronze.customers`, `bronze.orders`, `bronze.products`, `bronze.ingest_metadata`.

**Guarantees:**

1. One Bronze row per CSV data row (header excluded).
2. Source columns are the CSV fields with the declared types (unparsable → null).
3. No row dropped, filled, or deduplicated.
4. `_ingest_row_id BIGINT` is unique within a table write. It is ingest lineage, not a source field. It is **not** stable across clusters/reruns; tests must not assert its values.
5. No quality flag columns on Bronze.

### bronze.* entity extra column

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| _ingest_row_id | BIGINT | NO after successful ingest | Surrogate row identity for Silver joins |

### bronze.ingest_metadata

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| ingest_id | STRING | NO | Run identifier |
| source_file | STRING | NO | Path read |
| table_name | STRING | NO | customers / orders / products |
| row_count | BIGINT | NO | Rows written |
| ingested_at | TIMESTAMP | NO | Run time |
| status | STRING | NO | SUCCESS / FAILED |
| error_message | STRING | YES | Failure text; never secrets |

Grain: one row per source file per run. Append-only.

---

## Silver logical contract

Tables: `silver.customers`, `silver.orders`, `silver.products`, `silver.quality_metrics`.

**Guarantees:**

1. `COUNT(*)` equals the matching Bronze table.
2. Source columns plus `_ingest_row_id` match Bronze (except equality of ingest id values after copy).
3. Every row has the quality attributes below.
4. Bad rows remain.

| Column | Type | Nullable | Purpose |
|---|---|---|---|
| quality_check_result | STRING | NO | `PASS` or `FAIL` |
| failed_checks | ARRAY&lt;STRING&gt; | NO (empty array if none) | All failed rule codes; never overwritten |
| completeness_pass | BOOLEAN | NO | Module 1 |
| uniqueness_pass | BOOLEAN | NO | Module 2 |
| type_validation_pass | BOOLEAN | NO | Module 3 |
| referential_integrity_pass | BOOLEAN | NO | Module 4 (`true` for products) |
| business_logic_pass | BOOLEAN | NO | Module 5 |

`quality_check_result = 'FAIL'` iff `size(failed_checks) > 0`. Equivalent names are allowed only if this file is updated first.

### silver.quality_metrics

| Column | Type | Meaning |
|---|---|---|
| table_name | STRING | silver entity |
| check_name | STRING | module, rule code, distinct-key uniqueness, or `quality_check_result` |
| total_evaluated | BIGINT | denominator for this check’s population |
| pass_count | BIGINT | rows/keys that passed that check |
| fail_count | BIGINT | rows/keys that failed that check |
| pass_pct | DECIMAL(7,4) | pass_count / total_evaluated (0.0000 if empty) |
| fail_pct | DECIMAL(7,4) | fail_count / total_evaluated |
| expected_fail_count | BIGINT | Stage 2 contract when known; null otherwise |
| population_kind | STRING | `physical_row`, `distinct_key`, or `table_outcome` |
| computed_at | TIMESTAMP | metrics time |

Do not invent extra metric dimensions (workspace, cluster id). `distinct_key` uniqueness uses distinct non-null business keys as the denominator, not physical rows.

---

## Gold logical contract

Gold contains **no source-grain orders**. Only aggregations required by the assignment plus daily/weekly trends.

Default fact filter (also stated in each SQL file header):

`silver.orders.order_status = 'Completed' AND silver.orders.quality_check_result = 'PASS'`

Failed-quality rows stay in Bronze and Silver for audit. Gold excludes them so duplicate copies, NULL/orphan keys, and business-logic failures cannot inflate order counts or revenue. Reconciliation tests assert Gold `SUM(total_revenue)` and `SUM(total_orders)` equal the same predicates applied to Silver.

Rounding: `avg_order_value` / `avg_revenue` are `CAST(total / NULLIF(count, 0) AS DECIMAL(18, 2))`. Spark DECIMAL casts use half-up to scale 2. Zero-order customers have `avg_order_value` NULL, not zero.

### gold.sales_by_product

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| product_id | INT | NO | Product key |
| product_name | STRING | YES | From canonical product |
| category | STRING | YES | From canonical product |
| total_orders | BIGINT | NO | Count of qualifying orders |
| total_revenue | DECIMAL(18,2) | NO | Sum of qualifying `total_amount` |
| avg_order_value | DECIMAL(18,2) | YES | revenue / orders; null if 0 orders |

Grain: one row per `product_id` that appears in canonical products (products with zero qualifying orders may be omitted or included with zeros; **decision: include only products that have at least one qualifying order**, to keep the top-10 tile meaningful. Products with no sales are absent.)

### gold.revenue_by_customer

| Column | Type | Nullable | Meaning |
|---|---|---|---|
| customer_id | INT | NO | Customer key |
| customer_name | STRING | YES | Canonical customer |
| customer_segment | STRING | YES | Source segment Premium/Standard/Basic |
| total_orders | BIGINT | NO | Qualifying order count |
| total_revenue | DECIMAL(18,2) | NO | Qualifying revenue |
| avg_order_value | DECIMAL(18,2) | YES | revenue / orders; null if 0 |
| lifetime_value_actual | DECIMAL(18,2) | NO | Same as total_revenue; **not** source `lifetime_value` |

Grain: one row per canonical `customer_id`. Include customers with zero qualifying orders (`total_orders = 0`, `total_revenue = 0`, `avg_order_value` null, `lifetime_value_actual = 0`) so Inactive segmentation has a source.

### gold.daily_trends

| Column | Type | Meaning |
|---|---|---|
| trend_date | DATE | `order_date` of qualifying orders |
| total_orders | BIGINT | |
| total_revenue | DECIMAL(18,2) | |
| avg_order_value | DECIMAL(18,2) | null if 0 orders on that date (should not happen if grain is dates that exist) |

Grain: one row per distinct qualifying `order_date`. Dates with no qualifying orders are omitted.

### gold.weekly_trends

| Column | Type | Meaning |
|---|---|---|
| week_start_date | DATE | Monday week start of `order_date` |
| total_orders | BIGINT | |
| total_revenue | DECIMAL(18,2) | |
| avg_order_value | DECIMAL(18,2) | |

Grain: one row per week that has at least one qualifying order. Week start convention: Spark `date_trunc('WEEK', order_date)` as implemented on Databricks; the SQL header must state the observed first day of week when written (design intent: Monday).

### gold.customer_segmentation

| Column | Type | Meaning |
|---|---|---|
| segment_type | STRING | High-Value / Repeat / One-Time / Inactive |
| customer_count | BIGINT | Canonical customers in the bucket |
| avg_revenue | DECIMAL(18,2) | total_revenue / NULLIF(customer_count, 0) |
| total_revenue | DECIMAL(18,2) | Sum of those customers’ `lifetime_value_actual` |

Grain: four rows (one per type), including zeros if a bucket is empty.

Priority rules: see `design-notes.md` (Inactive → High-Value >= 1000.00 → Repeat → One-Time).

---

## Columns explicitly not added

Not in this model unless a later spec change says so:

- Email validity flags as extra columns (business_logic codes suffice)
- Quarantine tables
- SCD2 history
- Hashdiff / CDC columns
- Country or category foreign-key dimensions
- Dashboard-specific materialized histogram bins (unless Databricks cannot histogram)

---

## Current files on disk

`data/*.csv` contain the Stage 2 generated datasets (10,010 / 100,020 / 500 rows, seed 42). `database/schema.sql` is the intended DDL and has **not** been applied to a warehouse. Bronze, Silver, and Gold job code write the same names when Spark is available (local parquet validated; Databricks not run).
