# Data model

Status: **planned contracts**. Tables have not been created. CSV files currently contain headers only.

## Source files (Bronze grain)

### customers

| Column | Type | Notes |
|---|---|---|
| customer_id | INT | Primary key; duplicates injected later |
| customer_name | STRING | Synthetic |
| email | STRING | Nullable; 50 NULLs planned |
| country | STRING | |
| signup_date | DATE | Optional future-date business-logic check |
| customer_segment | STRING | Premium / Standard / Basic |
| lifetime_value | DECIMAL | Source field; Gold also computes actuals |

### orders

| Column | Type | Notes |
|---|---|---|
| order_id | INT | Primary key; duplicates injected later |
| customer_id | INT | FK to customers; NULLs and orphans planned |
| order_date | DATE | |
| product_id | INT | FK to products; NULLs and orphans planned |
| quantity | INT | |
| unit_price | DECIMAL | |
| total_amount | DECIMAL | Should equal quantity * unit_price when valid |
| order_status | STRING | Pending / Completed / Cancelled |
| payment_date | DATE | Nullable |

### products

| Column | Type | Notes |
|---|---|---|
| product_id | INT | Primary key |
| product_name | STRING | |
| category | STRING | |
| price | DECIMAL | |
| cost | DECIMAL | |
| stock_quantity | INT | |
| reorder_level | INT | |

## Planned Bronze tables

- `bronze.customers`
- `bronze.orders`
- `bronze.products`
- `bronze.ingest_metadata` (source_file, row_count, ingested_at, ingest_id)

Bronze columns match the source files. No quality flag columns on Bronze.

## Planned Silver tables

Silver copies Bronze columns and adds quality attributes:

| Column | Type | Purpose |
|---|---|---|
| quality_check_result | STRING | `PASS` or `FAIL` |
| failed_checks | ARRAY&lt;STRING&gt; | Names of failed modules/rules |
| completeness_pass | BOOLEAN | Module 1 |
| uniqueness_pass | BOOLEAN | Module 2 |
| type_validation_pass | BOOLEAN | Module 3 |
| referential_integrity_pass | BOOLEAN | Module 4 |
| business_logic_pass | BOOLEAN | Module 5 |

Planned tables:

- `silver.customers`
- `silver.orders`
- `silver.products`
- `silver.quality_metrics` (table_name, check_name, pass_count, fail_count, pass_pct, fail_pct, computed_at)

Equivalent column names are allowed only if documented here before implementation.

## Planned Gold tables / views

### gold.sales_by_product

product_id, product_name, category, total_orders, total_revenue, avg_order_value

### gold.revenue_by_customer

customer_id, customer_name, customer_segment, total_orders, total_revenue, avg_order_value, lifetime_value_actual

### gold.daily_weekly_trends

Grain and measures to be finalized in Gold SQL comments (daily and weekly revenue/order counts at minimum).

### gold.customer_segmentation

segment_type, customer_count, avg_revenue, total_revenue

Segment types: High-Value, Repeat, One-Time, Inactive. Mutually exclusive definitions will be written before SQL is implemented.

## Relationships

```
products (1) ----< orders >---- (1) customers
```

Referential integrity is enforced as a Silver *check*, not as a database constraint that would reject Bronze loads.

## Current files on disk

`data/*.csv` exist as header-only placeholders. They are not the 10,000 / 100,000 / 500 row datasets.
