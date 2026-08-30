# Data generation notes

Status: **not generated**. `data/*.csv` files are header-only placeholders so the required paths exist.

## Planned targets (future)

| File | Base rows | Mandatory injected issues |
|---|---|---|
| customers.csv | 10,000 unique customers, plus 10 duplicate `customer_id` rows | 50 NULL emails; 10 duplicate customer_id rows |
| orders.csv | 100,000 unique orders, plus 20 duplicate `order_id` rows | 100 NULL customer_id; 200 NULL product_id; 50 orphan customer_id; 30 orphan product_id; 20 duplicate order_id |
| products.csv | 500 | none listed as mandatory |

Exact output line counts (base + duplicates) will be recorded here **after** the generator runs.

## Approximately 700 rows

The guide’s ~700 / 0.7% figure is **not** a generation target. Listed issue instances sum to 460. See `requirements-analysis.md`.

## Future signup dates

30 future signup dates appear only in an example prompt. Decision at generation time: inject or skip; document here. Until then, they are not in the source files.

## Responsible AI

All names, emails, and other fields will be synthetic. No real customer PII.
