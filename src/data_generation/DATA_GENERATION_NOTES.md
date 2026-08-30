# Data generation notes

Status: **not generated**. `data/*.csv` files are header-only placeholders so the required paths exist.

Frozen contract from the requirements/design stage (do not change silently at Stage 2):

## Planned targets (future)

| File | Unique keys | File rows | Mandatory injected issues |
|---|---|---|---|
| customers.csv | 10,000 | 10,010 | 50 NULL emails; 10 extra duplicate `customer_id` rows |
| orders.csv | 100,000 | 100,020 | 100 NULL customer_id; 200 NULL product_id; 50 orphan customer_id; 30 orphan product_id; 20 extra duplicate order_id |
| products.csv | 500 | 500 | none listed as mandatory |

Uniqueness module flags **all copies**: expect 20 customer rows and 40 order rows with uniqueness FAIL, from 10 and 20 extra rows respectively.

Keep mandatory issue classes on disjoint rows so counts are independently verifiable.

Use a **frozen RNG seed**. Record the seed here when the generator is written.

Exact output line counts will be printed by the generator and copied here **after** it runs.

## Approximately 700 rows

The guide’s ~700 / 0.7% figure is **not** a generation target. Listed issue instances sum to 460. See `requirements-analysis.md`.

## Future signup dates

30 future signup dates appear only in an example prompt. Decision at generation time: inject or skip; document here. Until then, they are not in the source files. Silver still evaluates `signup_date` against a frozen as-of date (planned `2026-08-31`).

## Responsible AI

All names, emails, and other fields will be synthetic. No real customer PII.
