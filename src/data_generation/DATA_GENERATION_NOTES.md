# Data generation notes

Status: **generated and validated** (Stage 2). Files in `data/` are the seed CSVs for Bronze.

Generator: `src/data_generation/generate_sample_data.py`
Standard library only (no Faker, no pandas).
CLI: `python src/data_generation/generate_sample_data.py --output-dir data --seed 42`

## Frozen contract

| File | Unique business keys | Physical rows | Mandatory injected issues |
|---|---|---|---|
| customers.csv | 10,000 | 10,010 | 50 NULL emails; 10 extra duplicate `customer_id` rows |
| orders.csv | 100,000 | 100,020 | 100 NULL customer_id; 200 NULL product_id; 50 orphan customer_id; 30 orphan product_id; 20 extra duplicate order_id |
| products.csv | 500 | 500 | none listed as mandatory |

Uniqueness module (later Silver) flags **all copies**: 20 customer rows and 40 order rows with uniqueness FAIL, from 10 and 20 extra rows respectively.

Mandatory issue classes are on **disjoint rows**. Listed issue instances total **460**. The guide’s ~700 / 0.7% figure is **not** a generation target. See `requirements-analysis.md`.

## Reproducibility

| Setting | Value |
|---|---|
| Default seed | **42** |
| As-of date | **2026-08-31** (frozen; not `current_date()`) |
| Encoding | UTF-8, no BOM |
| Line endings | LF (`\n`) |
| RNG | `random.Random(seed)` only; no `uuid`, no wall-clock in CSV cells |
| Money rounding | `Decimal` quantized to 0.01 with **ROUND_HALF_EVEN** (banker's rounding) |

Same seed and configuration produce **byte-identical** CSV output. Confirmed by two temp-dir runs and by regenerating over the committed `data/` files (SHA-256 match). `.gitattributes` pins `data/*.csv` to LF so Windows `core.autocrlf` does not rewrite working-tree bytes.

Observed SHA-256 after the seed-42 run that populated `data/`:

| File | SHA-256 |
|---|---|
| customers.csv | `7f8ae14c788c65cd502be5a3c91ac12afc5618634f923c2ea2495834e9e0b044` |
| orders.csv | `b244c3d954320535d50703450145d6ad8cd66a1f0833aac3d2b830b4cf06e6e1` |
| products.csv | `a7e568ace967c07706b0c0274223d8633fe1543be89057d022778d3f5b4b21e9` |

## ID ranges

| Entity | Valid IDs | Orphan namespace (orders only) |
|---|---|---|
| customers | `1..10000` | `90001..90050` (50 distinct values, one per orphan row) |
| products | `1..500` | `9001..9030` (30 distinct values, one per orphan row) |
| orders | `1..100000` | n/a |

NULL foreign keys are empty CSV fields, not the orphan IDs.

## Duplicate design

One mechanism only: after unique rows exist, append exact copies of **clean** source rows (not NULL/orphan/future-signup rows).

Duplicate extra rows are the last 10 customer rows and last 20 order rows. Each extra row is field-for-field identical to its source.

Source `customer_id` values (seed 42):

`1418, 2051, 2067, 2109, 2150, 5017, 5234, 5469, 6742, 9783`

Source `order_id` values (seed 42):

`4699, 12720, 21379, 21670, 22735, 23894, 24396, 28887, 40122, 46808, 48743, 51213, 61010, 61772, 64193, 65207, 69108, 73521, 83635, 92560`

## Optional future signup dates (not in the 460)

**Decision: inject 30 future `signup_date` values.**

Why:

- Silver business logic already has `business:customers.signup_not_future` against the frozen as-of date.
- The example prompt mentioned 30 future signups. That is not a silent change to the mandatory list.
- The 30 rows are a separate, exact, documented count.

How:

- 30 customer rows disjoint from NULL-email rows and from duplicate-source rows.
- Dates are `as_of + 1 day` through `as_of + 30 days` (`2026-09-01` .. `2026-09-30`).
- Those 30 customer IDs are **excluded from the order purchaser pool**. Order dates are capped at as-of, so an order for a post-as-of signup would fail `order_not_before_signup` as an *accidental* extra defect. Excluding them keeps that accidental count at 0.

Future-signup `customer_id` values (seed 42):

`1273, 1318, 1368, 1759, 1896, 2255, 2754, 2988, 3292, 3379, 3684, 3921, 4031, 4048, 4746, 5344, 5522, 5615, 5647, 6615, 6992, 7152, 7552, 7988, 8011, 8016, 8059, 8362, 8493, 9329`

These 30 are **not** counted toward the 460 mandatory issue instances.

## NULL email rows

50 customer rows have an empty `email` field. Disjoint from future signups and duplicate sources.

`customer_id` values (seed 42):

`45, 88, 117, 127, 131, 525, 821, 1102, 1213, 1331, 1558, 1579, 1595, 1681, 1732, 1734, 1995, 2107, 2378, 2546, 3532, 3551, 3611, 5448, 5932, 6349, 6772, 6898, 7063, 7202, 7481, 7625, 7812, 7830, 8000, 8290, 8380, 8416, 8519, 8598, 8703, 8754, 9035, 9047, 9160, 9280, 9433, 9531, 9707, 9895`

## Overlap rules

| Classes | Overlap |
|---|---|
| NULL email ∩ future signup ∩ duplicate customer sources | none |
| NULL order `customer_id` ∩ NULL order `product_id` | none |
| NULL FK ∩ matching orphan on the same field | none (NULL ≠ orphan) |
| Orphan customer_id ∩ orphan product_id on the same order | none |
| Duplicate extra order rows copied from defect rows | none |

No row is intentionally given two mandatory issue types. Optional future signups also stay off the mandatory customer issue rows.

## Referential relationships

1. Generate 500 products and 10,000 unique customers.
2. Generate 100,000 orders with valid `customer_id` / `product_id` (future-signup customers excluded).
3. Then overlay NULL FKs and orphan FKs on disjoint unique-order rows.
4. Then append exact duplicate copies from remaining clean rows so extras cannot create additional NULLs or orphans.

For normal valid records: customer IDs reference customers; product IDs reference products.

## Financial consistency

For every order (including defect rows):

`total_amount = quantity * unit_price`, quantized to 2 decimal places with **ROUND_HALF_EVEN**.

`unit_price` for valid `product_id` is the product’s list `price`. For NULL/orphan `product_id` rows, `unit_price` remains the price of the originally chosen valid product (the `product_id` is then overwritten). Quantity ≥ 1. No negative prices or amounts.

Source `lifetime_value` is a synthetic customer attribute, **not** the Gold `lifetime_value_actual` (which will be summed from qualifying orders later).

## Dates and statuses

| Field | Rule |
|---|---|
| Normal signup_date | `2022-01-01` .. `2026-08-31` |
| Future signup_date | `2026-09-01` .. `2026-09-30` (30 rows) |
| order_date | `max(signup_date, 2023-01-01)` .. `2026-08-31` |
| Completed | `payment_date` not null, `order_date` ≤ `payment_date` ≤ `order_date + 14 days` |
| Cancelled | `payment_date` empty |
| Pending | ~40% have a payment date on/after order_date; otherwise empty |

`order_status` ∈ {Pending, Completed, Cancelled}. Weights: 75% / 15% / 10%.

## Distributions (synthetic)

| Dimension | Distribution |
|---|---|
| customer_segment | Premium 12%, Standard 53%, Basic 35% |
| country | 12 synthetic labels, US-weighted (US 28%, India 12%, UK 10%, …) |
| product category | 10 categories (Electronics, Clothing, Home & Kitchen, …) |
| product price | $5.99–$499.99 |
| cost | 40–75% of price |
| stock / reorder | stock 0–2000; reorder 5–80 |
| quantity | 1–10, weighted toward 1–3 |
| order customer mix | Premium 3×, Standard 2×, Basic 1× relative weight |
| names / emails | first+last from closed lists; `{first}.{last}{id}@example.com` |

All names and emails are synthetic. No real customer PII.

## Configuration

| Argument | Default | Notes |
|---|---|---|
| `--output-dir` | `<repo>/data` (from `__file__`, not a Windows user path) | Required CSVs written here |
| `--seed` | 42 | |
| `--as-of-date` | 2026-08-31 | Frozen as-of for future-signup logic |

Do not hard-code a Databricks workspace path.

## Built-in validation

After write, the generator checks physical counts, columns, nulls, duplicates, unique keys, valid FKs, intended orphans, domains, date/payment rules, financial equality, disjointness, and records SHA-256. Mandatory vs optional vs accidental extras are separate. Failure raises `GenerationError` and does not print success.

## Tests

`tests/` is **not** in the assignment’s required tree. `requirements-analysis.md` §6.15 froze it as the location for meaningful tests.

Run: `python -m unittest tests.test_generate_sample_data -v`

Coverage: physical rows, unique keys, NULLs, duplicates, orphans (NULL ≠ orphan), schema, seed-42 determinism, committed `data/` vs regeneration.

## Responsible AI

Synthetic names/emails only. No credentials in this module or in the CSVs.
