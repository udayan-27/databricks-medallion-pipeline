# Dashboard guide

Status: **queries implemented and locally validated**. A Databricks SQL dashboard has **not** been created or rendered in a workspace from this environment. There are no screenshots. Do not treat this file as evidence that Tile 1–3 were published.

Local validation uses PySpark `spark.sql` against Gold parquet tables. Databricks validation would use Databricks SQL against Delta tables in Unity Catalog or the Hive metastore. Those are different runtimes.

## 1. Prerequisites

Before anyone can build the dashboard in Databricks:

1. Stage 2 CSVs exist (`data/customers.csv`, `data/orders.csv`, `data/products.csv`). Do not regenerate them for the dashboard.
2. Bronze ingest has been run (`src/bronze/ingest_all.py`).
3. Silver tables have been written (`src/silver/create_silver_tables.py`).
4. Gold tables have been written (`src/gold/create_gold_tables.py`) from those Silver tables.
5. A SQL warehouse (or a cluster that can serve Databricks SQL) is running.
6. The operator can `SELECT` from the Gold schema. No extra grants are specified by the assignment.

Local-only path (this repository's tests):

- Python 3.11 venv + JDK 17 + PySpark 3.5.6
- `MEDALLION_TABLE_FORMAT=parquet`
- Gold tables created in an isolated Spark warehouse by `tests/test_dashboard_queries.py`

That local path does **not** create a Databricks dashboard.

Automated Databricks SQL validation (queries against `workspace.gold.*`) is part of `python src/databricks/run_pipeline.py`. That command has **not** been executed from this environment. Visual tile rendering remains a Databricks UI action and has **not** been created or rendered.

## 2. Catalog / schema assumptions

| Setting | Local default | Databricks |
|---|---|---|
| Catalog | unset (Hive metastore) | `MEDALLION_CATALOG` if Unity Catalog is used |
| Gold schema | `gold` | `gold` or `<catalog>.gold` |
| Table format | `parquet` in tests | `delta` |

SQL in `dashboard_queries.sql` uses `{gold_schema}` so tests can point at an isolated schema. In the Databricks SQL Editor, replace `{gold_schema}` with `gold` or `<catalog>.gold`. Do not commit a personal catalog name, workspace URL, or DBFS user path.

Example after substitution, no catalog:

```sql
SELECT product_id, product_name, category, total_revenue, total_orders
FROM gold.sales_by_product
ORDER BY total_revenue DESC, product_id ASC
LIMIT 10
```

Example with Unity Catalog catalog `dev`:

```sql
FROM dev.gold.sales_by_product
```

## 3. Required Gold tables

Dashboard analytics read Gold only.

| Table | Grain | Required by |
|---|---|---|
| `gold.sales_by_product` | one row per `product_id` with ≥1 qualifying order | Tile 1, category filter values |
| `gold.revenue_by_customer` | one row per canonical `customer_id` (including zeros) | Tile 2, customer_segment filter values |
| `gold.customer_segmentation` | four rows (`High-Value`, `Repeat`, `One-Time`, `Inactive`) | Tile 3 |

Not used by the required tiles:

- `gold.daily_trends`
- `gold.weekly_trends`

Those tables have a date grain. They are **not** a date filter for Tile 1–3. Filtering product or customer Gold facts by `order_date` would require re-aggregating Silver, which this dashboard must not do.

Qualifying-order definition is already inside Gold (`Completed` + `quality_check_result = 'PASS'`). Dashboard SQL does not repeat it.

## 4. Dashboard creation steps

These are workspace click-path instructions. They have **not** been executed here. Repository automation validates the SQL; it does not create the visual dashboard.

1. Confirm Gold tables exist: `SHOW TABLES IN gold` (or `SHOW TABLES IN <catalog>.gold`).
2. Open **SQL Editor**. Create a new query. Paste each `DASHBOARD_QUERY` from `src/dashboard/dashboard_queries.sql` after substituting `{gold_schema}`.
3. Save three tile queries with stable names, for example:
   - `top_10_products`
   - `customer_revenue_distribution`
   - `customer_segmentation`
4. Optionally save the two `filter_values_*` queries for dropdowns.
5. For each tile query, add a visualization (section 6).
6. Create a **Databricks SQL Dashboard** (classic SQL Dashboards) **or** a **Databricks Dashboard** (Lakeview) if that is what the workspace provides. The assignment asks for a Databricks SQL Dashboard with three tiles; either product can host the same queries.
7. Add the three visualizations as tiles.
8. Attach filters as specified in section 9. Do **not** add a category widget on Tile 1's already-limited result.
9. Run the dashboard against the SQL warehouse. Record warehouse id / dashboard URL only in private notes, not in this repo.

If the workspace has no SQL warehouse, stop. Do not pretend a dashboard exists.

## 5. Query-to-tile mapping

| Tile | Query name in `dashboard_queries.sql` | Gold table | Viz |
|---|---|---|---|
| 1. Top 10 products by revenue | `top_10_products` | `sales_by_product` | bar |
| 2. Customer revenue distribution | `customer_revenue_distribution` | `revenue_by_customer` | histogram |
| 3. Customer segmentation | `customer_segmentation` | `customer_segmentation` | pie |

Supporting queries (not tiles): `filter_values_category`, `filter_values_customer_segment`.

## 6. Visualization type

| Tile | Type | Why |
|---|---|---|
| 1 | Bar | Ranked comparison of product revenue. Horizontal bars are acceptable if product names are long; it is still a bar chart. |
| 2 | Histogram | Distribution of a numeric measure (`lifetime_value_actual`) over the customer population. Not a bar of pre-built buckets. |
| 3 | Pie | Mix of exclusive `segment_type` counts. |

Do not use a table viz as a substitute for the required chart types in the submitted dashboard.

## 7. Axis configuration

### Tile 1 — bar

| Axis | Field | Aggregation in the viz |
|---|---|---|
| Category / X (or bar label) | `product_name` | none (already one row per product) |
| Measure / Y | `total_revenue` | none, or `SUM` of a single Gold row (do not `AVG`) |
| Optional color | `category` | none |

Sort: the SQL already returns descending revenue with `product_id` as the tie-break. If the viz has its own sort control, set it to follow the query or to `total_revenue` descending. Do not re-sort alphabetically.

Tooltip: `product_id`, `category`, `total_orders` are useful. Do not add Silver fields.

### Tile 2 — histogram

| Setting | Field |
|---|---|
| Numeric values to bin | `lifetime_value_actual` |
| Count | customer rows (implicit; one Gold row per customer) |

Do **not** put `customer_id` on the category axis (that would draw one bar per customer). Do **not** put `customer_segment` on the bin axis unless you are splitting series after the population filter.

### Tile 3 — pie

| Setting | Field |
|---|---|
| Slice label | `segment_type` |
| Slice size | `customer_count` |

Aggregation: none or `SUM` of the Gold count column. Do not `COUNT(*)` the four Gold rows (that would make every slice 1).

## 8. Histogram configuration

**Chosen approach:** one Gold customer row per canonical customer; Databricks visualization bins `lifetime_value_actual`. SQL does not invent bucket widths.

This matches `requirements-analysis.md` §6.17. There is no required bucketing strategy in the Gold contract.

Recommended Databricks histogram settings (configure in the viz, not in SQL):

- Column: `lifetime_value_actual`
- Bin count: auto, or a modest fixed count such as 20 if auto is noisy
- Include zeros: **yes**
- Scale: linear (log scale would hide the Inactive spike at 0.00)

If a workspace histogram viz cannot bin a raw measure, document that limitation and only then add a SQL `WIDTH_BUCKET` with a stated width. That fallback has **not** been implemented because it has not been observed.

Zero-revenue customers (`lifetime_value_actual = 0.00`) are Inactive in Gold. They must remain in this query. A spike at zero is expected.

## 9. Filter configuration

Only two filters. Both use Gold columns. Neither requires joining Silver or Bronze.

| Filter | Field | Gold table | Tiles | Reason | Expected behavior |
|---|---|---|---|---|---|
| Category | `category` | `sales_by_product` | Tile 1 only | Merchandise slice of Top 10 | Bind as a **query parameter** in a `WHERE` clause **before** `LIMIT 10`. Selecting `Home` returns the top 10 (or fewer) products in `Home`, not the global top 10 with other categories hidden. Unset / All → unfiltered Top 10. |
| Customer segment | `customer_segment` | `revenue_by_customer` | Tile 2 only | Source tier (`Premium` / `Standard` / `Basic`) | Filter the histogram population. Revenue values stay Gold `lifetime_value_actual`. Empty selection → no rows. |

Dropdown sources:

- Category: query `filter_values_category`
- Customer segment: query `filter_values_customer_segment`

### Tile 1 category — correct vs incorrect

Correct (parameter before limit):

```sql
SELECT product_id, product_name, category, total_revenue, total_orders
FROM gold.sales_by_product
WHERE (:category IS NULL OR category <=> :category)
ORDER BY total_revenue DESC, product_id ASC
LIMIT 10
```

Incorrect: add a dashboard-level filter on the **result** of the unfiltered `LIMIT 10` query. That keeps global winners and drops other categories, which is not “top 10 in this category.”

Local tests execute the unfiltered `top_10_products` query and separately assert the pre-`LIMIT` filter pattern against Gold tables. They do not bind Databricks `:category` parameters.

### Filters not used

| Filter | Why not |
|---|---|
| Order date range | Tile 1–3 Gold tables have no `order_date`. Using `daily_trends` would be a different tile. Re-aggregating Silver in the dashboard would duplicate Gold. |
| `country` | Not a Gold revenue-by-customer column. |
| `segment_type` on the pie | The pie exists to show all four exclusive buckets, including empty ones. |
| `customer_segment` on the pie | Pie is Gold `segment_type`, not source `customer_segment`. Joining to filter would rebuild Gold. |
| Category on the histogram | Histogram grain is customer, not product. |

Do not add extra filters to look busy.

## 10. Expected business interpretation

### Tile 1

“These are the products with the highest qualifying revenue.” Unused products are absent because Gold omits them. Ties are broken by `product_id`, not by name. Pending, cancelled, duplicate, and FAIL orders are already excluded in Gold.

### Tile 2

“This is the distribution of actual qualifying revenue per canonical customer.” `lifetime_value_actual` is not the source `customers.lifetime_value` column. Zeros are Inactive customers, including people whose only orders failed quality checks or were not Completed. Filtering to `Premium` answers “how is Premium customer revenue distributed?” not “what is Premium’s share of Gold segmentation.”

### Tile 3

“Every canonical customer has exactly one of: Inactive, High-Value (≥ 1000.00 qualifying revenue), Repeat (≥ 2 qualifying orders and not High-Value), One-Time (exactly one qualifying order and not High-Value).” `SUM(customer_count)` equals the Gold customer population (10,000 on seed-42). Empty slices have count 0.

## 11. Dashboard QA checklist

Use this in Databricks after the dashboard is actually built. Items marked local have already been executed in this repository.

- [x] **Local:** Tile 1 SQL returns ≤ 10 rows (`tests/test_dashboard_queries.py`)
- [x] **Local:** Tile 1 order is `total_revenue DESC`, `product_id ASC`
- [x] **Local:** Tile 2 row count equals Gold `revenue_by_customer`; `customer_id` unique
- [x] **Local:** Tile 2 includes `lifetime_value_actual = 0.00` customers
- [x] **Local:** Tile 3 counts match Gold `customer_segmentation`; four types; sum reconciles
- [x] **Local:** queries reference Gold tables only (no Bronze/Silver in executable SQL)
- [x] **Local:** no eligibility/`1000.00`/segmentation `CASE` reimplementation in dashboard SQL
- [x] **Local:** no SQL histogram buckets
- [x] **Local:** category filter-before-limit pattern does not equal filter-after-limit
- [ ] **Databricks UI:** bar / histogram / pie actually rendered — **not done**
- [ ] **Databricks UI:** category parameter recomputes Top 10 inside the category — **not done**
- [ ] **Databricks UI:** histogram bins configured in the viz — **not done**
- [ ] **Databricks UI:** pie shows Gold `customer_count` (not a count of four rows) — **not done**
- [ ] **Databricks UI:** zero-count segments visible or documented as a viz hide — **not done**

Do not check the Databricks boxes without a warehouse run.

## 12. Local vs Databricks limitations

| | LOCAL VALIDATION | DATABRICKS VALIDATION |
|---|---|---|
| Engine | PySpark | Databricks SQL warehouse |
| Table format | parquet | Delta |
| Catalog | none / Hive metastore | Unity Catalog or workspace HMS |
| What was run | `spark.sql` of `dashboard_queries.sql` in unit tests | **not run** |
| Automated SQL checks | local Spark against parquet Gold | `src/databricks` dashboard SQL validation (**not run** in a workspace from this environment) |
| What it proves | Query logic vs Gold contract, Top-N, population, segments, filter pattern | Tiles, viz types, widget filters, warehouse permissions |
| What it cannot prove | Bar/histogram/pie rendering, widget UX, auto-bin appearance | Nothing until executed |

Never claim the dashboard was rendered in Databricks unless that run happened.

## Tile documentation summary

| Tile | Gold source | Business question | Dimensions | Measures | Ordering | Interpretation | Filters |
|---|---|---|---|---|---|---|---|
| Top 10 products | `sales_by_product` | Highest qualifying product revenue | `product_id`, `product_name`, `category` | `total_revenue` | `total_revenue DESC`, `product_id ASC`, `LIMIT 10` | Gold already excludes unused products and non-qualifying orders | `category` **before** `LIMIT` |
| Customer revenue distribution | `revenue_by_customer` | Shape of customer revenue | `customer_id`, `customer_segment` | `lifetime_value_actual` | `customer_id ASC` (stable, not a rank) | All canonical customers; zeros = Inactive; viz bins | `customer_segment` |
| Customer segmentation | `customer_segmentation` | Exclusive segment mix | `segment_type` | `customer_count` | none required | Four buckets, mutually exclusive, empty allowed | none |

## Edge cases (deterministic)

| Case | Behavior |
|---|---|
| Fewer than 10 products in Gold | Tile 1 returns that many rows |
| Tied `total_revenue` | Lower `product_id` ranks first among ties |
| Tie at the 10th slot | Lowest remaining `product_id` values fill the limit; others are excluded |
| Zero-revenue customers | Tile 2 keeps them; Tile 3 Inactive includes them |
| Empty Gold segment | Tile 3 still has the row with `customer_count = 0` |
| Null `category` | Tile 1 includes the row; category dropdown includes NULL |
| One-segment population | Pie still has four Gold rows; three may be zero |
| No Gold products | Tile 1 returns 0 rows |
| No Gold customers | Tile 2 returns 0 rows; Tile 3 still has four zero-count types if Gold was built from an empty canonical set |

Do not over-engineer extra dashboard datasets for these cases.
