# Design notes

Status: architecture and design decisions for implementation. **Bronze ingest code exists.** **All five Silver quality modules and `create_silver_tables.py` exist** (local Spark / parquet validated; Databricks Delta **PASS**). **Gold SQL aggregations and `create_gold_tables.py` exist** (local Spark / parquet validated; Databricks Gold **PASS**). **Dashboard SQL queries and `DASHBOARD_GUIDE.md` exist** (local Spark / parquet validated; Databricks dashboard SQL **PASS**). **Databricks bootstrap/orchestration exists in `src/databricks/`** and **has been executed** (`python src/databricks/run_pipeline.py`). The published dashboard **DE C1 E-Commerce Sales Dashboard** was rendered **manually in the Databricks SQL UI** (not by Cursor).

Keep this design inside the assignment’s roughly **20–25 hour** core: batch full-refresh PySpark + SQL, five Silver modules, four Gold queries, one dashboard. Rejected extras are listed at the end of this file.

## Goal

A Databricks Medallion pipeline for synthetic e-commerce sales, with Bronze raw ingest, Silver quality flagging, Gold SQL aggregations, and a SQL dashboard.

## Architecture

```
CSV (repo data/  or  configured S3/DBFS/volume path)
    -> Bronze ingest (PySpark, explicit schema, PERMISSIVE, raw source columns)
        -> bronze.customers / bronze.orders / bronze.products
        -> bronze.ingest_metadata
    -> Silver quality modules (PySpark; flag, do not delete)
        -> completeness → uniqueness → type validation → referential integrity → business logic
        -> combine flags on _ingest_row_id
        -> silver.customers / silver.orders / silver.products
        -> silver.quality_metrics
    -> Gold aggregations (Spark SQL / Databricks SQL files)
        -> gold.sales_by_product
        -> gold.revenue_by_customer
        -> gold.daily_trends
        -> gold.weekly_trends
        -> gold.customer_segmentation
    -> Databricks SQL Dashboard (3+ tiles + filters)
```

Catalog name is configuration (`MEDALLION_CATALOG` or `src/config.py` at implementation). Schema names are `bronze`, `silver`, `gold`.

---

## Source data flow

1. Stage 2 writes synthetic `data/customers.csv`, `data/orders.csv`, `data/products.csv` with documented defects.
2. Bronze reads those files (or the configured remote prefix that contains the same files).
3. Each entity is written as a Delta table with source columns unchanged plus `_ingest_row_id`.
4. Silver reads Bronze only (never rereads CSV for quality logic, so Bronze remains the system of record for raw data).
5. Gold reads Silver only.
6. Dashboard datasets point at Gold tables/views.

There is no CDC, no streaming, and no Silver→Bronze write path.

---

## Table naming

| Layer | Name | Grain |
|---|---|---|
| Bronze | `{catalog}.bronze.customers` | one CSV row |
| Bronze | `{catalog}.bronze.orders` | one CSV row |
| Bronze | `{catalog}.bronze.products` | one CSV row |
| Bronze | `{catalog}.bronze.ingest_metadata` | one row per source file per ingest run |
| Silver | `{catalog}.silver.customers` | same as Bronze customers |
| Silver | `{catalog}.silver.orders` | same as Bronze orders |
| Silver | `{catalog}.silver.products` | same as Bronze products |
| Silver | `{catalog}.silver.quality_metrics` | one row per (table_name, check_name, computed_at) |
| Gold | `{catalog}.gold.sales_by_product` | one row per product_id |
| Gold | `{catalog}.gold.revenue_by_customer` | one row per customer_id |
| Gold | `{catalog}.gold.daily_trends` | one row per order_date |
| Gold | `{catalog}.gold.weekly_trends` | one row per Monday week start |
| Gold | `{catalog}.gold.customer_segmentation` | one row per segment_type |

Do not use personal database names (`udayan_dev`, etc.) in committed SQL.

---

## Schema strategy

- **Contract:** `data-model.md` and `database/schema.sql`.
- **Read:** explicit StructType / Spark SQL types. Do not let `inferSchema` drive the written table.
- **Diagnostic only:** optional log of inferred schema vs declared schema.
- **Mode:** `PERMISSIVE` so bad tokens become null. Never `DROPMALFORMED`.
- **Bronze constraints:** no PRIMARY KEY, UNIQUE, FOREIGN KEY, or NOT NULL constraints on Bronze tables. Those would reject the required defects.
- **Silver/Gold:** quality and grain are enforced in code/SQL, not by rejecting loads.
- **Decimals:** `DECIMAL(18,2)` for money.
- **Dates:** `DATE`. CSV format `yyyy-MM-dd`.

---

## Bronze immutability

Source field values are not cleaned, filled, deduplicated, or repaired.

Allowed:

- Adding `_ingest_row_id` (lineage).
- Writing `bronze.ingest_metadata`.
- Casting to the declared schema on read (type coercion is ingest, not business repair). Unparsable values become null and remain visible.

Forbidden:

- Dropping rows.
- Filling emails or FKs.
- Deduplicating PKs.
- Updating Bronze from Silver.
- `CREATE OR REPLACE` that writes transformed business values.

**Rerun:** Bronze entity tables are **overwritten** from the current CSVs (full refresh). That replace is not a mutation of a previous cleaned copy; it is a fresh raw load. Tests after a rerun: Bronze row counts still match the files; a hash/except of source columns vs CSV still matches.

Silver and Gold must not `INSERT`/`UPDATE`/`DELETE` Bronze.

---

## Ingestion metadata

`bronze.ingest_metadata` columns:

| Column | Meaning |
|---|---|
| ingest_id | UUID (or equivalent) for the run |
| source_file | Configured path actually read |
| table_name | `customers` / `orders` / `products` |
| row_count | Rows written to that Bronze table |
| ingested_at | Timestamp of the run |
| status | `SUCCESS` or `FAILED` |
| error_message | Null on success; short error on failure (no secrets) |

One row per source file per run. **Append-only** (do not overwrite history). Entity tables still full-refresh.

Also log the same counts with the standard Python/Spark logger.

Fail the job if a source file is missing or has no header. Do not write an empty Bronze table and report success.

---

## Silver validation sequencing

Modules are **independent readers of Bronze**. They must not depend on another module having already flagged a row. Recommended execution order is documentation and orchestration only:

1. Completeness
2. Uniqueness
3. Type validation
4. Referential integrity (needs parent key sets from Bronze customers/products)
5. Business logic (may look at parent signup_date for order-vs-signup)
6. `create_silver_tables.py` joins module outputs on `_ingest_row_id`

Referential integrity and business-logic customer lookups use **sets of parent keys that exist**, including keys that are duplicated. Existence is “at least one parent row has this id,” not “exactly one.”

---

## Row-level quality status

Silver copies every Bronze source column and `_ingest_row_id`, then adds:

| Column | Rule |
|---|---|
| completeness_pass | boolean |
| uniqueness_pass | boolean |
| type_validation_pass | boolean |
| referential_integrity_pass | boolean |
| business_logic_pass | boolean |
| failed_checks | `ARRAY<STRING>` of rule codes |
| quality_check_result | `PASS` if `failed_checks` is empty; else `FAIL` |

Products have no FKs: `referential_integrity_pass` is `true` for every product row (module still runs and records a 100% pass metric).

---

## Multiple simultaneous quality failures

Modules **do not write** `quality_check_result` by themselves in a last-writer-wins way.

Each module returns:

- `_ingest_row_id`
- `<module>_pass`
- `<module>_failed_checks` (array, possibly empty)

Combiner:

```
failed_checks = concat(completeness, uniqueness, type, ri, business_logic)
quality_check_result = CASE WHEN size(failed_checks) > 0 THEN 'FAIL' ELSE 'PASS' END
```

A NULL FK plus a negative quantity produces two codes, for example:

- `completeness:orders.customer_id`
- `business_logic:orders.quantity_positive`

Neither code replaces the other. Metrics count **rule failures** and **distinct FAIL rows** separately.

Join key for combining is `_ingest_row_id`, never `order_id` / `customer_id` alone.

---

## Quality metrics

`silver.quality_metrics`:

- `table_name`
- `check_name` (module, rule code, distinct-key uniqueness, or combined `quality_check_result`)
- `total_evaluated`, `pass_count`, `fail_count`
- `pass_pct`, `fail_pct` (fail_count / **that check’s** total_evaluated)
- `expected_fail_count` (nullable Stage 2 contract)
- `population_kind` (`physical_row` / `distinct_key` / `table_outcome`)
- `computed_at`

Also emit a **table-level** summary: distinct physical rows with `quality_check_result = FAIL`.

Do not gate the job on `fail_pct = 0`. Intentional defects must fail checks. Tests assert expected fail counts, not a clean warehouse.

Thresholds are documented in `data-quality-strategy.md`. They are detection targets, not “pipeline red” SLOs.

---

## Gold dependencies

Gold reads Silver. It does not read CSV or Bronze (except that Bronze and Silver row counts must match in tests).

Default **qualifying order** (every Gold fact unless a query header says otherwise):

```
order_status = 'Completed'
AND quality_check_result = 'PASS'
```

`PASS` already requires uniqueness, so duplicate order copies are not summed.

**Canonical product** for joins: `silver.products` where `uniqueness_pass` (planned data has unique product_id). If a duplicate product ever appears, take `min(_ingest_row_id)` per `product_id` only for dimension attributes — facts still exclude FAIL orders.

**Canonical customer** for revenue/segmentation dimension attributes: `quality_check_result = 'PASS'` customers. Duplicate customer_id rows are FAIL on uniqueness, so they do not appear as extra dimension rows. Orders that PASS still carry `customer_id` and aggregate to that id. A customer whose **only** Silver rows are uniqueness failures (the 10 duplicate extras plus their originals) will not have a PASS customer dimension row; revenue-by-customer should still list `customer_id` from PASS **orders**, and look up name/segment from a canonical parent (`min(_ingest_row_id)` among Bronze/Silver rows for that id) so Gold does not drop real customers whose only defect is a duplicated profile row.

**Practical join for customer attributes:**

```
FROM qualifying_orders o
LEFT JOIN canonical_customers c ON o.customer_id = c.customer_id
```

`canonical_customers` = one row per `customer_id` using `row_number() = 1` ordered by `_ingest_row_id`. This is a **Gold-only** anti-fan-out pattern. Silver still retains every duplicate.

Inactive customers: all canonical customer_ids with zero qualifying orders, including customers who only have pending/cancelled/failing orders.

---

## Aggregation rules

| Measure | Rule |
|---|---|
| total_orders | `COUNT(*)` of qualifying orders |
| total_revenue | `SUM(total_amount)` of qualifying orders |
| avg_order_value | `total_revenue / NULLIF(total_orders, 0)` |
| lifetime_value_actual | same as that customer’s `total_revenue` |
| daily/weekly | same measures, grouped by `order_date` / Monday week start |

Do not `SUM(quantity)` as `total_orders`. Do not `AVG(avg_order_value)` across groups. Do not use source `lifetime_value` as `lifetime_value_actual`.

Sales by product joins qualifying orders to canonical products on `product_id`. Orders that PASS still have a valid product_id by construction of the quality modules (completeness + RI + types). If a query is written incorrectly without those filters, tests must catch double counts.

---

## Customer segmentation rules

Mutually exclusive, one label per canonical customer:

| Order | segment_type | Rule |
|---|---|---|
| 1 | Inactive | qualifying order count = 0 |
| 2 | High-Value | qualifying revenue >= 1000.00 |
| 3 | Repeat | qualifying order count >= 2 |
| 4 | One-Time | qualifying order count = 1 |

A one-order customer with revenue >= 1000.00 is High-Value, not One-Time. A repeat customer above the threshold is High-Value, not Repeat.

Outputs: `segment_type`, `customer_count`, `avg_revenue` (`total_revenue / customer_count`), `total_revenue`.

The 1000.00 threshold is a design default. After generation, change it only with a documented note if the bucket is empty or nearly universal.

---

## Dashboard dependencies

Tiles read Gold, not Silver:

1. Top 10 products by revenue — `gold.sales_by_product` `ORDER BY total_revenue DESC, product_id ASC LIMIT 10` (bar). Tie-break is `product_id` so Top-N is deterministic.
2. Customer revenue distribution — `gold.revenue_by_customer.lifetime_value_actual` (histogram in Databricks SQL). Population is all canonical customers, including zeros. Binning is a visualization setting, not SQL.
3. Customer segmentation — `gold.customer_segmentation` (pie on `customer_count`). SQL does not recompute the 1000.00 / Repeat / One-Time CASE.

Filters (implemented in the guide; Databricks widgets attached on the published dashboard):

- `category` on Tile 1 as a query parameter **before** `LIMIT 10`
- `customer_segment` on Tile 2

Rejected for these tiles: order date range (no date grain), `country` (not on Gold), `segment_type` on the pie (would hide the four-way mix). Exact widget steps: `DASHBOARD_GUIDE.md`. Visual rendering was completed manually in the Databricks UI. No fake screenshots. Cursor did not generate the visual dashboard.

---

## Error handling

| Event | Behavior |
|---|---|
| Missing CSV / empty file | Fail ingest; no SUCCESS metadata; do not create empty “success” tables |
| Schema extra/missing columns | Fail ingest with a clear column diff |
| Unparsable value | Null in Bronze (PERMISSIVE); Silver type/completeness flags |
| Spark job exception | Fail; metadata row with status FAILED if the run got far enough to record it |
| Gold division by zero | `NULLIF` on denominators |
| Dashboard warehouse down | Document as environment failure; do not invent query results |

No retry/merge complexity. Rerun the full refresh.

---

## Rerun / idempotency

Given the same CSVs and config:

- Bronze entity tables: overwrite → same source column contents.
- `_ingest_row_id` values may differ across clusters; tests must not assert those ids.
- `ingest_metadata`: **new append rows** each run (audit).
- Silver/Gold: overwrite/recreate from current Bronze/Silver.

Not idempotent: metadata history grows. That is intended.

Two concurrent runs are out of scope; document “do not run overlapping jobs.”

---

## Configuration / path strategy

Introduce at Bronze implementation (not in this commit):

| Setting | Purpose | Default for local |
|---|---|---|
| `MEDALLION_CATALOG` | UC catalog or unused for HMS | unset (do not assume `main`) |
| `MEDALLION_DATA_PATH` | Directory containing the three CSVs | repo `data/` |
| `MEDALLION_BRONZE_SCHEMA` | schema name | `bronze` |
| `MEDALLION_SILVER_SCHEMA` | schema name | `silver` |
| `MEDALLION_GOLD_SCHEMA` | schema name | `gold` |

No tokens, JDBC passwords, or personal DBFS paths in git. `.env` is gitignored.

---

## Logging / observability

Use Python `logging` (INFO):

- run id, source path, row counts, duration per entity
- each Silver module fail_count
- Gold row counts after write

Do not log full email lists or entire DataFrames. Synthetic data is still not dumped at INFO.

Spark UI is enough for stage-level debugging; no extra metrics stack.

---

## Testing strategy

Add `tests/` when implementation starts. Planned real tests (must actually run):

| Test | Proves |
|---|---|
| Generation counts vs contract | 10,010 / 100,020 / 500; each injected issue count |
| Generator seed stability | second run identical bytes or identical aggregates |
| Bronze count = file count | no silent drop |
| Bronze source columns except-all vs CSV sample/full | no value repair |
| Bronze rerun does not change source columns | overwrite is raw |
| Silver count = Bronze count per entity | no deletes |
| Each module flags known injected defects | completeness 50/100/200, uniqueness 20+10 keys, RI 50/30, etc. |
| NULL FK is completeness FAIL and RI PASS | classification |
| Multi-failure row has 2+ `failed_checks` | no overwrite |
| Gold columns present; queries execute | contract |
| Duplicate orders do not double revenue | join/uniqueness protection |
| `avg_order_value` uses NULLIF | no div/0 |
| Segmentation labels mutually exclusive and exhaustive | four types, sum of counts = canonical customers |
| Repo grep: no secrets patterns | responsible AI |

If a test cannot run (no Spark), say so and do not claim PASS.

---

## Processing engines

| Layer | Engine |
|---|---|
| Data generation | Python (local OK) |
| Bronze | PySpark |
| Silver | PySpark |
| Gold | `.sql` files executed by Spark SQL / Databricks SQL |
| Dashboard | Databricks SQL |
| Orchestration | Databricks: `src/databricks/run_pipeline.py` calls the existing `ingest_all.py`, `create_silver_tables.py`, and `create_gold_tables.py`. Local parquet CLIs remain. Orchestrators must not replace Gold SQL with hidden PySpark aggregations |

---

## Key decisions

| Decision | Choice | Why |
|---|---|---|
| Processing engine | PySpark for Bronze/Silver | Distributed processing requirement |
| Gold / dashboard | SQL files | Spec |
| Bad rows | Flag, retain | Spec forbids deleting rows to pass checks |
| Bronze | Immutable source columns | Spec |
| Quality modules | All five | Required tree + documented business logic |
| Defect counts | Listed 460 issue instances, not padded to 700 | `requirements-analysis.md` |
| Catalog/schema/paths | Parameters / env | Environment portability; no secrets |
| Data | Synthetic only | Responsible AI |
| CSV parse | Explicit schema, PERMISSIVE | Do not drop rows; do not rely on inference |
| Duplicate uniqueness | All copies FAIL | Detection, not silent survivor in Silver |
| NULL vs orphan | Completeness vs RI | Verifiable counts |
| Gold facts | Completed + PASS | Avoid cancelled/duplicate revenue |
| Segmentation | Mutually exclusive priority list | Pie chart needs one label |
| Scope | Batch full refresh | 20–25 hour assignment |

---

## Senior-engineer design review

Reviewer stance: this design is sufficient for the assignment if the failure modes below are tested. It is not a production lakehouse platform.

### Failure mode 1 — Duplicate amplification from joins

**Why it could occur:** `orders.order_id` and `customers.customer_id` are duplicated on purpose. Joining Silver modules on those keys, or joining facts to duplicated dimension rows, multiplies revenue and order counts.

**Prevention / detection:** Combine quality outputs on `_ingest_row_id`. Gold facts use `quality_check_result = 'PASS'` (uniqueness must pass). Customer/product attribute joins use a canonical `row_number` per natural key.

**Test:** Build a fixture with one `order_id` duplicated and a duplicated customer dimension; assert Gold `total_revenue` equals the single qualifying amount, not 2x or 4x.

### Failure mode 2 — NULL semantics mixed with orphans

**Why it could occur:** SQL `NOT IN` / anti-join treats NULL specially; an implementer may mark NULL `customer_id` as “absent from customers.”

**Prevention / detection:** RI module filters `customer_id IS NOT NULL` (and product_id) before the anti-join. Completeness owns NULLs. Metrics for orphans must equal 50 and 30 on generated data.

**Test:** Fixture with one NULL FK and one orphan FK; assert different `failed_checks` codes; assert RI pass for the NULL row.

### Failure mode 3 — Accidental Bronze mutation

**Why it could occur:** Silver code overwrites Bronze, or ingest “helps” by filling emails, or a second write path uses `merge`.

**Prevention / detection:** Silver/Gold jobs only read Bronze. Ingest does not fill/dedupe. After Silver, compare Bronze source columns to CSV.

**Test:** Run ingest, capture a checksum of `bronze.orders` source columns, run Silver, checksum again; must match. Row count equals file count.

### Failure mode 4 — Accidental row deletion

**Why it could occur:** `DROPMALFORMED`, `dropna()`, filtering FAIL rows into Silver, or `INNER JOIN` parents that drop NULL FK orders.

**Prevention / detection:** PERMISSIVE reads; Silver is Bronze left-joined to flags, not inner-joined to “valid parents.” Silver count equals Bronze count.

**Test:** Assert Silver row counts == Bronze row counts for all three entities, including rows with NULL FKs.

### Failure mode 5 — Aggregation double counting

**Why it could occur:** Counting all statuses; summing duplicate PASS rows if uniqueness is wrongly implemented as “first copy PASS”; `SUM(total_amount)` after a fan-out join.

**Prevention / detection:** Qualifying predicate includes uniqueness PASS; canonical dimensions; `COUNT(*)` of orders not a post-join explosion. SQL comments state grain.

**Test:** Known one-product, two-duplicate-order fixture: `total_orders = 0` for those copies (both FAIL uniqueness) or, if a separate canonical-fact test is added, revenue not doubled. Also a PASS order joined to two product rows must not exist in Gold.

### Failure mode 6 — Inconsistent quality-status handling

**Why it could occur:** Each module writes `quality_check_result` last-wins; Gold filters `completeness_pass` only; metrics use a different definition than the row flag.

**Prevention / detection:** Combiner owns `quality_check_result`. Gold documents the same predicate. Metrics derived from the same flag columns.

**Test:** A row that fails completeness and business logic has both codes in `failed_checks` and overall FAIL. Metric fail_count for each module includes that row.

### Failure mode 7 — Nondeterministic sample-data generation

**Why it could occur:** Unseeded `random` / Faker; `uuid4` in business keys; unordered sets when assigning orphan ids.

**Prevention / detection:** Frozen seed; deterministic id ranges; documented orphan id namespace that cannot collide with real PKs; generation notes record the seed.

**Test:** Run generator twice in a temp dir; file hashes or sorted row hashes match.

### Failure mode 8 — Hard-coded paths and environment-specific assumptions

**Why it could occur:** `/Workspace/Users/udayan/...`, `s3://company-prod/...`, or a personal catalog committed in SQL.

**Prevention / detection:** Config/env; SQL uses schema names `bronze`/`silver`/`gold` without workspace URLs. Reviewer grep before commit.

**Test:** Static check (ripgrep) that committed `src/` has no `dbfs:/Users/`, no `https://` workspace hosts, no `AKIA`, no `.env` files.

### Failure mode 9 — Foreign-key validation using the wrong parent set

**Why it could occur:** Looking up Silver customers that already dropped duplicates; comparing types (string `"1"` vs int `1`); using `COUNT` join instead of `IS IN`.

**Prevention / detection:** RI uses Bronze parent key sets (distinct ids). Typed schema on both sides. Semi/anti join on equal types.

**Test:** Orphan product_id not in the 500 real ids is FAIL; a real product_id is PASS even if that product later fails a business rule.

### Failure mode 10 — DECIMAL/NULL arithmetic in Gold

**Why it could occur:** `DOUBLE` inference; `AVG` of nulls; divide by zero for customers with zero orders in a filtered subset; `total_amount` scale vs `quantity * unit_price`.

**Prevention / detection:** `DECIMAL(18,2)`; `NULLIF`; business-logic amount equality with documented tolerance (0.01). Inactive `avg_revenue` uses `customer_count` not order count.

**Test:** Empty qualifying-order group returns null average, not an exception. Amount mismatch of 0.01 flags BL; exact 10.00 * 2 = 20.00 passes.

---

## Open design items (not silently closed)

- Exact Unity Catalog catalog name in the candidate’s workspace (config at runtime).
- Whether local tests use a temp SparkSession or Databricks Connect.
- Histogram binning if Databricks visualization cannot histogram the raw measure (fallback not implemented; viz-side binning remains the contract).
- High-Value threshold sanity check after real generated distributions exist.

Stage 2 closed: 30 future signup dates **were injected** (see `DATA_GENERATION_NOTES.md`).

## Rejected over-engineering (out of 20–25 hour scope)

- Streaming / Autoloader / CDF
- SCD2 customer history
- dbt, Great Expectations, Monte Carlo, or other DQ platforms
- Airflow/ADF orchestration
- Quarantine tables (would look like deleting from the main Silver grain)
- RFC-level email validation
- Country ISO allowlists
- Terraform, multi-workspace promotion
- Row-level Unity Catalog grants as a deliverable

## Non-goals for this stage

These notes do not include measured row counts, query runtimes, dashboard screenshots, or test pass/fail results.
