# Requirements analysis

Canonical source: `DE_C1_REQUIREMENTS.md`. This document records how those requirements are interpreted, which human decisions resolve ambiguities, and how each requirement traces to artifacts. It does not claim that the pipeline has been implemented or validated.

**Stage status:** requirements and architecture review complete. **Stage 2 data generation is complete** (seed 42). **Bronze ingest code is complete.** Local Spark parquet Bronze ingest tests passed. **All five Silver quality modules and Silver table orchestration are implemented and locally validated.** Gold / Dashboard have **not** started.

## 1. Problem statement

Build a complete Databricks Medallion Architecture pipeline for synthetic e-commerce sales data:

`CSV / S3 / DBFS -> Bronze -> Silver -> Gold -> Dashboard`

The pipeline must ingest three source files (`customers.csv`, `orders.csv`, `products.csv`), preserve raw Bronze data, apply documented Silver data-quality checks without deleting bad rows, produce Gold business aggregations in SQL, and support a Databricks SQL dashboard.

The exercise also requires engineering evidence: requirement analysis, architecture, AI-assisted implementation with recorded prompts, testing, validation of AI output, debugging notes, documentation, responsible AI usage, human ownership of decisions, and meaningful Git history.

## 2. Functional requirements

### 2.1 Source data (mandatory when generated)

| Dataset | Target rows (guide wording) | Keys / notable fields |
|---|---|---|
| `customers.csv` | 10,000 | `customer_id` INT PK; name, email, country, signup_date, customer_segment (`Premium` / `Standard` / `Basic`), lifetime_value DECIMAL |
| `orders.csv` | 100,000 | `order_id` INT PK; `customer_id` FK; `product_id` FK; order_date; quantity; unit_price; total_amount; order_status (`Pending` / `Completed` / `Cancelled`); payment_date nullable |
| `products.csv` | 500 | `product_id` INT PK; name, category, price, cost, stock_quantity, reorder_level |

Guide wording of “10,000 / 100,000 / 500 rows” conflicts with extra duplicate rows. Frozen generation contract: **10,010 customer rows**, **100,020 order rows**, **500 product rows**. See section 6.6.

Intentional quality issues to inject (mandatory listed counts):

| Issue | Count |
|---|---|
| Customers: NULL email | 50 |
| Customers: duplicate `customer_id` | 10 |
| Orders: NULL `customer_id` | 100 |
| Orders: NULL `product_id` | 200 |
| Orders: `customer_id` absent from customers | 50 |
| Orders: `product_id` absent from products | 30 |
| Orders: duplicate `order_id` | 20 |

### 2.2 Bronze

- Read CSVs into Databricks (S3/DBFS or equivalent documented local path for development).
- Create Bronze tables.
- Keep data raw/unchanged: no cleaning and no transformation that alters source values.
- Handle schema inference and types.
- Log ingestion metadata including row counts and timestamp.

### 2.3 Silver

Implement all five quality modules required by the repository structure:

1. Completeness — no NULLs in critical fields (`email`, `customer_id`, `product_id` at minimum).
2. Uniqueness — duplicate `order_id` and `customer_id` handling.
3. Type validation — explicit/valid types; malformed values handled.
4. Referential integrity — `customer_id` and `product_id` must exist in parent tables.
5. Business logic — documented rules for quantity, amount, date, status, and payment consistency.

Additional Silver rules:

- Never delete bad rows merely to make checks pass.
- Flag bad rows with `quality_check_result` (or a clearly documented equivalent).
- Generate quality metrics with pass/fail counts and percentages.

### 2.4 Gold (SQL)

| Artifact | Required grain / outputs |
|---|---|
| Sales by Product | `product_id`, `product_name`, `category`, `total_orders`, `total_revenue`, `avg_order_value` |
| Revenue by Customer | `customer_id`, `customer_name`, `customer_segment`, `total_orders`, `total_revenue`, `avg_order_value`, `lifetime_value_actual` |
| Daily / weekly trends | Time-series sales/revenue (repository structure requirement) |
| Customer Segmentation | `segment_type` in {High-Value, Repeat, One-Time, Inactive}; `customer_count`, `avg_revenue`, `total_revenue` |

### 2.5 Dashboard

Databricks SQL dashboard with at least three tiles:

- Top 10 products by revenue (bar)
- Customer revenue distribution (histogram)
- Customer segmentation (pie)

Filters must be configured. Exact filter dimensions are not fully specified in the guide (see clarifications).

### 2.6 Engineering / submission artifacts

README setup instructions, schema/setup and seed notes, meaningful tests, full prompt history, requirement/design/test artifacts, debugging and code-review notes, reflection, Cursor workflow artifacts, and a Git repository link via the required organizational process. Hosting/deployment is not required.

## 3. Non-functional requirements

- Use **PySpark** for distributed Bronze/Silver processing (project working rule; the condensed `DE_C1_REQUIREMENTS.md` states the medallion layers and repository layout, and this repo freezes PySpark as the Bronze/Silver engine).
- Use **SQL** for required Gold and dashboard queries.
- Bronze must remain raw/unchanged.
- Do not put real PII, credentials, secrets, passwords, tokens, or private production connection details into the project or prompts. Use synthetic data.
- Every major change must be derived from the spec, tested, reviewed, recorded in `ai-prompts/`, and committed.
- Never claim a test passed unless it was actually executed.
- Never fabricate AI prompt history or validation results.
- Never silently resolve contradictory requirements.
- Keep the implementation inside the assignment’s roughly **20–25 hour** core scope: batch full-refresh, no streaming, no SCD2, no extra DQ platforms.

## 4. Assumptions

These are human decisions where the guide is silent or incomplete. They will be revisited if later official clarification contradicts them.

1. **Repository root.** Files live at this project root (`DE C1 Project-Udayan Mahajan`), which fulfills the required `databricks-medallion-pipeline/` tree. `DE_C1_REQUIREMENTS.md` is kept as the working specification.
2. **Synthetic data only.** Names, emails, and addresses in generated files are fake.
3. **Mandatory defect counts are exact.** The listed issue counts (50 NULL emails, 10 duplicate customers, etc.) are the generation targets. The “approximately 700” figure is not used as a generation target (see section 8).
4. **Duplicate meaning.** “10 duplicate customer_id rows” means 10 extra customer rows that reuse an existing `customer_id` (10 IDs appear twice). Same interpretation for 20 duplicate `order_id` rows. Exact generation mechanics will be documented in `src/data_generation/DATA_GENERATION_NOTES.md` when data is generated.
5. **Overlapping defects.** A single row may fail more than one check. Metrics will count check failures and distinct failing rows separately. Defect injection will keep mandatory issue classes on **disjoint row sets** so listed counts remain independently verifiable, unless a note documents a forced overlap.
6. **Silver retains all rows.** Every Bronze row appears in Silver, with quality flags. Gold aggregations document which Silver rows they include (default: qualifying Completed orders that pass checks required for that metric; Inactive customers may have zero qualifying orders).
7. **Type validation** runs on declared types and malformed values. Extra malformed-type injection beyond the mandatory defect list is optional and must be documented before it is added. Default: do not invent extra type defects solely to make the module “find something.”
8. **Business-logic rules** are written in `data-quality-strategy.md` before Silver business-logic code is implemented.
9. **Future signup dates** (30 rows in an example prompt) are an optional documented business-logic defect, not a silent change to the mandatory counts (see clarifications).
10. **Development vs Databricks.** Code will be written for Databricks (Spark DataFrames, SQL). Local PySpark may be used for tests if documented; Unity Catalog / schema names will be parameterized rather than hardcoded to a private workspace.
11. **Critical completeness fields** named in the guide are `email`, `customer_id`, and `product_id`. Completeness also checks primary keys for NULL (`customers.customer_id`, `orders.order_id`, `products.product_id`) because a missing key is a completeness failure of a critical identifier. Other fields are not completeness-mandatory.
12. **Cancelled / pending payment_date.** `payment_date` is nullable. A missing payment_date on Completed orders is a business-logic issue, not a completeness failure of a named critical field.
13. **Money precision.** Unspecified `DECIMAL` is implemented as `DECIMAL(18,2)`.
14. **Revenue status filter.** Gold revenue and order-count measures use `order_status = 'Completed'` only. Pending and Cancelled orders are excluded from revenue. This is a business assumption, not stated in the guide.
15. **Batch full refresh.** Each successful run overwrites Bronze/Silver/Gold tables from the current CSVs. Incremental CDC is out of scope.
16. **CSV read mode.** Spark read uses an explicit schema and `PERMISSIVE` mode so malformed values become null rather than dropping rows (`DROPMALFORMED` would violate “never delete bad rows”).
17. **Tests live under `tests/`**, which is not in the required file tree but is required by “meaningful tests.” The directory will be added when tests are written, not during this design stage.
18. **As-of date** for “future signup” is a frozen date documented at generation time (planned default: `2026-08-31`, the date of this design review), not “whenever the job runs.”

## 5. Edge cases

- A row can fail multiple checks (NULL `customer_id` is **not** classified as an orphan; see section 6.7).
- Duplicate PKs in Bronze: uniqueness flags **all** rows that share a duplicated key; Bronze still contains every copy.
- Orphan FKs vs NULL FKs are different classes of failure.
- `total_amount` not equal to `quantity * unit_price` (decimal scale).
- Negative or zero quantity, price, or amount.
- `order_date` after `payment_date`, or `payment_date` on Cancelled orders.
- `signup_date` in the future relative to the documented as-of date.
- `order_date` before customer `signup_date`.
- `order_status` or `customer_segment` values outside the allowed set (type/domain vs business-logic).
- Schema inference treating dates as strings or decimals as doubles.
- Header-only or empty files (current placeholders; not valid generated input).
- Gold averages with zero orders (division by zero).
- Segmentation: a customer can match more than one naive definition; rules must be mutually exclusive (see `design-notes.md`).
- Duplicate parent keys amplifying Gold joins.
- NULL `customer_id` on orders: customer revenue cannot attribute the order; product sales still can if `product_id` is valid.
- Duplicate `order_id` copies counted twice in `SUM`/`COUNT` if uniqueness is ignored.
- Products have no mandatory injected defects; Silver must still run and mostly PASS.
- Spark `monotonically_increasing_id()` is unique per write, not stable across reruns.

## 6. Clarifications (requirement ambiguities)

The assignment contains contradictions and gaps. They are recorded here rather than hidden. Each item quotes or paraphrases the source, states interpretations, and records the implementation decision.

### 6.1 Approximately 700 problematic rows vs listed issues

**Source:** The guide calls this approximately 700 problematic rows / 0.7%, but lists seven issue counts.

**Ambiguity:** The listed issues sum as follows:

| Listed issue | Count |
|---|---|
| NULL emails | 50 |
| Duplicate customer_id rows | 10 |
| NULL order customer_id | 100 |
| NULL order product_id | 200 |
| Orphan customer_id | 50 |
| Orphan product_id | 30 |
| Duplicate order_id rows | 20 |
| **Sum of listed issues** | **460** |

460 ≠ 700. 0.7% of 100,000 orders is 700; 0.7% of 110,500 rows across all three files is about 774. The “700 / 0.7%” language is a rough magnitude, not a reproducible total of the listed defects.

**Possible interpretations:** (A) generate extra unspecified defects until 700 distinct bad rows exist; (B) treat 700 as the orders-table target and ignore listed counts; (C) generate only the listed counts and document the gap.

**Implementation decision:** Treat the individually listed counts as the mandatory generation contract. Do **not** invent extra defects to force a total of 700. Do **not** delete or rewrite source rows later to make a 700-row metric appear. Document actual injected counts and actual Silver failure counts when those stages exist. Record the 240-row gap as a known spec inconsistency, not as a defect in the pipeline.

**Why:** Listed counts are testable. “Approximately 700” is not. Padding would hide the inconsistency the assignment asks us to notice.

### 6.2 Four quality checks vs five Silver modules

**Source:** The narrative repeatedly says four quality checks. The required repository structure explicitly lists five Silver modules: completeness, uniqueness, type validation, referential integrity, and business logic. The Silver paragraph also asks for business-logic rules documented before implementation.

**Ambiguity:** Implement four modules, or five?

**Possible interpretations:** (A) omit business logic to match “four”; (B) implement five files; (C) fold business logic into type validation.

**Implementation decision:** Implement **all five** modules so the repository matches the explicit structure and exceeds the minimum “four checks” reading. Business-logic rules are specified in `data-quality-strategy.md` before that module is coded. The “four checks” phrase is treated as incomplete narrative, not as permission to omit business logic.

**Why:** The file tree is an acceptance artifact. Business logic is also stated in the Silver requirements paragraph.

### 6.3 Example prompt mentioning 30 future signup dates

**Source:** An example prompt mentions 30 future signup dates. That issue is **not** in the mandatory listed counts.

**Ambiguity:** Are future signup dates required source defects, a Silver rule with zero injected rows, or out of scope?

**Possible interpretations:** (A) inject 30 rows and count them toward 700; (B) skip entirely; (C) implement the Silver rule and decide injection at generation time.

**Implementation decision:** Treat future signup dates as an **optional, explicitly documented business-logic check**. **Stage 2 injected 30 rows** (`2026-09-01` through `2026-09-30`), documented in `DATA_GENERATION_NOTES.md`, disjoint from mandatory customer defects, and excluded from the order purchaser pool. They are not counted toward the mandatory 460. Silver still evaluates `signup_date` against the frozen as-of date `2026-08-31`.

**Why:** Mandatory counts are enumerated. Example-prompt-only defects must not be smuggled into the contract.

### 6.4 Dashboard filters

**Source:** “Also configure filters.” No dimensions are named.

**Ambiguity:** Which filters, on which tiles, with which widgets?

**Possible interpretations:** date only; full dimensional slicers; Databricks SQL parameters vs widget filters.

**Implementation decision (planned, not implemented):** At least **order date range** and **customer_segment**. Add **category** on product tiles if the dashboard dataset allows it without extra joins that change grain. Final filter list will be documented in `src/dashboard/DASHBOARD_GUIDE.md` when the dashboard is built.

**Why:** Segment and date are available from Gold customer/order grains and match the required tiles. Country is deferred unless Gold customer tables carry it (they do not, per the required Revenue-by-Customer columns).

### 6.5 Gold inclusion rules

**Source:** Gold column lists are specified. The guide does not say whether Gold uses all Silver rows or only passing rows, nor which `order_status` values count as sales.

**Ambiguity:** Include Failed rows? Include Pending/Cancelled? Count duplicate order copies?

**Possible interpretations:** (A) all rows; (B) `quality_check_result = PASS` only; (C) per-metric predicates.

**Implementation decision:** Silver stores all rows plus flags. Each Gold query states its filter in SQL comments. Default qualifying order:

- `order_status = 'Completed'`
- `quality_check_result = 'PASS'`

Duplicate keys cannot be PASS on uniqueness, so duplicate copies are excluded from Gold measures. Dimension joins use a canonical parent row (see `design-notes.md`). Inactive customers are customers with zero qualifying orders, not deleted customers.

**Why:** Revenue from cancelled/pending or from duplicate copies would be incorrect. Per-query comments keep the rule reviewable.

### 6.6 “10,000 rows” vs extra duplicate rows

**Source:** `customers.csv` is “10,000 rows” and also “10 duplicate customer_id rows.” Same pattern for orders (100,000 + 20 duplicate `order_id` rows).

**Ambiguity:** Are duplicates included in the headline counts or added on top?

**Possible interpretations:**

- (A) 10,000 total customer rows including 10 extras (9,990 unique IDs).
- (B) 10,000 unique customers plus 10 extra duplicate rows (10,010 rows, 10,000 unique IDs).
- (C) 10,000 rows among which 10 IDs appear twice (same as A).

**Implementation decision:** Interpretation **(B)**. Output files will have **10,010 customer rows** and **100,020 order rows**. Unique key counts remain 10,000 and 100,000. Notes at generation time must print both numbers.

**Why:** The duplicate issues are specified as extra defective rows to detect, not as a reduction of the unique-entity targets. Tests can assert unique IDs and total lines separately.

### 6.7 NULL foreign key vs orphan foreign key

**Source:** 100 NULL `customer_id`, 200 NULL `product_id`, 50 customer_id values absent from customers, 30 product_id values absent from products. Completeness: no NULLs in those fields. Referential integrity: IDs exist in parent tables.

**Ambiguity:** Is a NULL FK also a referential failure?

**Possible interpretations:** (A) NULL is both completeness and RI failure; (B) NULL is completeness only; RI applies only to non-null FKs; (C) NULL is RI only.

**Implementation decision:** **(B)**. Completeness flags NULL FKs. Referential integrity flags **non-null** FKs whose value is absent from the parent key set. Metrics for “50 orphans” must not include the 100 NULL `customer_id` rows.

**Why:** Mixing the classes would make the mandatory 50/30 orphan counts unverifiable and would treat missingness as the same error as a broken link.

### 6.8 Schema inference vs explicit types

**Source:** Bronze must “handle schema inference and types” and keep data raw/unchanged.

**Ambiguity:** Infer schema from CSV, or apply a declared schema? Inference can type dates as strings and decimals as doubles.

**Possible interpretations:** (A) `inferSchema=True` only; (B) explicit schema only; (C) infer, log, then apply explicit schema.

**Implementation decision:** **Explicit schema on read**, using the contract in `data-model.md`. Inference may be used only as a diagnostic log (print inferred vs declared) and must not drive the write schema. `PERMISSIVE` mode keeps unparsable values as null rather than dropping rows.

**Why:** Inference is nondeterministic across Spark versions and sample sizes. Explicit schema is the data contract. Diagnostic inference still “handles” inference without letting it mutate types.

### 6.9 Bronze immutability vs ingest technical columns

**Source:** Raw/unchanged data; no cleaning/transformation. Also: log ingestion metadata.

**Ambiguity:** May Bronze tables add lineage columns such as `_ingest_row_id`?

**Possible interpretations:** (A) source columns only; metadata only in a separate table; (B) add technical columns on Bronze; (C) add them only on Silver.

**Implementation decision:** Source columns on Bronze tables are an unchanged mapping of the CSV fields. A technical `_ingest_row_id BIGINT` is added on Bronze (and copied to Silver) as **ingest lineage**, not as a cleaned business field. File-level metadata (`row_count`, `ingested_at`, source path, `ingest_id`) lives in `bronze.ingest_metadata`. No source value is repaired.

**Why:** Duplicate PKs make `order_id` / `customer_id` unsafe join keys when combining Silver module outputs. A surrogate row id prevents quality-flag join amplification. This is the same class of metadata as the required ingest log, not a business transform.

### 6.10 `avg_order_value` and `lifetime_value_actual`

**Source:** Gold requires `avg_order_value` and `lifetime_value_actual` without formulas. Source customers already have `lifetime_value`.

**Ambiguity:** Is average `AVG(total_amount)` or `SUM / COUNT`? Is actual LTV the source column, a sum of orders, or a comparison?

**Possible interpretations:** several formula variants; using source `lifetime_value` as `lifetime_value_actual`.

**Implementation decision:**

- `total_orders` = `COUNT` of qualifying orders (not `SUM(quantity)`).
- `total_revenue` = `SUM(total_amount)` of qualifying orders.
- `avg_order_value` = `total_revenue / NULLIF(total_orders, 0)`.
- `lifetime_value_actual` = `SUM(total_amount)` of that customer’s qualifying orders. Source `lifetime_value` is a Bronze/Silver field and is **not** copied into `lifetime_value_actual`.

**Why:** Actuals must be derived from orders or the Gold metric is circular. `SUM/COUNT` matches “average order value” as used in sales reporting and avoids `AVG` of already-aggregated grains.

### 6.11 Customer segmentation overlap

**Source:** `segment_type`: High-Value / Repeat / One-Time / Inactive, plus counts and revenue. No definitions.

**Ambiguity:** High-Value customers are often also Repeat. Inactive could mean “no orders” or “no recent orders.”

**Possible interpretations:** overlapping labels; recency windows; percentile vs fixed threshold.

**Implementation decision:** Mutually exclusive buckets, applied in this order to every customer (canonical customer row):

1. **Inactive** — zero qualifying orders.
2. **High-Value** — qualifying revenue `>= 1000.00`.
3. **Repeat** — two or more qualifying orders and not High-Value.
4. **One-Time** — exactly one qualifying order and not High-Value.

The `1000.00` threshold is a design default. After data generation, if the High-Value bucket is empty or contains almost every active customer, the constant may be changed **only** with a documented note in Gold SQL and `data-model.md`. Recency windows are out of scope.

**Why:** The dashboard pie requires a single segment per customer. A fixed threshold is testable; a hidden percentile is harder to explain in SQL comments.

### 6.12 Uniqueness: which duplicate copies fail?

**Source:** “duplicate order_id and customer_id handling.”

**Ambiguity:** Flag every row that shares a duplicated key, or keep one canonical row as PASS?

**Possible interpretations:** (A) all copies FAIL uniqueness; (B) first copy PASS, extras FAIL; (C) all FAIL uniqueness but Gold picks min `_ingest_row_id` as dimension attributes.

**Implementation decision:** **(A)** for the uniqueness module: every row whose key appears more than once gets `uniqueness_pass = false`. Gold therefore excludes them from fact measures via `quality_check_result = PASS`. Dimension attributes for a duplicated customer_id, if ever needed for a failing row, are not used in default Gold facts.

**Why:** “Handling” duplicates is detection, not silent survivor selection in Silver. Survivor selection in Silver would look like deleting/ignoring copies.

### 6.13 Quality-check overwrite vs multiple failures

**Source:** Flag with `quality_check_result` (or equivalent). Five modules. Rows may fail more than one check.

**Ambiguity:** A single PASS/FAIL column would overwrite earlier module results if each module writes last-wins.

**Implementation decision:** Modules never overwrite each other’s results. Each module writes its own boolean and a list of rule codes. `create_silver_tables.py` unions `failed_checks` and sets `quality_check_result = 'FAIL'` if the combined list is non-empty, else `'PASS'`. See `data-quality-strategy.md`.

**Why:** Last-writer-wins would hide simultaneous failures and break metrics.

### 6.14 Paths, catalog names, and S3/DBFS

**Source:** Read CSVs from S3/DBFS into Databricks. No bucket, catalog, or schema names.

**Ambiguity:** Hardcode a workspace path, or configure it?

**Implementation decision:** All catalog, schema, and filesystem paths come from environment variables / a small `src/config.py` introduced at Bronze implementation (not in this design-only commit). Local default: repo `data/` directory. Databricks: a configured volume/DBFS/S3 prefix. No personal workspace URLs, tokens, or bucket names in source.

**Why:** Hard-coded paths are an environment-specific failure mode and a secret-leak risk.

### 6.15 Tests are required but absent from the file tree

**Source:** Completion needs “meaningful tests.” The required repository structure lists no `tests/` directory and no test files.

**Ambiguity:** Tests inside each `src` module, notebooks, or a separate tree?

**Implementation decision:** Add `tests/` at implementation time (pytest + local SparkSession where possible). Do not pretend tests exist now. Do not fold tests into production modules as un-runnable comments.

**Why:** The evaluation asks for tests; the tree omitted them. Creating empty test files in this design stage would look like fake coverage.

### 6.16 Daily/weekly trend columns

**Source:** Repository requires `src/gold/03_daily_weekly_trends.sql` without a column list.

**Ambiguity:** One table or two? Week start Sunday vs Monday? Which timezone?

**Implementation decision:** Two Gold objects:

- `gold.daily_trends`: `trend_date DATE`, `total_orders`, `total_revenue`, `avg_order_value`
- `gold.weekly_trends`: `week_start_date DATE` (Monday, Spark `date_trunc('WEEK', ...)` documented in SQL), `total_orders`, `total_revenue`, `avg_order_value`

Dates are civil dates from `order_date` with no timezone conversion. Qualifying-order filter matches other Gold facts.

**Why:** Minimum measures that match Sales-by-Product; Monday weeks are a single documented convention.

### 6.17 Histogram binning

**Source:** Customer revenue distribution (histogram). No bin size.

**Ambiguity:** Databricks visualization binning vs pre-binned SQL.

**Implementation decision:** Dashboard query returns **one row per customer** with `lifetime_value_actual` (or equivalent revenue). Binning is left to the Databricks SQL visualization. If the visualization cannot histogram a raw measure, pre-bin in SQL with documented width and record that in `DASHBOARD_GUIDE.md`.

**Why:** Avoid inventing a bin contract before the dashboard exists.

### 6.18 Country and product category domains

**Source:** `country STRING`, `category STRING` with no allowed lists.

**Ambiguity:** Free text vs enum; invalid country as type vs business failure.

**Implementation decision:** Generation will use a small documented set of synthetic countries and categories. Type validation does **not** fail unknown country/category values unless they are null/malformed types. Domain enums that **do** fail type/domain checks are only `customer_segment` and `order_status` (named in the spec).

**Why:** The spec names those two enumerations. Inventing a world-country allowlist would create false positives.

## 7. Missing requirements (guide is silent)

These are not secretly filled in code later; they are designed in `design-notes.md` / `data-quality-strategy.md`.

| Gap | Why it matters | Where decided |
|---|---|---|
| No test directory or framework | “Meaningful tests” cannot be traced to a required path | Section 6.15 |
| No config/path strategy | S3/DBFS/local will otherwise be hard-coded | Section 6.14 |
| No rerun/idempotency rule | Second run could duplicate facts | `design-notes.md` |
| No error handling for missing files | Empty Bronze tables could look like success | `design-notes.md` |
| No logging beyond ingest metadata | Failures would be invisible | `design-notes.md` |
| No Gold status filter | Revenue would include cancelled orders | Section 6.5 / 6.10 |
| No segmentation definitions | Pie chart would be arbitrary | Section 6.11 |
| No DECIMAL precision | Spark inference may use DOUBLE | Assumption 13 |
| No as-of date for future signups | “Future” depends on job time | Assumption 18 |
| No Spark read mode | `DROPMALFORMED` would delete rows | Assumption 16 |
| No uniqueness survivor rule | Gold double-count risk | Section 6.12 |
| No multi-failure representation | Flags overwrite each other | Section 6.13 |
| No NULL-vs-orphan rule | Orphan metrics would be wrong | Section 6.7 |
| No orchestration (job vs notebook vs `*_all.py`) | `ingest_all.py` exists as a stub only | `design-notes.md`: Python orchestrators allowed |
| No Spark/Python version pin | Local vs Databricks drift | Pin at implementation in README when known |
| Submission account/email process | Organizational Git URL not in repo | `candidate-info.md` remains incomplete on purpose |
| Dashboard warehouse / screenshot rules | Guide vs this repo | No fake screenshots |

## 8. Conflicting requirements

| Conflict | Resolution (documented, not silent) |
|---|---|
| ~700 rows vs listed 460 | Listed counts win; gap remains visible |
| Four checks vs five modules | Five modules |
| 30 future signup dates in an example prompt only | Optional BL defect; not mandatory |
| “10,000 rows” vs extra duplicates | Unique targets + extra duplicate rows (6.6) |
| Schema inference vs typed contract | Explicit schema; inference diagnostic only |
| Raw Bronze vs ingest metadata columns | Source columns raw; `_ingest_row_id` is lineage |
| Tests required vs tests absent from tree | Add `tests/` later |
| `lifetime_value` source column vs `lifetime_value_actual` | Actuals from orders |

## 9. Environment dependencies

- Databricks workspace (Spark, SQL warehouse, dashboard product).
- Ability to read CSVs from DBFS, S3, a UC volume, or local `data/` in dev.
- PySpark compatible with the workspace Spark version; local tests need a Spark install or Databricks Connect.
- Python 3.x for `generate_sample_data.py`.
- Git + the organizational account/email process (not stored in this repo).
- No assumption that Unity Catalog names, cluster IDs, or warehouse IDs in this workspace exist elsewhere.

If Databricks is unavailable during a later coding stage, local PySpark tests may still validate transforms; the dashboard cannot be claimed complete without the SQL warehouse product.

## 10. Risks (requirements that can cause incorrect implementation)

| Risk | Incorrect implementation | Mitigation |
|---|---|---|
| Treating 700 as a generation target | Extra fake defects, unverifiable counts | Section 6.1 |
| Omitting business logic | Missing required Silver file | Section 6.2 |
| Counting NULL FKs as orphans | RI fail_count ≠ 50/30 | Section 6.7 |
| `DROPMALFORMED` / dropping FAIL rows | Bronze/Silver row loss | Assumption 16; never delete |
| Joining on duplicated PKs | Fan-out, inflated revenue | `_ingest_row_id`; uniqueness filter |
| Using source `lifetime_value` as actual | Gold ignores orders | Section 6.10 |
| Overlapping injected defects | Cannot assert 50 NULL emails independently | Assumption 5 |
| Unseeded Faker/random | Regenerating data changes all tests | Frozen seed at generation |
| Hard-coded `/Workspace/...` paths | Pipeline only runs on one laptop/workspace | Config |
| Gold without status filter | Cancelled revenue | Section 6.5 |
| Last-writer quality flag | Hidden multi-failures | Section 6.13 |
| Pandas `collect()` of 100k+ rows | Driver OOM, violates distributed processing | PySpark rules |
| Enforcing SQL UNIQUE/FK on Bronze | Ingest would reject required defects | Bronze DDL unconstrained |

## 11. Acceptance criteria

A later complete submission should include:

- [ ] End-to-end working pipeline (Bronze -> Silver -> Gold -> dashboard queries)
- [x] Intentional quality issues present in source data at the listed counts
- [x] All five Silver quality modules implemented
- [x] Completeness, uniqueness, type validation, referential integrity, and business logic implemented locally
- [x] Completeness/uniqueness/type/RI/business-logic flag bad rows without deleting them; Bronze source columns unchanged in tests
- [x] Combined `quality_check_result` / Silver tables / `silver.quality_metrics` table (local parquet; Databricks not written)
- [x] Quality metrics with pass/fail counts and percentages
- [ ] Gold aggregations A–C plus daily/weekly trends SQL
- [ ] Dashboard with 3+ required tiles and filters
- [ ] Schema, setup, and seed-data notes
- [ ] README setup instructions that match the actual project
- [x] Meaningful tests that were actually run
- [ ] Full prompt history in `ai-prompts/`
- [ ] Requirement, design, and data-quality artifacts
- [ ] Debugging / code-review notes and reflection
- [ ] Responsible AI: synthetic data only; no secrets in repo or prompts
- [ ] Meaningful Git history (not a single dump of finished work)

**Current status of these checkboxes:** source CSVs contain the listed quality issues (Stage 2). Bronze ingest code exists; local parquet ingest tests passed; Databricks Bronze is still not run. All five Silver quality modules and combined Silver tables are implemented and locally tested. Gold and dashboard stay unchecked.

## 12. Requirements traceability matrix

Status values used below:

| Status | Meaning |
|---|---|
| PASS | A real artifact exists and fulfills this requirement as written |
| DESIGNED | Written contract/decision exists; no runtime evidence |
| PARTIAL | Required path or stub exists; the requirement is not fulfilled |
| NOT STARTED | No implementation and no completed evidence |

Do not read DESIGNED or PARTIAL as PASS.

| Requirement | Implementation Artifact | Validation/Test Evidence | AI Prompt Evidence | Status |
|---|---|---|---|---|
| Medallion flow CSV/S3/DBFS → Bronze → Silver → Gold → Dashboard | `README.md`, `design-notes.md`, `cursor-workflow/spec.md` | None (pipeline not run) | `ai-prompts/documentation.md` Prompts 1–3 | DESIGNED |
| Source dataset `customers.csv` (10k unique + documented extras; required columns) | `data/customers.csv`; `src/data_generation/generate_sample_data.py` | Generator validation + `tests/test_generate_sample_data.py` (14 tests OK) | `ai-prompts/data-generation.md` Prompt 1 | PASS |
| Source dataset `orders.csv` (100k unique + documented extras; required columns) | `data/orders.csv`; generator | Same | `ai-prompts/data-generation.md` | PASS |
| Source dataset `products.csv` (500 rows; required columns) | `data/products.csv`; generator | Same | `ai-prompts/data-generation.md` | PASS |
| Intentional DQ: 50 NULL emails | Generator | Counted 50 on generating run and tests | `ai-prompts/data-generation.md` | PASS |
| Intentional DQ: 10 duplicate `customer_id` rows | Generator | 10 extra rows; 20 uniqueness-fail rows | `ai-prompts/data-generation.md` | PASS |
| Intentional DQ: 100 NULL order `customer_id` | Generator | Counted 100 | `ai-prompts/data-generation.md` | PASS |
| Intentional DQ: 200 NULL order `product_id` | Generator | Counted 200 | `ai-prompts/data-generation.md` | PASS |
| Intentional DQ: 50 orphan `customer_id` | Generator | Counted 50; NULL ≠ orphan | `ai-prompts/data-generation.md` | PASS |
| Intentional DQ: 30 orphan `product_id` | Generator | Counted 30 | `ai-prompts/data-generation.md` | PASS |
| Intentional DQ: 20 duplicate `order_id` rows | Generator | 20 extra rows; 40 uniqueness-fail rows | `ai-prompts/data-generation.md` | PASS |
| Record ~700 vs listed-count discrepancy | `requirements-analysis.md` §§6.1, 14; `cursor-workflow/project-context.md` | Document review only (arithmetic 50+10+100+200+50+30+20=460) | `ai-prompts/documentation.md` Prompts 1–3 | PASS (documentation of the ambiguity) |
| Optional 30 future signup dates (example prompt only) | `DATA_GENERATION_NOTES.md`; generator | Counted 30; not in the 460 | `ai-prompts/data-generation.md` | PASS (optional injection, documented) |
| Bronze: read CSVs into Databricks | `src/bronze/01_ingest_customers.py`, `02_ingest_orders.py`, `03_ingest_products.py`, `ingest_all.py`, `ingest_core.py` | Contract tests run; Spark ingest tests **skipped** (no PySpark) | `ai-prompts/bronze-layer.md` Prompt 1 | PARTIAL (code complete; Databricks/Spark runtime BLOCKED) |
| Bronze: create Bronze tables | `bronze.customers/orders/products` via overwrite `saveAsTable`; DDL in `database/schema.sql` | Tables **not** created in a workspace from this environment | `ai-prompts/bronze-layer.md` | PARTIAL |
| Bronze: raw/unchanged; no cleaning | `ingest_core.py`; AST tests forbid drop/dedupe; PERMISSIVE options | No Spark except-all vs CSV in this environment | `ai-prompts/bronze-layer.md` | PARTIAL |
| Bronze: schema / types | Explicit contract in `src/bronze/contracts.py` / `data-model.md` | Contract tests assert field names and DECIMAL(18,2); Spark schema test skipped | `ai-prompts/bronze-layer.md` | PARTIAL |
| Ingestion metadata (row counts, timestamp) | `bronze.ingest_metadata` written by `ingest_core.py` | Spark metadata tests skipped; column contract tested | `ai-prompts/bronze-layer.md` | PARTIAL |
| Silver completeness module | `src/silver/01_quality_completeness.py`; `src/silver/quality_common.py` | Local Spark: 50 NULL emails, 100 NULL order customer_ids, 200 NULL order product_ids; rows retained | `ai-prompts/silver-layer.md` Prompt 1 | PARTIAL (transforms implemented and tested locally; Databricks Silver not written) |
| Silver uniqueness module | `src/silver/02_quality_uniqueness.py`; `src/silver/quality_common.py` | Local Spark: 20 customer and 40 order uniqueness-fail physical rows; all copies flagged | `ai-prompts/silver-layer.md` Prompt 1 | PARTIAL (same: Databricks Silver not written) |
| Silver type validation module | `src/silver/03_quality_type_validation.py`; `src/silver/quality_common.py` | Local Spark: 0 type failures on seed-42 committed data; malformed INT/DATE/DECIMAL and domain fixtures fail without deleting rows; completeness-owned NULLs are not type failures | `ai-prompts/silver-layer.md` Prompt 2 | PARTIAL |
| Silver referential integrity module | `src/silver/04_quality_referential_integrity.py`; `src/silver/quality_common.py` | Local Spark: 50 customer orphans, 30 product orphans; 100/200 NULL FKs not classified as orphans; no join fan-out | `ai-prompts/silver-layer.md` Prompt 2 | PARTIAL |
| Silver business logic module | `src/silver/05_quality_business_logic.py`; rules in `data-quality-strategy.md` | Local Spark: 30 future signups fail `signup_not_future`; other frozen BL rules 0 on seed-42; `order_not_before_signup` 0; fixtures cover valid/invalid/boundary/NULL-deferral | `ai-prompts/silver-layer.md` Prompt 3 | PARTIAL (local Spark; Databricks not written) |
| Never delete bad rows | `data-quality-strategy.md`; coding rules; all five transforms + orchestrator | Silver Spark tests: counts remain 10010 / 100020 / 500 after all five checks and combine | `ai-prompts/silver-layer.md` Prompts 1–3 | PARTIAL (proven locally; Databricks not run) |
| Flag rows (`quality_check_result` or equivalent) | Per-module `*_pass` / `*_failed_checks`; combiner `failed_checks` + `quality_check_result` | Tests assert accumulation across five modules; combined PASS/FAIL written | `ai-prompts/silver-layer.md` Prompts 1–3 | PARTIAL |
| Quality reporting (pass/fail counts and percentages) | `CheckMetrics`; `silver.quality_metrics` (local parquet overwrite) | Completeness/uniqueness/type/RI/BL/table-outcome metrics asserted; expected vs observed on seed-42 | `ai-prompts/silver-layer.md` Prompts 1–3 | PARTIAL |
| Gold: Sales by Product | `src/gold/01_sales_by_product.sql`; `create_gold_tables.py` stub | SQL placeholder; not executed | `ai-prompts/gold-layer.md` empty of impl prompts | PARTIAL |
| Gold: Revenue by Customer | `src/gold/02_revenue_by_customer.sql` | Placeholder; not executed | `ai-prompts/gold-layer.md` | PARTIAL |
| Gold: Customer Segmentation | `src/gold/04_customer_segmentation.sql`; rules in `design-notes.md` §segmentation | Placeholder; not executed | `ai-prompts/gold-layer.md`; `ai-prompts/documentation.md` | PARTIAL |
| Daily/weekly trends SQL | `src/gold/03_daily_weekly_trends.sql`; columns in `data-model.md` | Placeholder; not executed | `ai-prompts/documentation.md` (column decision) | PARTIAL |
| Dashboard: 3+ tiles (bar, histogram, pie) | `src/dashboard/dashboard_queries.sql`; `DASHBOARD_GUIDE.md` | Not built; no workspace dashboard | `ai-prompts/dashboard.md` empty of impl prompts | PARTIAL |
| Dashboard filters | Planned in §6.4 and `DASHBOARD_GUIDE.md` | Not configured | `ai-prompts/documentation.md` | DESIGNED |
| Schema / setup | `database/schema.sql`, `database/setup-notes.md` | Schema not applied to a warehouse | `ai-prompts/documentation.md` | DESIGNED |
| Seed-data notes | `database/seed-data-notes.md` | Records generator command, counts, synthetic confirmation; Bronze not loaded | `ai-prompts/data-generation.md` | PASS for generation notes; Bronze seed still pending |
| Tests (“meaningful tests”) | `tests/test_generate_sample_data.py`; `tests/test_bronze_contract.py`; `tests/test_bronze_ingest.py`; `tests/test_silver_contract.py`; `tests/test_silver_quality.py` | Combined relevant set **147/147 OK** (generator 14, Bronze 58, Silver contract 20, Silver Spark 55); 0 skipped | `ai-prompts/data-generation.md`; `ai-prompts/bronze-layer.md`; `ai-prompts/silver-layer.md` | PARTIAL (local Spark passed; Databricks not run) |
| README setup instructions | `README.md` | Generation, Bronze, and Silver completeness/uniqueness/type/RI commands documented; Databricks ingest not run | `ai-prompts/data-generation.md`; `ai-prompts/bronze-layer.md`; `ai-prompts/silver-layer.md` | PARTIAL (local verified; Databricks setup not) |
| Prompt history format (prompt, response, accept/change/reject, validation, decision) | `ai-prompts/*.md` | Init/design, Stage 2 data-generation, Stage 3 bronze-layer, Stage 4 Silver completeness/uniqueness, type/RI, and business-logic/orchestration | `ai-prompts/documentation.md`; `ai-prompts/data-generation.md`; `ai-prompts/bronze-layer.md`; `ai-prompts/silver-layer.md` | PARTIAL (Gold/dashboard logs still empty) |
| Cursor workflow artifacts | `cursor-workflow/project-context.md`, `spec.md`, `cursor-rules-or-instructions.md`, `task-breakdown.md` | Files exist and were reviewed this stage | `ai-prompts/documentation.md` Prompt 3 | PASS (artifacts exist and are current for this stage) |
| Debugging notes | `debugging-notes.md` | Placeholder; no runtime defects | `ai-prompts/debugging.md` empty | PARTIAL |
| Reflection | `reflection.md` | Explicitly not filled with fabricated experience | `ai-prompts/documentation.md` | PARTIAL |
| Final AI usage summary | `final-ai-usage-summary.md` | Initialization + this design stage only | `ai-prompts/documentation.md` | PARTIAL |
| Responsible AI (no real PII/secrets in repo or prompts) | Policy in README, rules, `.gitignore`; `candidate-info.md` omits emails/tokens | `.gitignore` reviewed; no `.env`; CSVs use `@example.com` synthetic names | `ai-prompts/data-generation.md` | PASS for Stage 2 contents (re-check after later stages) |
| Git / submission (meaningful history; org account process; repo link) | Git repo; commit `16ee902`; this stage’s commit when created | `git log` exists; **no submission URL**; not a complete history yet | `ai-prompts/documentation.md` | PARTIAL |
| Working rule: spec → test → review → prompt log → commit | `tool-workflow.md`, `cursor-workflow/spec.md` §definition of done | Followed for init; followed for this docs stage (no code tests because no code) | `ai-prompts/documentation.md` | PARTIAL (process in use; not done for implementation stages) |
| Required repository file tree | Paths listed in `DE_C1_REQUIREMENTS.md` | Tree verified at init (`16ee902`) and re-checked this stage | `ai-prompts/documentation.md` Prompts 2–3 | PASS (paths exist; bodies remain stubs where required) |
| Quality of AI use (context, spec, refinement, validation, reject notes) | `cursor-workflow/*`, this file, `ai-prompts/documentation.md` | Design review performed; no fabricated test results | `ai-prompts/documentation.md` | PARTIAL (design-stage evidence only) |

## 13. Explicit treatment — approximately 700 problematic rows

**What the document says:** approximately 700 problematic rows / 0.7%.

**What the document lists:** 50 + 10 + 100 + 200 + 50 + 30 + 20 = **460** issue instances.

**Why they do not match:**

- Listed issues may count *issue instances*, not distinct rows (duplicates and multi-field failures differ).
- 0.7% of the orders table equals 700, which looks like a rounded marketing figure rather than a sum.
- The example prompt’s 30 future signup dates still yield 490, not 700.

**Decision:** Do not hide the discrepancy. Do not pad source data to 700. Generate the listed mandatory issues. Report both “listed issue instances” and “distinct flagged rows” in Silver metrics. If a reviewer expects 700, point them to this section.

## 14. Explicit treatment — four vs five quality checks

**What the narrative says:** four quality checks.

**What the required tree says:** five modules under `src/silver/`.

**Named in prose:** completeness, uniqueness, referential integrity, and type validation appear as the core four; business logic is also stated under Silver requirements (“sensible rules such as quantity/amount/date/status/payment consistency, documented before implementation”).

**Decision:** Implement all five files:

- `01_quality_completeness.py`
- `02_quality_uniqueness.py`
- `03_quality_type_validation.py`
- `04_quality_referential_integrity.py`
- `05_quality_business_logic.py`

This satisfies the explicit repository structure and the Silver paragraph that already mentions business logic. The “four checks” wording is recorded as inconsistent documentation, not as a reason to drop a required module.

## 15. Out of scope for the requirements/design stage (historical)

That stage did not implement generation or the pipeline. **Stage 2 later completed data generation.** **Stage 3 completed Bronze ingest code** (local parquet tests passed; Databricks still not run). **Stage 4 completed all five Silver quality modules and Silver table orchestration** (local Spark validated). Still out of scope until requested:

- Gold / Dashboard implementation
- Fabricated runtime results, reflection, or debugging stories
