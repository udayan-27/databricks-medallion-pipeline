# Requirements analysis

Canonical source: `DE_C1_REQUIREMENTS.md`. This document records how those requirements are interpreted and which human decisions resolve ambiguities. It does not claim that the pipeline has been implemented or validated.

## 1. Problem statement

Build a complete Databricks Medallion Architecture pipeline for synthetic e-commerce sales data:

`CSV / S3 / DBFS -> Bronze -> Silver -> Gold -> Dashboard`

The pipeline must ingest three source files (`customers.csv`, `orders.csv`, `products.csv`), preserve raw Bronze data, apply documented Silver data-quality checks without deleting bad rows, produce Gold business aggregations in SQL, and support a Databricks SQL dashboard.

The exercise also requires engineering evidence: requirement analysis, architecture, AI-assisted implementation with recorded prompts, testing, validation of AI output, debugging notes, documentation, responsible AI usage, human ownership of decisions, and meaningful Git history.

## 2. Functional requirements

### 2.1 Source data (mandatory when generated)

| Dataset | Target rows | Keys / notable fields |
|---|---|---|
| `customers.csv` | 10,000 | `customer_id` INT PK; name, email, country, signup_date, customer_segment (`Premium` / `Standard` / `Basic`), lifetime_value DECIMAL |
| `orders.csv` | 100,000 | `order_id` INT PK; `customer_id` FK; `product_id` FK; order_date; quantity; unit_price; total_amount; order_status (`Pending` / `Completed` / `Cancelled`); payment_date nullable |
| `products.csv` | 500 | `product_id` INT PK; name, category, price, cost, stock_quantity, reorder_level |

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

README setup instructions, schema/setup and seed notes, meaningful tests, full prompt history, requirement/design/test artifacts, debugging and code-review notes, reflection, and a Git repository link via the required organizational process. Hosting/deployment is not required.

## 3. Non-functional requirements

- Use **PySpark** for distributed Bronze/Silver processing.
- Use **SQL** for required Gold and dashboard queries.
- Bronze must remain raw/unchanged.
- Do not put real PII, credentials, secrets, passwords, tokens, or private production connection details into the project or prompts. Use synthetic data.
- Every major change must be derived from the spec, tested, reviewed, recorded in `ai-prompts/`, and committed.
- Never claim a test passed unless it was actually executed.
- Never fabricate AI prompt history or validation results.
- Never silently resolve contradictory requirements.

## 4. Assumptions

These are human decisions where the guide is silent or incomplete. They will be revisited if later official clarification contradicts them.

1. **Repository root.** Files live at this project root (`DE C1 Project-Udayan Mahajan`), which fulfills the required `databricks-medallion-pipeline/` tree. `DE_C1_REQUIREMENTS.md` is kept as the working specification.
2. **Synthetic data only.** Names, emails, and addresses in generated files are fake.
3. **Mandatory defect counts are exact.** The listed issue counts (50 NULL emails, 10 duplicate customers, etc.) are the generation targets. The “approximately 700” figure is not used as a generation target (see section 8).
4. **Duplicate meaning.** “10 duplicate customer_id rows” means 10 extra customer rows that reuse an existing `customer_id` (10 IDs appear twice). Same interpretation for 20 duplicate `order_id` rows. Exact generation mechanics will be documented in `src/data_generation/DATA_GENERATION_NOTES.md` when data is generated.
5. **Overlapping defects.** A single row may fail more than one check. Metrics will count check failures and distinct failing rows separately. Defect injection will avoid accidental extra overlaps unless a note documents them.
6. **Silver retains all rows.** Every Bronze row appears in Silver, with quality flags. Gold aggregations will document which Silver rows they include (planned default: rows that pass checks required for that metric; Inactive customers may have zero qualifying orders).
7. **Type validation** runs on declared types and malformed values. Extra malformed-type injection beyond the mandatory defect list is optional and must be documented before it is added. Default: do not invent extra type defects solely to make the module “find something.”
8. **Business-logic rules** will be written in `data-quality-strategy.md` before Silver business-logic code is implemented.
9. **Future signup dates** (30 rows in an example prompt) are an optional documented business-logic defect, not a silent change to the mandatory counts (see clarifications).
10. **Development vs Databricks.** Code will be written for Databricks (Spark DataFrames, SQL). Local PySpark may be used for tests if documented; Unity Catalog / schema names will be parameterized rather than hardcoded to a private workspace.
11. **Critical completeness fields** named in the guide are `email`, `customer_id`, and `product_id`. Additional completeness checks (for example `order_id`, `product_name`) may be added if documented; they are not assumed mandatory.
12. **Cancelled / pending payment_date.** `payment_date` is nullable. A missing payment_date on Completed orders is a business-logic issue, not a completeness failure of a named critical field.

## 5. Edge cases

- A row can fail multiple checks (NULL `customer_id` is also a referential-integrity failure depending on how NULL FK is classified).
- Duplicate PKs in Bronze: uniqueness flags all copies or all-but-one; Bronze still contains every copy.
- Orphan FKs vs NULL FKs are different classes of failure.
- `total_amount` not equal to `quantity * unit_price`.
- Negative or zero quantity, price, or amount.
- `order_date` after `payment_date`, or `payment_date` on Cancelled orders.
- `signup_date` in the future relative to a documented “as-of” date.
- `order_date` before customer `signup_date`.
- `order_status` or `customer_segment` values outside the allowed set (type/domain vs business-logic).
- Schema inference treating dates as strings or decimals as doubles.
- Header-only or empty files during this initialization phase (not valid production input).
- Gold averages with zero orders (division by zero).
- Segmentation: a customer can arguably match more than one segment definition; rules must be mutually exclusive and documented before Gold SQL is written.

## 6. Clarifications (requirement ambiguities)

The assignment contains contradictions. They are recorded here rather than hidden.

### 6.1 Approximately 700 problematic rows vs listed issues

The guide says approximately **700 problematic rows / 0.7%**, but the individually listed issues sum as follows:

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

460 ≠ 700. 0.7% of 100,000 orders is 700; 0.7% of 110,500 rows across all three files is about 774. The “700 / 0.7%” language is therefore a rough magnitude, not a reproducible total of the listed defects.

**Implementation decision:** Treat the individually listed counts as the mandatory generation contract. Do **not** invent extra defects to force a total of 700. Do **not** delete or rewrite source rows later to make a 700-row metric appear. Document actual injected counts and actual Silver failure counts when those stages exist. Record the 240-row gap as a known spec inconsistency, not as a defect in the pipeline.

### 6.2 Four quality checks vs five Silver modules

The narrative repeatedly says **four** quality checks. The required repository structure explicitly lists **five** Silver modules: completeness, uniqueness, type validation, referential integrity, and business logic.

**Implementation decision:** Implement **all five** modules so the repository matches the explicit structure and exceeds the minimum “four checks” reading. Business-logic rules will be specified in writing before that module is coded. The “four checks” phrase is treated as incomplete narrative, not as permission to omit business logic.

### 6.3 Example prompt mentioning 30 future signup dates

An example prompt mentions **30 future signup dates**. That issue is **not** in the mandatory listed counts.

**Implementation decision:** Treat future signup dates as an **optional, explicitly documented business-logic check**. If injected, they will be 30 rows, documented in data-generation notes, and evaluated by the business-logic module. They will not be used to silently pad the mandatory 460-count list toward 700. Whether to inject them will be decided at data-generation time and recorded then. Until then, the check is in scope for Silver business logic as a rule (signup_date must not be in the future relative to a documented as-of date), even if zero such rows exist.

### 6.4 Dashboard filters

The guide requires filters but does not name them.

**Implementation decision (planned, not implemented):** At least date range and one of country / customer_segment / category. Final filter list will be documented in `src/dashboard/DASHBOARD_GUIDE.md` when the dashboard is built.

### 6.5 Gold inclusion rules

The guide does not say whether Gold uses all Silver rows or only passing rows.

**Implementation decision (planned):** Silver stores all rows plus flags. Each Gold query will state its filter (default: exclude rows that failed checks that would invalidate that metric, e.g. orphan product_id excluded from sales-by-product). This will be written into the Gold SQL comments and `data-model.md` when implemented.

## 7. Acceptance criteria

A later complete submission should include:

- [ ] End-to-end working pipeline (Bronze -> Silver -> Gold -> dashboard queries)
- [ ] Intentional quality issues present in source data at the listed counts
- [ ] All five Silver quality modules implemented
- [ ] Bad rows flagged, not deleted; Bronze unchanged
- [ ] Quality metrics with pass/fail counts and percentages
- [ ] Gold aggregations A–C plus daily/weekly trends SQL
- [ ] Dashboard with 3+ required tiles and filters
- [ ] Schema, setup, and seed-data notes
- [ ] README setup instructions that match the actual project
- [ ] Meaningful tests that were actually run
- [ ] Full prompt history in `ai-prompts/`
- [ ] Requirement, design, and data-quality artifacts
- [ ] Debugging / code-review notes and reflection
- [ ] Responsible AI: synthetic data only; no secrets in repo or prompts
- [ ] Meaningful Git history (not a single dump of finished work)

**Current status of these checkboxes:** none of the pipeline/data/test items are complete. This initialization commit only satisfies the repository-structure and requirements-analysis starting point.

## 8. Explicit treatment — approximately 700 problematic rows

**What the document says:** approximately 700 problematic rows / 0.7%.

**What the document lists:** 50 + 10 + 100 + 200 + 50 + 30 + 20 = **460** issue instances.

**Why they do not match:**

- Listed issues may count *issue instances*, not distinct rows (duplicates and multi-field failures differ).
- 0.7% of the orders table equals 700, which looks like a rounded marketing figure rather than a sum.
- The example prompt’s 30 future signup dates still yield 490, not 700.

**Decision:** Do not hide the discrepancy. Do not pad source data to 700. Generate the listed mandatory issues. Report both “listed issue instances” and “distinct flagged rows” in Silver metrics. If a reviewer expects 700, point them to this section.

## 9. Explicit treatment — four vs five quality checks

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

## 10. Out of scope for this commit

- Data generation
- Pipeline implementation
- Tests
- Dashboard construction
- Any claimed validation results
