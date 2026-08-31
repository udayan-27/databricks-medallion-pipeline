# Dashboard prompts

## Prompt 1 — Stage 6 Dashboard implementation

### PROMPT SENT

User requested Stage 6 Dashboard of the DE C1 exercise after Gold QA closed (148 + 27 local tests OK; Stage 2 CSV SHA-256 unchanged; Gold production SQL unchanged). Instructions: read the frozen spec and Gold SQL; implement Databricks SQL dashboard queries with at least three tiles (Top 10 products bar, customer revenue histogram, customer segmentation pie) based on Gold only; document Gold source / business question / dimensions / measures / ordering / interpretation / filters; do not invent unsupported fields; do not recalculate Gold logic; prefer Databricks viz histogram bins over SQL buckets; keep High-Value / Repeat / One-Time / Inactive exclusive; design practical Gold-field filters; complete `DASHBOARD_GUIDE.md` including local vs Databricks limitations; do not claim Databricks UI rendering unless executed; add tests and run them; adversarial BI review; update `ai-prompts/dashboard.md` and related docs; commit `feat: add Databricks SQL dashboard queries and guide`; do not start final audit or push.

### AI RESPONSE SUMMARY

Implemented Gold-only SQL in `src/dashboard/dashboard_queries.sql` (three tiles + two filter-value queries), a workspace guide that distinguishes local PySpark/parquet from Databricks SQL/Delta/UC, Spark-free contract tests, and Spark tests against the Gold fixture plus Gold-shaped edge tables. Category filter is a pre-LIMIT query parameter. Histogram population is the full Gold customer set including zeros. Segmentation SQL selects Gold counts only.

### ACCEPTED

- Gold sources: `sales_by_product`, `revenue_by_customer`, `customer_segmentation`.
- Top 10 via `ORDER BY total_revenue DESC, product_id ASC LIMIT 10` (deterministic ties, at most 10 rows).
- Histogram: one row per canonical customer, `lifetime_value_actual`, viz-side bins (requirements-analysis §6.17).
- Pie: `segment_type` + `customer_count` from Gold; no CASE / 1000.00 reimplementation.
- Filters: `category` (Tile 1, before LIMIT), `customer_segment` (Tile 2).
- Honest Databricks UI status: not rendered.

### CHANGED

- Histogram keeps zero-revenue / Inactive customers so the population matches the Gold contract (a spike at 0.00 is expected).
- `{gold_schema}` placeholder so local tests can isolate schemas; Databricks Editor substitutes `gold` or `<catalog>.gold`.
- Supporting `filter_values_*` queries for dropdowns (not extra tiles).
- Histogram Spark assertion aliases join columns after an `AMBIGUOUS_REFERENCE` on `lifetime_value_actual`.

### REJECTED

- Date-range filter on the three required tiles — those Gold tables have no `order_date`; using `daily_trends` would be a different tile; re-aggregating Silver would duplicate Gold.
- `country` filter — not a Gold revenue-by-customer column.
- `segment_type` filter on the pie — would hide the four exclusive buckets the tile exists to show.
- `customer_segment` on the pie — different grain/field; joining would rebuild Gold.
- SQL `WIDTH_BUCKET` / CASE histogram bins — conflicts with the frozen Gold/dashboard contract.
- `RANK()` / `ROW_NUMBER` Top-N — can return more than 10 on ties; assignment requires at most 10 rows.
- Dashboard column widget on Tile 1 category after `LIMIT 10` — would subset the global Top 10 instead of recomputing Top 10 inside the category.
- Querying Bronze or repeating Completed + PASS eligibility in dashboard SQL.
- Claiming Databricks UI validation (not executed).
- Regenerating Stage 2 CSVs, redesigning Gold, or starting final audit.

### VALIDATION

Spark-free: `python -m unittest tests.test_dashboard_contract -v` → **15 tests OK**.

Spark (this increment, after JAVA_HOME set): first `tests.test_dashboard_queries` run **12 OK, 1 ERROR** (`AMBIGUOUS_REFERENCE` on joined `lifetime_value_actual`; test defect). After aliasing columns:

- `tests.test_dashboard_queries -v` → **Ran 13 tests in 70.184s OK**
- `tests.test_dashboard_contract tests.test_dashboard_queries -v` → **Ran 28 tests in 69.924s OK**

Regression (this cycle, sequential, one process at a time):

- generator/Bronze/Silver: **Ran 148 tests in 390.485s OK**
- Gold: **Ran 27 tests in 120.584s OK**
- Combined relevant: **Ran 203 tests in 548.539s OK** (0 failed, 0 errors, 0 skipped)

Databricks SQL Dashboard UI: **not run**. Stage 2 CSV SHA-256 unchanged. Gold production SQL/orchestrator unchanged. No tests weakened.

### FINAL DECISION

Ship Gold-only dashboard SQL + guide + local tests. Do not treat local parquet `spark.sql` as Databricks rendering. Keep Gold production SQL unchanged.
