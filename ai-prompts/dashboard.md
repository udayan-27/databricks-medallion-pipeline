# Dashboard prompts

Evaluator scan format: PROMPT SENT / AI RESPONSE SUMMARY / ACCEPTED / CHANGED / REJECTED / VALIDATION / FINAL DECISION / COMMIT / ARTIFACT. Historical FINAL DECISION wording that the Databricks UI was not yet rendered was true at that prompt’s time. Current status is in `tool-workflow.md` and `FINAL_AUDIT.md`. Cursor created dashboard SQL and later export automation. The human created and published the visual dashboard in the Databricks UI.

## P013 — Stage 6 Dashboard implementation

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

### COMMIT / ARTIFACT

`2a470fc` — `feat: add Databricks SQL dashboard queries and guide`. Files: `src/dashboard/dashboard_queries.sql`, `src/dashboard/DASHBOARD_GUIDE.md`, dashboard tests.

---

## P016 — 2026-08-31 — Databricks workflow validates dashboard SQL, not visual tiles

### PROMPT SENT

Same interaction as `ai-prompts/documentation.md` Prompt 7 / P016. Validate `src/dashboard/dashboard_queries.sql` against Gold. Do not claim visual dashboard rendering is automated unless a supported mechanism exists.

### AI RESPONSE SUMMARY

`validate_dashboard_sql` loads the existing SQL file, substitutes `{gold_schema}`, and runs the five named queries (Top 10, histogram population, segmentation, category filter values, customer_segment filter). Category filter is applied before `LIMIT 10`. Visual bar/histogram/pie creation remains a Databricks UI action documented in `DASHBOARD_GUIDE.md`. Dashboard SQL was not rewritten.

### ACCEPTED

SQL validation in the repository workflow; UI rendering stays manual.

### CHANGED

Guide/README wording so the official process is `run_pipeline.py` plus the existing click-path for tiles.

### REJECTED

Claiming Lakeview/SQL dashboard widgets are created by the Python workflow; fabricating a Databricks UI run.

### VALIDATION

P016 sequential suite **223/223 OK**, including existing dashboard contract/Spark tests.

### FINAL DECISION

Dashboard SQL remains Gold-only. Visual rendering is still not automated and has not been executed.

### COMMIT / ARTIFACT

`063854b` — `feat: automate Databricks environment and pipeline validation`. Dashboard SQL unchanged. Visual tiles were not created by Cursor. Duplicate log: `ai-prompts/documentation.md` P016.

---

## P017 — 2026-08-31 — Closeout: dashboard SQL PASS + published UI (manual)

### PROMPT SENT

Same interaction as `ai-prompts/documentation.md` Prompt 8 / P017. Record Databricks dashboard SQL PASS and the published dashboard. Do not change dashboard SQL semantics. Do not modify the Databricks dashboard. Do not claim Cursor generated the visual tiles.

### AI RESPONSE SUMMARY

No `dashboard_queries.sql` change. Documented Databricks SQL: Top 10 = 10 rows; histogram population = 10,000; segmentation = four buckets; category filter before LIMIT 10; customer_segment slices histogram. Published title: **DE C1 E-Commerce Sales Dashboard** (bar, histogram, pie/donut, two filters tested and returned to All). Sharing: Anyone in my account can view. Visual rendering was completed manually in the Databricks UI.

### ACCEPTED

SQL validation is automated; tile rendering is a UI operation. Cursor did not generate the visual dashboard.

### CHANGED

Guide/README/FINAL_AUDIT current-status only.

### REJECTED

Editing dashboard SQL; calling account-scoped sharing public internet access; attributing the visual dashboard to Cursor.

### VALIDATION

Local closeout suite **223/223 OK**. Databricks dashboard SQL PASS and published-dashboard facts are the candidate’s workspace/UI result.

### FINAL DECISION

Dashboard SQL remains Gold-only and unchanged. The published dashboard is a manual UI artifact.

### COMMIT / ARTIFACT

`ead99e1` — `docs: close out Databricks validation and dashboard`. Dashboard SQL unchanged. Visual tiles: human Databricks UI, not Cursor.

---

## P018 — 2026-08-31 — Version-control the actual published dashboard definition

### PROMPT SENT

Version-control the actual existing Databricks dashboard definition. Do not create a fake `.lvdash.json`. Do not recreate the dashboard from SQL. Do not modify the published dashboard, Bronze, Silver, Gold, or dashboard SQL. Do not push.

Retrieve the real published **DE C1 E-Commerce Sales Dashboard** via Databricks CLI/API and store it as `dashboards/DE_C1_E-Commerce_Sales_Dashboard.lvdash.json`. Install the CLI if needed. Use an existing profile if valid. Browser OAuth if authentication is missing; never ask for a PAT. Locate with `lakeview list/get/get-published` (read-only). Export via `workspace export` if possible. Validate structure, Gold-only sources, three visualizations, two filters. Security-scan. Add `dashboards/README.md` and local `tests/test_dashboard_artifact.py`. Optional genuine screenshot only. Update prompt history and justified project docs. One commit: `feat: version-control Databricks dashboard definition`.

### AI RESPONSE SUMMARY

Cursor installed Databricks CLI v1.14.1 (`winget`). No profile existed on first inspection; Cursor stopped for browser OAuth. The user completed `databricks auth login` (profile `DE_C1`). Cursor then used that profile for read-only discovery and export. `databricks workspace export` wrote the real Lakeview file. It matches `serialized_dashboard` from `lakeview get`. Cursor did not create, update, publish, or delete the live dashboard. Dashboard SQL and medallion transforms were not changed. No screenshot was found. Two P013 guide assertions in `tests/test_dashboard_contract.py` were already failing after P017’s status rewrite; they were aligned to the documented manual-UI facts without loosening Gold/SQL contracts.

### ACCEPTED

- Export the existing published definition; do not fabricate JSON.
- Profile `DE_C1` for all CLI calls after the user finished OAuth.
- Read-only Lakeview + workspace export only.
- Document that Git holds the serialized definition, not every publish/sharing ACL.
- Local structural tests; no Databricks required for those tests.

### CHANGED

- CLI was not pre-installed; installed via winget.
- OAuth was a manual browser step by the user; Cursor did not perform account authorization.
- Workspace path in docs uses `/Users/<workspace-user>/...` rather than the personal folder name.
- Two stale `test_dashboard_contract` guide assertions (P013 “not created / not done”) updated to match P017+P018 documented status.

### REJECTED

- Hand-written or SQL-reconstructed `.lvdash.json`.
- Create/update/publish/delete Lakeview APIs.
- Editing Bronze/Silver/Gold or `dashboard_queries.sql`.
- Asking for a PAT or committing `.databrickscfg`.
- Inventing a screenshot.
- Pushing to GitHub.

### VALIDATION

Identity: `databricks auth profiles` showed `DE_C1` VALID; `databricks current-user me --profile DE_C1` succeeded.

Export: `databricks workspace export` of the Lakeview object; file equals `serialized_dashboard` from `lakeview get`. JSON parsed. Widgets: bar / histogram / pie with the three required titles; `filter-single-select` on `category` and `customer_segment`; datasets query `workspace.gold.*` only. No PAT/OAuth/password/key/local-path matches in the artifact.

Spark-free: `python -m unittest tests.test_dashboard_artifact tests.test_dashboard_contract -v` → **Ran 24 tests in 0.009s OK**.

Complete sequential suite (venv Python 3.11.9 + Temurin 17; one process):

`python -m unittest tests.test_generate_sample_data tests.test_bronze_contract tests.test_bronze_ingest tests.test_silver_contract tests.test_silver_quality tests.test_gold_contract tests.test_gold_aggregations tests.test_dashboard_contract tests.test_dashboard_artifact tests.test_dashboard_queries tests.test_databricks_workflow -v`

→ **Ran 232 tests in 558.859s OK** (0 failed, 0 errors, 0 skipped).

### FINAL DECISION

Ship the exported Lakeview definition plus docs/tests. Do not treat Git as controlling warehouse binding or account-level sharing. Do not push.

### COMMIT / ARTIFACT

`59f7ebd` — `feat: version-control Databricks dashboard definition`. Files: `dashboards/DE_C1_E-Commerce_Sales_Dashboard.lvdash.json`, `dashboards/README.md`, `tests/test_dashboard_artifact.py`. OAuth was a manual browser step by the user.
