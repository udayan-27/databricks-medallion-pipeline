# AI prompt index

Navigator for reviewers. Each ID is a **real** documented interaction. Nothing was invented to fill a sequence. Historical FINAL DECISION blocks that say Databricks was not yet executed were true at that prompt’s time; they are not rewritten. Current status: `tool-workflow.md` and `FINAL_AUDIT.md`.

How to read a prompt file: `PROMPT SENT` → `AI RESPONSE SUMMARY` → `ACCEPTED` → `CHANGED` → `REJECTED` → `VALIDATION` → `FINAL DECISION` → `COMMIT / ARTIFACT`. If a field was not separately recorded in the original interaction, the prompt file says so. Do not treat an older VALIDATION block as the latest test run.

Latest sequential **local** suite (P019 freeze): **Ran 232 tests in 531.708s OK**. P018: 232 tests in 558.859s. P017 closeout: 223 tests in 840.713s. Public-repo audit (P015): 203 tests. Databricks workspace PASS results are a separate evidence class.

## Cursor/AI vs human (Databricks UI)

| Actor | What actually happened |
|---|---|
| **Cursor/AI** | Created the repository automation and validation workflow (`src/databricks/`). Created dashboard SQL and the workspace guide. Assisted with dashboard-definition source-control / read-only export automation. Recorded the candidate’s workspace PASS counts; did not invent them. |
| **Human** | Databricks OAuth browser authorization. Actual visual dashboard creation/publishing UI operation. Workspace execution of `python src/databricks/run_pipeline.py` (P017 record). |

Do not imply that a manual action was performed by Cursor.

## Prompt type legend

| Type | Meaning |
|---|---|
| Implementation | Produced or changed pipeline / test / generator code |
| Validation / review | Compatibility audit, public-repo audit, closeout evidence recording |
| Debugging | Diagnosed a real runtime or test-harness defect |
| Documentation | Spec, structure, README, prompt index, reflection |
| Databricks automation | Repository-owned bootstrap / run / validate workflow (code, not a workspace run) |
| Dashboard export / source-control | Read-only export of the already-published Lakeview definition |

## Index

| Prompt ID | Type | Activity | Purpose | Primary files affected | Validation performed | Result / status | Commit |
|---|---|---|---|---|---|---|---|
| P001 | Documentation | Requirements analysis | Read `DE_C1_REQUIREMENTS.md`; name 460 vs ~700, four vs five, optional future signups | none (read-only turn) | File read only; no tests | Confirmation only | none |
| P002 | Documentation | Initialization | Create the required repository tree without implementing the pipeline | required paths, stubs, header-only CSVs, `.gitignore` | Required-path check vs `DE_C1_REQUIREMENTS.md`; git status | Structure created; pipeline not implemented | `16ee902` |
| P003 | Documentation | Architecture / data model / DQ strategy | Freeze ambiguities, medallion design, Silver rules, Cursor bootstrap | `requirements-analysis.md`, `design-notes.md`, `data-model.md`, `data-quality-strategy.md`, `cursor-workflow/` | Doc/repo inspection; 460 ≠ 700 arithmetic; stubs still unimplemented | Frozen design; no fabricated tests | `3fa1c57` |
| P004 | Implementation | Data generation and data validation | Deterministic seed-42 CSVs with listed defects only | `src/data_generation/generate_sample_data.py`, `data/*.csv`, `tests/test_generate_sample_data.py` | Generator built-in validation; `tests.test_generate_sample_data` 14 OK | Seed-42 CSVs; SHA-256 recorded | `f5f3acf` |
| P005 | Implementation | Bronze implementation | Raw CSV ingest, explicit schema, ingest metadata | `src/bronze/*`, `src/config.py`, Bronze tests | Contract tests OK; Spark ingest skipped (no PySpark/JDK yet); Databricks not executed **at that time** | Bronze code committed; local Spark later | `a581cbc` |
| P006 | Documentation | Environment / local Spark setup | Isolated Python 3.11 + JDK 17 + PySpark 3.5.6 | README / debugging notes; `.venv` not committed | Smoke test passed; ingest tests failed (Windows Hadoop) | Runtime installed; parquet ingest not yet complete | `b6352b3` |
| P007 | Debugging | Bronze debugging | Windows URI encoding and winutils-free parquet writes | `spark_input_path`, `NoWinutilsRawLocalFileSystem`, `src/spark_local.py` | Bronze contract 37 OK; ingest 21 OK; combined 58 OK | Local parquet Bronze PASS; Databricks not yet run **at that time** | `f809c13` |
| P008 | Implementation | Silver completeness / uniqueness | Flag NULL emails/FKs and all duplicate-key copies; do not delete | `01_quality_completeness.py`, `02_quality_uniqueness.py`, `quality_common.py` | Silver contract 8 OK; Silver Spark 16 OK; combined 96 OK | Flag-not-delete locally validated | `cfc71d5` |
| P009 | Implementation | Silver type / referential integrity | Domain/malformed types; NULL ≠ orphan; no RI fan-out | `03_quality_type_validation.py`, `04_quality_referential_integrity.py` | Combined 120 OK; 50/30 orphans; type fail 0 | Type + RI locally validated | `6925aca` |
| P010 | Implementation | Silver business logic / orchestration | Frozen BL rules, combiner, `silver.quality_metrics` | `05_quality_business_logic.py`, `create_silver_tables.py` | Combined 147 OK after one test-assertion fix | Silver complete locally | `99a5ad7` |
| P011 | Implementation | Gold | Completed + PASS aggregations; `lifetime_value_actual` from orders | `src/gold/*.sql`, `create_gold_tables.py`, Gold tests | Combined 174 OK after test-harness overwrite fixes | Gold SQL locally validated; dashboard not started **at that time** | `eb25da1` |
| P012 | Debugging | Gold QA / Spark isolation | Concurrent Spark `PermissionError` vs sequential isolation | `start_local_test_spark`; Gold SQL unchanged | Sequential 148 + 27 OK (175 including helper test) | Environment isolation; Gold SQL unchanged | `aa48ca2` |
| P013 | Implementation | Dashboard SQL | Three Gold-only tiles + filters; workspace guide | `dashboard_queries.sql`, `DASHBOARD_GUIDE.md`, dashboard tests | Combined 203 OK; UI **not** rendered **at that time** | Gold-only SQL shipped; visual UI later (human) | `2a470fc` |
| P014 | Validation / review | Databricks compatibility preparation | Code review that local Windows workarounds stay off the cluster path | Docs only (`README.md`, `database/setup-notes.md`, debugging notes) | No `src/` change; prior suite 203 OK not re-run | Compatible pending runtime parameters **at that time** | `eb9c61c` |
| P015 | Validation / review | Public repository audit | Structure, security, prompt index, reproducibility, `FINAL_AUDIT.md` | This index, `FINAL_AUDIT.md`, README/reflection closeout | Sequential **203 tests in 535.902s OK**; SHA-256 match | Docs/audit shipped; Databricks still unexecuted **at that time** | `bf7ee06` |
| P016 | Databricks automation | Databricks workflow redesign | Automate bootstrap, Git-folder→volume copy, pipeline run, and validation without duplicating Bronze/Silver/Gold | `src/databricks/`, `tests/test_databricks_workflow.py`, README / setup-notes | Sequential **223 tests in 534.573s OK**; local copy SHA-256 match | Workflow code shipped; Databricks **not** executed in that turn | `063854b` |
| P017 | Validation / review | Databricks closeout | Record actual workspace `run_pipeline.py` PASS results and the manually published dashboard; do not change pipeline/Gold/dashboard SQL | `FINAL_AUDIT.md`, README, setup-notes, prompt index, reflection | Local **223 tests in 840.713s OK**; SHA-256 unchanged; workspace PASS recorded from candidate run | Databricks pipeline + published dashboard complete | `ead99e1` |
| P018 | Dashboard export / source-control | Dashboard definition export | Export the real published Lakeview definition into Git; do not recreate it from SQL or modify the live dashboard | `dashboards/DE_C1_E-Commerce_Sales_Dashboard.lvdash.json`, `dashboards/README.md`, `tests/test_dashboard_artifact.py` | Spark-free 24 OK; sequential **232 tests in 558.859s OK**; CLI export matches `serialized_dashboard` | Definition version-controlled; live dashboard unchanged | `59f7ebd` |
| P019 | Documentation | Final evidence / ownership freeze | Remove stale current-status wording; make prompt history evaluator-scannable; complete candidate-info without extra personal data | `tool-workflow.md`, `ai-prompts/*`, `candidate-info.md`, `FINAL_AUDIT.md`, related docs | Sequential local suite **232 tests in 531.708s OK**; SHA-256 match; security scan | Documentation/evidence only; pipeline/data/dashboard logic unchanged | this commit (`docs: finalize submission evidence and ownership`) |

## Duplicate file pointers (same interaction, two logs)

| Interaction | Why two files |
|---|---|
| P007 | Implementation in `bronze-layer.md`; debugging cycle in `debugging.md` |
| P012 | Debugging diagnosis in `debugging.md`; Gold file notes that SQL was unchanged |
| P016 | Official Databricks process recorded in documentation; layer files record that existing transforms were reused, not rewritten |
| P017 | Closeout records the actual workspace run and the manually published dashboard; layer files record that transforms and dashboard SQL were not changed |
| P018 | Dashboard export recorded in `dashboard.md`; `documentation.md` records that the live dashboard was not modified |
| P019 | Documentation closeout in `documentation.md`; this index is the navigator |

## Lifecycle coverage

| Required activity | Indexed as |
|---|---|
| 1. Initialization | P002 |
| 2. Requirements analysis | P001, P003 |
| 3. Architecture | P003 |
| 4. Data model | P003 |
| 5. Data quality strategy | P003 |
| 6. Data generation | P004 |
| 7. Data validation | P004 |
| 8. Bronze implementation | P005 |
| 9. Bronze debugging | P006, P007 |
| 10. Silver completeness/uniqueness | P008 |
| 11. Silver type/RI | P009 |
| 12. Silver business logic/orchestration | P010 |
| 13. Gold | P011 |
| 14. Gold QA/debugging | P012 |
| 15. Dashboard SQL | P013 |
| 16. Databricks compatibility preparation | P014 |
| 17. Documentation | P002, P003, P015, P017, P019 |
| 18. Environment/debugging interactions | P006, P007, P012 |
| 19. Databricks workflow redesign (automation code) | P016 |
| 20. Databricks workspace execution + published dashboard (recorded, not fabricated) | P017 |
| 21. Version-controlled serialized dashboard definition | P018 |
| 22. Final evidence / ownership freeze | P019 |
