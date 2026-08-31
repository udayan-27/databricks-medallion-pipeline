# AI prompt index

Navigator for reviewers. Each ID is a **real** documented interaction. Nothing was invented to fill a sequence.

Format: prompt ID, lifecycle activity, evidence file, purpose, resulting artifacts, Git commit when that interaction produced a commit.

| ID | Lifecycle activity | Prompt file | Purpose | Resulting implementation / documentation | Git commit |
|---|---|---|---|---|---|
| P001 | Requirements analysis | [documentation.md](documentation.md) Prompt 1 | Read `DE_C1_REQUIREMENTS.md`; name 460 vs ~700, four vs five, optional future signups | Confirmation only; no files | none (read-only turn) |
| P002 | Initialization | [documentation.md](documentation.md) Prompt 2 | Create the required repository tree without implementing the pipeline | Required paths, stubs, header-only CSVs, `.gitignore` | `16ee902` |
| P003 | Architecture / data model / data-quality strategy | [documentation.md](documentation.md) Prompt 3 | Freeze ambiguities, medallion design, Silver rules, Cursor bootstrap | `requirements-analysis.md`, `design-notes.md`, `data-model.md`, `data-quality-strategy.md`, `cursor-workflow/` | `3fa1c57` |
| P004 | Data generation and data validation | [data-generation.md](data-generation.md) Prompt 1 | Deterministic seed-42 CSVs with listed defects only | `src/data_generation/generate_sample_data.py`, `data/*.csv`, `tests/test_generate_sample_data.py` | `f5f3acf` |
| P005 | Bronze implementation | [bronze-layer.md](bronze-layer.md) Prompt 1 | Raw CSV ingest, explicit schema, ingest metadata | `src/bronze/*`, `src/config.py`, Bronze tests | `a581cbc` |
| P006 | Environment / local Spark setup | [documentation.md](documentation.md) Prompt 4 | Isolated Python 3.11 + JDK 17 + PySpark 3.5.6 | README / debugging notes; `.venv` not committed | `b6352b3` |
| P007 | Bronze debugging | [bronze-layer.md](bronze-layer.md) Prompt 2; [debugging.md](debugging.md) Prompt 1 | Windows URI encoding and winutils-free parquet writes | `spark_input_path`, `NoWinutilsRawLocalFileSystem`, `src/spark_local.py` | `f809c13` |
| P008 | Silver completeness / uniqueness | [silver-layer.md](silver-layer.md) Prompt 1 | Flag NULL emails/FKs and all duplicate-key copies; do not delete | `01_quality_completeness.py`, `02_quality_uniqueness.py`, `quality_common.py` | `cfc71d5` |
| P009 | Silver type / referential integrity | [silver-layer.md](silver-layer.md) Prompt 2 | Domain/malformed types; NULL ≠ orphan; no RI fan-out | `03_quality_type_validation.py`, `04_quality_referential_integrity.py` | `6925aca` |
| P010 | Silver business logic / orchestration | [silver-layer.md](silver-layer.md) Prompt 3 | Frozen BL rules, combiner, `silver.quality_metrics` | `05_quality_business_logic.py`, `create_silver_tables.py` | `99a5ad7` |
| P011 | Gold | [gold-layer.md](gold-layer.md) Prompt 1 | Completed + PASS aggregations; `lifetime_value_actual` from orders | `src/gold/*.sql`, `create_gold_tables.py`, Gold tests | `eb25da1` |
| P012 | Gold QA / debugging | [debugging.md](debugging.md) Prompt 2; [gold-layer.md](gold-layer.md) Prompt 2 | Concurrent Spark `PermissionError` vs sequential isolation | `start_local_test_spark`; Gold SQL unchanged | `aa48ca2` |
| P013 | Dashboard | [dashboard.md](dashboard.md) Prompt 1 | Three Gold-only tiles + filters; workspace guide | `dashboard_queries.sql`, `DASHBOARD_GUIDE.md`, dashboard tests | `2a470fc` |
| P014 | Databricks compatibility preparation | [debugging.md](debugging.md) Prompt 3; [documentation.md](documentation.md) Prompt 5 | Code review that local Windows workarounds stay off the cluster path | Docs only (`README.md`, `database/setup-notes.md`, debugging notes) | `eb9c61c` |
| P015 | Documentation / public repository audit | [documentation.md](documentation.md) Prompt 6 | Structure, security, prompt index, reproducibility, `FINAL_AUDIT.md` | This index, `FINAL_AUDIT.md`, README/reflection closeout | `chore: complete public repository readiness audit` |
| P016 | Databricks workflow redesign | [documentation.md](documentation.md) Prompt 7; [bronze-layer.md](bronze-layer.md) Prompt 3; [silver-layer.md](silver-layer.md) Prompt 4; [gold-layer.md](gold-layer.md) Prompt 3; [dashboard.md](dashboard.md) Prompt 2; [debugging.md](debugging.md) Prompt 4 | Automate Databricks bootstrap, Git-folder→volume copy, pipeline run, and validation without duplicating Bronze/Silver/Gold | `src/databricks/`, `tests/test_databricks_workflow.py`, README / setup-notes | `feat: automate Databricks environment and pipeline validation` |

## How to read the prompt files

Every meaningful prompt file uses:

- **PROMPT SENT** — actual prompt or a labeled summary
- **AI RESPONSE SUMMARY** — what Cursor proposed
- **ACCEPTED / CHANGED / REJECTED** — human decisions
- **VALIDATION** — commands that were actually run
- **FINAL DECISION** — what shipped

Do not treat an older VALIDATION block as the latest test run. Latest sequential counts: **Ran 223 tests in 534.573s OK** (this Databricks-workflow increment, including `tests.test_databricks_workflow`). The public-repo audit recorded 203 tests in `FINAL_AUDIT.md`.

## Duplicate file pointers (same interaction, two logs)

| Interaction | Why two files |
|---|---|
| P007 | Implementation in `bronze-layer.md`; debugging cycle in `debugging.md` |
| P012 | Debugging diagnosis in `debugging.md`; Gold file notes that SQL was unchanged |
| P016 | Official Databricks process recorded in documentation; layer files record that existing transforms were reused, not rewritten |

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
| 15. Dashboard | P013 |
| 16. Databricks compatibility preparation | P014 |
| 17. Documentation | P002, P003, P015, P016 |
| 18. Environment/debugging interactions | P006, P007, P012 |
| 19. Databricks workflow redesign | P016 |
