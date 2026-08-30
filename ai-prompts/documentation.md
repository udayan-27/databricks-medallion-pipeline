# Documentation prompts

Meaningful AI interactions for documentation and repository structure. Entries are real; they are not invented after the fact.

---

## Prompt 1 — 2026-08-31 — Read requirements (no implementation)

### Prompt text (summary)

Act as the implementation agent for the DE C1 AI Capability Exercise. Before any implementation, read `DE_C1_REQUIREMENTS.md` completely and treat it as canonical. Do not implement the pipeline yet. First understand the assignment, including requirement analysis through complete AI prompt history. Record the ~700-row inconsistency, four-vs-five quality-check inconsistency, and the example prompt’s future signup dates. Confirm: full requirements read; repository structure understood; ambiguities identified; AI workflow evidence is part of the submission.

### AI response summary

The assistant read the full requirements file and restated the medallion pipeline, source schemas, Bronze/Silver/Gold/dashboard rules, required tree, and working constraints (PySpark, SQL Gold, no PII, no deleting bad rows, Bronze raw). It listed the three ambiguities with a 460 vs 700 count table and stated it would not hide them. It confirmed AI prompt history is scored and that this confirmation turn should later be recorded. No files were created.

### Accepted and why

- No coding on that turn, matching the instruction.
- Ambiguities named rather than smoothed over.
- Five Silver modules treated as required.

### Changed and why

None; there was no implementation.

### Rejected and why

Not applicable (no code suggestions).

### Validation performed

File read of `DE_C1_REQUIREMENTS.md` only. No tests. No pipeline validation.

### Final decision

Hold implementation until the user requests the next stage.

---

## Prompt 2 — 2026-08-31 — Initialize project structure

### Prompt text (summary)

Initialize the complete repository structure required by `DE_C1_REQUIREMENTS.md`. Do not implement the pipeline. Do not populate final sample CSV data. Create all listed root files, `src/*`, `data/`, `database/`, `ai-prompts/`, and `cursor-workflow/`. Write `requirements-analysis.md` with problem statement through acceptance criteria plus explicit 700-row and four-vs-five treatment. Write Cursor context, spec, rules, and task breakdown. Create `.gitignore`. Update this file with the actual interaction. If Git is not initialized, initialize and commit:

`chore: initialize project structure and engineering spec`

Show the file tree, verify required files, Git status, first commit, and unresolved items. Do not start data generation.

### AI response summary

The assistant created the required tree at the project root (fulfilling `databricks-medallion-pipeline/` layout without nesting an extra folder). Documentation was written as planned/honest status, not as fake results. Python/SQL modules are stubs (`NotImplementedError` or “NOT IMPLEMENTED” comments). CSVs are header-only. `requirements-analysis.md` records the 460 vs 700 gap and the five-module decision. Git was initialized and the requested commit was made after the files existed.

### Accepted and why

- Full required path list, including five Silver stubs.
- Header-only CSVs instead of generated 10k/100k/500 rows.
- Ambiguities documented as implementation decisions, not hidden.
- Prompt history limited to this real documentation/init work.

### Changed and why

- Files placed at workspace root rather than a nested `databricks-medallion-pipeline/` directory, because this folder is already the project root. Recorded in `requirements-analysis.md` assumption 1 and `README.md`.

### Rejected and why

- Implementing ingest/quality/Gold SQL bodies — out of scope.
- Generating sample rows — explicitly forbidden.
- Inventing test results, debugging stories, or reflection content.
- Padding defect counts to 700.

### Validation performed

- Required paths checked against the list in `DE_C1_REQUIREMENTS.md` after creation.
- Git status and `git log` inspected after the initial commit.
- No PySpark jobs, no pytest, no CSV row-count validation (data not generated).

### Final decision

Stage 1 (structure + spec) is the Git baseline. Stage 2 (data generation) waits for an explicit user request.

---

## Prompt 3 — 2026-08-31 — Requirements/architecture review (no implementation)

### PROMPT SENT

Continue DE C1 from the existing repo. Do not recreate or reinitialize. Read `DE_C1_REQUIREMENTS.md`, `requirements-analysis.md`, Cursor workflow files, `data-model.md`, `design-notes.md`, and `data-quality-strategy.md`. Prior commit: `16ee902 chore: initialize project structure and engineering spec`. This is a requirements/design stage only. Do not implement pipeline code. Do not generate sample data.

Tasks requested:

1. Complete traceability matrix in `requirements-analysis.md` (Requirement, Implementation Artifact, Validation/Test Evidence, AI Prompt Evidence, Status). Include every requirement class (sources, injected issues, Bronze, metadata, five Silver modules, quality reporting, Gold, trends, dashboard, schema/setup, tests, README, prompt history, Cursor workflow, debugging, reflection, responsible AI, Git/submission). Do not mark PASS without real artifacts/evidence.
2. Challenge requirements: missing, ambiguous, conflicting, assumptions, edge cases, environment dependencies, risks, incorrect-implementation traps. Do not silently resolve. For each ambiguity: quote/paraphrase, explain, list interpretations, choose a practical decision, explain why, document it. Preserve the ~700 vs 460 discrepancy, four-vs-five Silver inconsistency, and 30 future signup dates appearing only in an example prompt.
3. Complete architecture in `design-notes.md` (CSV/S3/DBFS → Bronze → Silver → Gold → Databricks SQL Dashboard) covering flow, naming, schema, Bronze immutability, metadata, Silver sequencing, row-level status, multiple failures, metrics, Gold dependencies, aggregations, segmentation, dashboard, errors, rerun/idempotency, config/paths, logging, testing. Stay in ~20–25 hour scope.
4. Senior-engineer review: at least five failure modes with cause, prevention/detection, and proving test. Attention to join amplification, NULL semantics, FK validation, Bronze mutation, row deletion, aggregation double counting, quality-status handling, nondeterministic generation, hard-coded paths, environment assumptions.
5. Review `data-model.md` and `database/schema.sql` for all three sources; document columns, types, nullability, keys, meaning, domains, precision, dates; Bronze/Silver/Gold contracts; no unnecessary columns.
6. Finalize `data-quality-strategy.md` for completeness, uniqueness, type, RI, business logic (fields, detection, row-level, metrics, thresholds, intentional failures, tests, edge cases, false positives). Define multi-failure representation. Distinguish NULL FK, orphan FK, duplicate key, malformed value, business-rule violation. Do not delete bad rows.
7. Improve Cursor workflow files so a new Agent chat can continue without the old conversation.

Record real AI interactions under `ai-prompts/`. One git commit after validation: `docs: finalize requirements architecture and data quality design`. Show diff --stat and status first. Report files changed, decisions, ambiguities, risks, validation performed, commit hash.

### AI RESPONSE SUMMARY

The assistant read the listed files plus `database/schema.sql`, stubs, header-only CSVs, existing prompt logs, and git HEAD `16ee902`. It did not generate data or implement pipeline bodies.

Proposed documentation updates:

- Traceability matrix with honest statuses (PASS only for existing docs/tree/responsible-AI-so-far and the recorded 700-gap writeup; pipeline items PARTIAL/NOT STARTED/DESIGNED).
- Additional ambiguities beyond the original three (row counts vs extra duplicates, NULL vs orphan, schema inference vs explicit types, Gold filters/formulas, segmentation overlap, uniqueness all-copies, multi-failure overwrite, paths, missing `tests/` tree, trend columns, histogram bins, country/category domains).
- Architecture: full-refresh medallion, `_ingest_row_id`, PERMISSIVE reads, config/env paths, Gold Completed+PASS, segmentation priority, ten failure modes with tests.
- Data model and commented DDL for Bronze/Silver/Gold including ingest metadata and quality columns.
- Frozen five-module DQ strategy with failure-class dictionary and concatenated `failed_checks`.
- Cursor workflow files rewritten as bootstrap context for a new chat.

### ACCEPTED

- Design-only scope; no `generate_sample_data` run; no Spark/SQL implementation.
- Preserve 700 vs 460, four vs five, and optional 30 future signups.
- Listed defect counts as the generation contract; disjoint injection where possible.
- All five Silver modules; never delete bad rows.
- NULL FK vs orphan split; flag all duplicate copies; combiner on `_ingest_row_id`.
- Explicit schema + PERMISSIVE; `DECIMAL(18,2)`.
- Gold formulas and mutually exclusive segmentation with default High-Value threshold 1000.00 (change later only if documented).
- Honest matrix statuses; no fabricated tests.
- Single documentation commit when the user-requested validation is done.

### CHANGED

- Uniqueness **row** fail counts documented as 20 customers / 40 orders (all copies), while extra-row issue instances remain 10 / 20 — needed so Stage 2 tests do not assert the wrong number.
- `_ingest_row_id` added as Bronze lineage (not a source field) after weighing “no extra columns” against join-amplification risk; recorded as an explicit ambiguity decision rather than a silent extra attribute.
- `tests/` planned but **not created** in this stage (empty tests would look like fake coverage).
- `src/config.py` described but **not added** (Bronze-stage artifact).
- High-Value threshold frozen as a default with an allowed documented revisit after generation, because no real revenue distribution exists yet.
- Dashboard country filter deferred: Revenue-by-Customer Gold columns do not include country.

### REJECTED

- Generating CSVs or implementing ingest/quality/Gold SQL.
- Padding defects to 700 or omitting business logic.
- Marking pipeline/test/dashboard rows PASS.
- Filling `reflection.md` / `debugging-notes.md` with invented experience.
- Over-engineering (streaming, dbt, Great Expectations, SCD2, quarantine tables, RFC email validation, country ISO allowlists).
- Enforcing SQL PK/FK/NOT NULL on Bronze (would reject required defects).
- Using `DROPMALFORMED` or `current_date()` for the future-signup rule.
- Using source `lifetime_value` as `lifetime_value_actual`.
- Multiple tiny git commits.

### VALIDATION

Performed in this environment (documentation/repo inspection only):

- Read canonical requirements and all listed design/workflow files.
- Confirmed git HEAD `16ee902` and a clean tree before edits.
- Confirmed `data/*.csv` are header-only (one header line each).
- Confirmed Bronze/Silver/Gold/dashboard modules still stubs / NOT IMPLEMENTED.
- Confirmed required paths from `DE_C1_REQUIREMENTS.md` still present (no reinit).
- Arithmetic check: 50+10+100+200+50+30+20 = 460 ≠ 700.
- Reviewed `.gitignore` for `.env` / credential patterns; did not open or commit secrets.
- No PySpark, pytest, or Databricks jobs were run (nothing executable to validate).

### FINAL DECISION

Treat Stage 1.5 (requirements, architecture, data-model, DQ strategy, Cursor context) as complete written design. Keep Stage 2+ blocked until explicitly requested. Commit once: `docs: finalize requirements architecture and data quality design`.

---

## Prompt 4 — 2026-08-31 — Local Spark environment (Python 3.11 / JDK 17 / PySpark 3.5.6)

### PROMPT SENT

Prepare the local development environment for DE C1. Re-check the machine first. Install Python 3.11 (`winget` user scope) and Eclipse Temurin JDK 17 (`winget` user scope). Create project `.venv` with Python 3.11, upgrade pip, install only `pyspark==3.5.6`. Do not install delta-spark, pandas, pyarrow, Jupyter, standalone Spark, Hadoop/winutils, or PySpark into system Python 3.12. Do not change Python 3.12 or 3.7. Do not alter Git history or Bronze/CSV source. Set session `JAVA_HOME` to the actual JDK path. Run a local[2] Spark smoke test (`createDataFrame`, `count()`, `collect()`, `stop()`). Then `python -m unittest tests.test_bronze_ingest -v` and `python -m unittest tests.test_bronze_contract tests.test_bronze_ingest -v`. Optional local Bronze parquet run only if tests succeed. Document facts. Commit only if the repository actually changed. Do not start Silver.

### AI RESPONSE SUMMARY

Re-check matched the prior inspection: Windows 11, Python 3.12.7 system-wide, 3.11 missing, 3.7.9 present, no JDK, no PySpark, Git clean at Bronze HEAD `a581cbc`, `.gitignore` already ignores `.venv/`.

Installed Python 3.11.9 user-scope via the requested winget command. Temurin 17 `--scope user` failed (no user installer); installed `--scope machine` after approval. JDK path: `C:\Program Files\Eclipse Adoptium\jdk-17.0.20.101-hotspot` (17.0.20.1). Created `.venv` with `py -3.11`; pip upgraded; `pyspark==3.5.6` only (plus dependency `py4j==0.10.9.7`). System 3.12 still has no pyspark.

Smoke test passed. Contract tests 33/33 OK. Spark ingest tests ran (not skipped) and failed: Hadoop cannot read percent-encoded `file:` URIs from `Path.as_uri()`, and writes need winutils (not installed). Bronze source was not changed. Optional `ingest_all.py --table-format parquet` was not run. Silver was not started.

### ACCEPTED

- Isolated `.venv` on Python 3.11; PySpark not installed into 3.12.
- Session `JAVA_HOME` / `PATH` for the actual Temurin directory; no extra packages.
- Honest local vs Databricks distinction.
- No Bronze logic change to hide Windows Hadoop failures.
- No `.venv` commit; `.gitignore` already lists `.venv/`.

### CHANGED

- JDK install used `--scope machine` because winget has no user-scope Temurin 17 installer. The MSI also set machine `JAVA_HOME`.
- Optional local Bronze parquet job skipped after ingest tests failed.

### REJECTED

- Hadoop/winutils, delta-spark, pandas, pyarrow, Jupyter, standalone Spark.
- Editing `spark_input_path()` / ingest code just to make Windows tests green.
- Claiming Databricks, Delta, DBFS, or Unity Catalog validation from the smoke test.
- Starting Silver.

### VALIDATION

- `py -3.11 --version` → 3.11.9; `.venv` executable is `.venv\Scripts\python.exe`.
- `java -version` / `javac -version` → 17.0.20.1.
- `import pyspark` in `.venv` → 3.5.6; system 3.12 → `ModuleNotFoundError`.
- Smoke: `SMOKE_COUNT=2`, collect two rows, Spark stopped.
- Contract: `Ran 33 tests` OK.
- Ingest: `Ran 5 tests` FAILED (failures=1, errors=2); combined `Ran 38 tests` FAILED (failures=1, errors=2).
- `git check-ignore` confirms `.venv/` is ignored. No generated parquet/warehouse in git. Stage 2 CSVs unchanged.

### FINAL DECISION

Local Spark **runtime** is installed. Local parquet Bronze **ingest validation is not complete**. Databricks remains blocked until a workspace job is actually run. Stop after environment setup; do not start Silver.
