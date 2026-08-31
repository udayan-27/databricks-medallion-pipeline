# Reflection

Written after local implementation, testing, Windows Spark debugging, the public-repository readiness audit, and the Databricks workflow redesign. Databricks workspace execution has not been done, so this does not invent a Databricks success or failure story. Earlier exploratory Databricks notebook work, if any, is not treated as Cursor history and is not the official submission path.

## What went well

Persistent written specifications (`cursor-workflow/`, `requirements-analysis.md`, `data-quality-strategy.md`) kept later stages from re-opening frozen decisions. The 460-vs-700 gap, five Silver modules, NULL-versus-orphan split, and Gold `Completed` + `PASS` rule stayed stable from design through Gold and the dashboard.

Stage discipline also worked. Each increment had a real prompt record, tests, and a descriptive Git commit. The history is requirements → design → data → Bronze → Silver → Gold → dashboard → compatibility, not a single dump of finished work.

Synthetic data generation was deterministic on the first seed-42 run. Built-in validation and 14 unit tests matched the listed defect counts without padding to 700.

## What was difficult

Local Windows Spark was harder than the pipeline logic. In-memory `createDataFrame` worked immediately. File-based parquet ingest failed for two independent Hadoop reasons: percent-encoded `file:` URIs from `Path.as_uri()`, and `mkdirs`/`listStatus` requiring winutils/`hadoop.dll`. Those were environment problems, not Bronze contract bugs.

A later Gold QA run showed that overlapping Spark unittest processes on Windows can fail during JVM gateway startup (`PermissionError` on a Py4J temp connection-info file). The operating rule is one Spark process at a time.

Keeping documentation honest was also work. Status lines drifted toward “Spark skipped” or “not yet implemented” after those stages had actually passed locally. The audit had to correct that without claiming Databricks results. The later Databricks workflow redesign had the same rule: automate schema/volume/source copy/pipeline/validation in git, and leave login, Git-folder connection, and visual dashboard rendering as the only unavoidable UI actions.

## Where AI helped

AI was most useful for:

- turning a frozen spec into modular PySpark/SQL that matched existing names and contracts
- scaffolding tests that assert injected defect counts rather than “zero failures”
- recording accept/change/reject notes in `ai-prompts/`
- keeping Gold logic in `.sql` files while the orchestrator only executes them
- drafting reviewer-facing documentation once the facts were known

Context-setting mattered more than clever one-line prompts. A new chat that read `cursor-workflow/project-context.md` first stayed inside the assignment. Vague prompts would have invited dbt, streaming, or padding defects to 700.

## Where AI was wrong or required correction

These are real corrections from this repository, not invented failures:

- A tautological generator validation check (`customer_id is None and False`) was removed in review; CSVs were not regenerated.
- FAILED ingest metadata always recorded `row_count = 0` even when a count already existed.
- Defaulting the catalog to `main` was rejected as environment-specific.
- The business-logic canonical-parent window originally partitioned on pre-alias column names.
- Contract tests matched documentation comments (`SUM(quantity)`, `F.sum`, `current_date()` in a docstring) instead of executable code.
- Gold tests held `spark.table()` DataFrames across parquet overwrite and tried to overwrite a table while still reading it.
- A Silver fixture test compared the sum of three business-rule failures to all-module FAIL rows.
- A dashboard test joined two Gold `lifetime_value_actual` columns without aliases (`AMBIGUOUS_REFERENCE`).
- Installing third-party winutils, dummy `winutils.exe`, skipping tests, or treating Windows Hadoop failures as Bronze logic bugs were rejected.

AI output was treated as a draft until it matched `DE_C1_REQUIREMENTS.md` and the frozen design.

## How AI output was validated

Validation was execution, not reading generated code:

- Generator: seed-42 run plus `tests.test_generate_sample_data` (physical rows, unique keys, NULLs, duplicates, orphans, SHA-256).
- Bronze/Silver/Gold/Dashboard: Spark-free contract tests plus local Spark parquet tests against fixtures and committed CSVs.
- Results were recorded as actual unittest counts. Skips were never called PASS. Databricks was never claimed from local parquet.

## Where human judgment was applied

- Listed defect counts (460 issue instances) win over “approximately 700.”
- Implement all five Silver modules even though the narrative says four.
- Inject 30 future signups as an optional, separately documented business-logic defect.
- NULL foreign keys are completeness failures, not orphans.
- Flag every duplicate-key copy; do not pick a Silver survivor.
- Gold facts use `Completed` + `PASS`; `lifetime_value_actual` is order revenue, not source `lifetime_value`.
- Stay inside batch PySpark + SQL. Reject dbt, Airflow, streaming, SCD2, and extra DQ platforms.
- Isolate Windows Spark workarounds so Databricks still uses the cluster session, Delta, and configured paths.

## Windows Spark debugging

The useful lesson was to split “Spark cannot start” from “the pipeline is wrong.” URI encoding, winutils, and Py4J temp-file races look like ingest failures if the stack is not read carefully. The accepted local fix was a posix path helper plus a tiny Java `RawLocalFileSystem` used only for locally created Windows sessions. Winutils was not installed and is not a Databricks dependency.

## Context-setting and persistent specifications

`cursor-workflow/` files were the difference between a continuation chat and a rewrite. Frozen decisions in `project-context.md` and `spec.md` stopped later prompts from “helpfully” padding data, deleting FAIL rows, or replacing Gold SQL with PySpark aggregations.

## Reusable prompting patterns

1. Point at the canonical spec and the matching `ai-prompts/<area>.md` file.
2. State what must not change (CSVs, Git history, Databricks claims, extra frameworks).
3. Require tests and actual result recording.
4. Record ACCEPTED / CHANGED / REJECTED with a reason tied to the spec.
5. Keep stages small enough that a reject note is cheaper than a rewrite.

Those patterns are why the prompt history is evidence rather than a diary of one-line prompts.
