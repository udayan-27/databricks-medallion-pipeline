# Tool workflow

This project is developed in Cursor with Git history and written specifications as the source of truth.

## Intended workflow

1. Read `DE_C1_REQUIREMENTS.md` and `cursor-workflow/` context before changing code.
2. Compare the requested change with the specification.
3. State the implementation approach, assumptions, and edge cases.
4. Implement the smallest change that satisfies the spec.
5. Run relevant tests or validation. Record actual results. Never claim a test passed unless it was executed.
6. Update the matching documentation and `ai-prompts/*.md` file with the real interaction.
7. Create a meaningful Git commit.

## Tools in scope

| Tool | Role |
|---|---|
| Cursor | AI-assisted design, implementation, and documentation |
| Git | Meaningful commit history; no fabricated history |
| PySpark | Distributed Bronze and Silver processing |
| Spark SQL / Databricks SQL | Required Gold aggregations and dashboard queries |
| Databricks | Target runtime for tables, jobs, and the SQL dashboard (`run_pipeline.py` executed in the workspace; visual dashboard rendered manually in the UI) |
| unittest | Contract tests and local Spark integration tests |

## What is not allowed

- Copying AI output without understanding it
- Fabricating prompt history or validation results
- Silently resolving contradictory requirements
- Deleting bad source records only to make quality checks pass
- Putting real PII, credentials, secrets, passwords, tokens, or private production connection details into the repo or prompts
- Shallow one-line prompts as the only AI usage evidence
- Claiming local Spark/parquet results are Databricks / Delta / Unity Catalog results
- Claiming Cursor performed Databricks OAuth or created the visual dashboard tiles

## Prompt-history standard

For meaningful prompts, record prompt text or summary, AI response summary, what was accepted/changed/rejected and why, validation performed, and the final decision.

Index: `ai-prompts/prompt-index.md`.

## Responsible AI (actual practice)

- **Data is synthetic.** Names and emails are generated (`{first}.{last}{id}@example.com`). Stage 2 CSVs contain no real customer PII.
- **No real customer PII was supplied to Cursor.** Prompts referenced schemas, counts, and synthetic examples.
- **Credentials and secrets were not supplied.** There is no Databricks PAT, cloud key, or `.env` in git. `.gitignore` excludes `.env` and `.venv`.
- **Production connection details were not supplied.** Catalog, data path, and table format are runtime configuration (`MEDALLION_CATALOG`, `MEDALLION_DATA_PATH`, `MEDALLION_TABLE_FORMAT`).
- **Sensitive information would be excluded from future AI prompts.** Workspace URLs, tokens, and real PII stay out of `ai-prompts/` and source.

This section describes how prompts and repository contents were kept free of secrets and real PII. Databricks workspace execution evidence is recorded separately in `FINAL_AUDIT.md` and is not implied or denied by this responsible-AI section.

## Ownership (AI vs human)

Cursor/AI was used for requirement decomposition, architecture/design, data generation, pipeline implementation, testing, debugging, dashboard SQL, Databricks automation, dashboard-definition export automation, and documentation/auditing.

Human ownership included interpreting ambiguities, accepting or rejecting AI proposals, deciding quality-rule semantics, approving fixes, validating test results, performing Databricks account authentication (OAuth browser step), and performing the visual dashboard creation/publishing UI action.

Honest examples of AI ideas that were rejected or constrained (already documented; not invented for this file): padding defects to ~700, Faker/pandas/dbt, winutils, default catalog `main`, using source `lifetime_value` as `lifetime_value_actual`, last-writer quality flags, claiming Cursor rendered the dashboard.

## Current status (final)

Unambiguous final state of this repository:

- Requirements, design, seed-42 data, Bronze, all five Silver modules, Gold SQL, dashboard queries, local Spark tests, Databricks compatibility code review, and the repository-owned Databricks bootstrap/validation workflow (`src/databricks/`) are complete.
- The Databricks workflow **was actually executed** in the workspace: `python src/databricks/run_pipeline.py`.
- Bootstrap/source validation **PASS**.
- Bronze **PASS** in Databricks.
- Silver **PASS** in Databricks.
- Gold **PASS** in Databricks.
- Dashboard SQL validation **PASS**.
- The actual Databricks SQL dashboard **DE C1 E-Commerce Sales Dashboard** was created and published **manually** in the Databricks UI. Cursor did not generate the visual tiles.
- The real `.lvdash.json` was exported read-only and is version-controlled in `dashboards/`.
- Dashboard sharing is account-scoped (**Anyone in my account can view**). That is **not** public internet access.

Local Spark/parquet tests remain a separate evidence class from Databricks. Visual tile rendering remains a human UI operation.

## Historical evolution (do not treat as current status)

The following was true at an earlier project stage (after the Databricks automation code existed, before the workspace run and published dashboard):

> A Databricks cluster/SQL warehouse run of that workflow and a Databricks SQL dashboard UI have **not** been executed.

That statement is preserved here as chronology. It is **not** the current status. Workspace execution and the published dashboard were recorded later (P017 / P018).
