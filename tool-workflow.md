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

## Prompt-history standard

For meaningful prompts, record prompt text or summary, AI response summary, what was accepted/changed/rejected and why, validation performed, and the final decision.

Index: `ai-prompts/prompt-index.md`.

## Responsible AI (actual practice)

- **Data is synthetic.** Names and emails are generated (`{first}.{last}{id}@example.com`). Stage 2 CSVs contain no real customer PII.
- **No real customer PII was supplied to Cursor.** Prompts referenced schemas, counts, and synthetic examples.
- **Credentials and secrets were not supplied.** There is no Databricks PAT, cloud key, or `.env` in git. `.gitignore` excludes `.env` and `.venv`.
- **Production connection details were not supplied.** Catalog, data path, and table format are runtime configuration (`MEDALLION_CATALOG`, `MEDALLION_DATA_PATH`, `MEDALLION_TABLE_FORMAT`).
- **Sensitive information would be excluded from future AI prompts.** Workspace URLs, tokens, and real PII stay out of `ai-prompts/` and source.

This is a description of what this repository contains and how prompts were written. It is not a claim that a Databricks workspace was used.

## Current state

Requirements, design, seed-42 data, Bronze, all five Silver modules, Gold SQL, dashboard queries, local Spark tests, a Databricks compatibility **code review**, and a repository-owned Databricks **bootstrap/validation workflow** (`src/databricks/`) are done. A Databricks cluster/SQL warehouse run of that workflow and a Databricks SQL dashboard UI have **not** been executed.
