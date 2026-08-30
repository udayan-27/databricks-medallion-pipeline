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
| Databricks | Target runtime for tables, jobs, and the SQL dashboard |

## What is not allowed

- Copying AI output without understanding it
- Fabricating prompt history or validation results
- Silently resolving contradictory requirements
- Deleting bad source records only to make quality checks pass
- Putting real PII, credentials, secrets, passwords, tokens, or private production connection details into the repo or prompts
- Shallow one-line prompts as the only AI usage evidence

## Prompt-history standard

For meaningful prompts, record prompt text or summary, AI response summary, what was accepted/changed/rejected and why, validation performed, and the final decision.

## Current state

This file describes the intended workflow. It does not claim that later pipeline stages have been executed.
