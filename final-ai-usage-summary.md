# Final AI usage summary

Status: complete for the work that actually happened in this repository. Databricks workspace jobs and a Databricks SQL dashboard UI have **not** been executed, so those are not summarized as successes.

Cursor was the AI tool. Git, unittest, and local PySpark were the validation tools. Technical decisions are owned by the candidate.

## What Cursor was used for

| Lifecycle | Prompt evidence | Result |
|---|---|---|
| Read requirements (no code) | `ai-prompts/documentation.md` Prompt 1 / `ai-prompts/prompt-index.md` P001 | Ambiguities named (460 vs ~700, four vs five, optional future signups) |
| Repository initialization | documentation Prompt 2 / P002 | Required tree, stubs, `.gitignore`, first commit |
| Requirements, architecture, data model, DQ strategy | documentation Prompt 3 / P003 | Frozen design; no fabricated tests |
| Data generation | `ai-prompts/data-generation.md` / P004 | Seed-42 CSVs, listed defects only |
| Bronze ingest | `ai-prompts/bronze-layer.md` Prompt 1 / P005 | PySpark ingest, config, contract tests |
| Local Spark environment | documentation Prompt 4 / P006 | Python 3.11 `.venv`, JDK 17, PySpark 3.5.6 |
| Bronze Windows debugging | bronze Prompt 2 + `ai-prompts/debugging.md` Prompt 1 / P007 | posix paths + local-only FileSystem adapter |
| Silver completeness/uniqueness | `ai-prompts/silver-layer.md` Prompt 1 / P008 | Flag, do not delete |
| Silver type/RI | silver Prompt 2 / P009 | NULL ≠ orphan; no join fan-out |
| Silver business logic + tables | silver Prompt 3 / P010 | Frozen rules; combined `quality_check_result` |
| Gold SQL aggregations | `ai-prompts/gold-layer.md` / P011 | Completed + PASS; `lifetime_value_actual` from orders |
| Gold QA / Spark isolation | debugging Prompt 2 / P012 | Sequential Spark tests; Gold SQL unchanged |
| Dashboard queries + guide | `ai-prompts/dashboard.md` / P013 | Gold-only tiles; Databricks UI not rendered |
| Databricks compatibility review | debugging Prompt 3 / documentation Prompt 5 / P014 | Code review; no Databricks execution |
| Public repository readiness audit | documentation Prompt 6 / P015 | Structure, security, prompt index, `FINAL_AUDIT.md` |
| Databricks workflow redesign | documentation Prompt 7 / bronze, silver, gold, dashboard, debugging / P016 | `src/databricks/` bootstrap + validate + `run_pipeline.py`; existing modules remain the transform source of truth; workspace **not** executed |

Index: `ai-prompts/prompt-index.md`.

## Quality of AI use (what this repository actually did)

Strong patterns that were used:

- Persistent project context (`cursor-workflow/`)
- Detailed specifications before implementation
- Specific, staged prompts rather than “build the lakehouse”
- Multiple refinement cycles (Bronze Windows, Silver BL review, Gold test harness, dashboard join alias)
- Validation before acceptance (unittest; SHA-256; recorded counts)
- Explicit reject notes (pad to 700, winutils, Faker, pandas, dbt, source LTV as actual, last-writer quality flags)
- Meaningful Git history (one descriptive commit per stage)

Weak patterns that were avoided:

- Vague one-line prompts as the only evidence
- Copying AI output without tests
- Fabricated prompt history or pass/fail numbers
- Silently resolving the 700-row and four-vs-five contradictions

## Responsible AI (actual practice)

- Source data is synthetic. Names and emails are generated (`{first}.{last}{id}@example.com`). No real customer PII was supplied to Cursor.
- No Databricks personal access tokens, cloud keys, `.env` files, or production connection strings were placed in prompts or in git.
- Catalog, data path, and table format are runtime configuration (`MEDALLION_CATALOG`, `MEDALLION_DATA_PATH`, `MEDALLION_TABLE_FORMAT`).
- Future prompts should continue to exclude secrets, workspace URLs, and real PII. This is a working rule, not a claim that Databricks was used.

See `tool-workflow.md`.

## What remains outside AI-usage claims

Local Spark/parquet tests are not Databricks, Delta, Unity Catalog, or Databricks SQL dashboard evidence. The `src/databricks/` workflow is the official supported Databricks process; it has not been executed in a workspace from this environment. Those items stay BLOCKED / NOT EXECUTED until a workspace run is requested.
