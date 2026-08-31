# Final AI usage summary

Status: complete for the work that actually happened in this repository. Databricks workspace jobs were executed with `python src/databricks/run_pipeline.py` (human-run in the workspace; Cursor recorded the results). The Databricks SQL dashboard UI was rendered **manually**. Cursor did not generate the visual tiles.

Cursor was the AI tool. Git, unittest, local PySpark, and the repository Databricks workflow were the validation tools. Technical decisions are owned by the candidate.

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
| Dashboard queries + guide | `ai-prompts/dashboard.md` / P013 | Gold-only tiles; UI later rendered manually |
| Databricks compatibility review | debugging Prompt 3 / documentation Prompt 5 / P014 | Code review; no Databricks execution in that turn |
| Public repository readiness audit | documentation Prompt 6 / P015 | Structure, security, prompt index, `FINAL_AUDIT.md` |
| Databricks workflow redesign | documentation Prompt 7 / bronze, silver, gold, dashboard, debugging / P016 | `src/databricks/` bootstrap + validate + `run_pipeline.py`; existing modules remain the transform source of truth; that turn did **not** execute Databricks |
| Repository closeout | documentation Prompt 8 / bronze, silver, gold, dashboard, debugging / P017 | Record actual workspace PASS results and the published dashboard; docs only; pipeline/Gold/dashboard SQL unchanged |

Index: `ai-prompts/prompt-index.md`.

The workspace command `python src/databricks/run_pipeline.py` was executed by the candidate in Databricks, not by Cursor in this closeout chat. Visual bar/histogram/pie widgets were created in the Databricks SQL UI. Those are not AI actions.

## Quality of AI use (what this repository actually did)

Strong patterns that were used:

- Persistent project context (`cursor-workflow/`)
- Detailed specifications before implementation
- Specific, staged prompts rather than “build the lakehouse”
- Multiple refinement cycles (Bronze Windows, Silver BL review, Gold test harness, dashboard join alias, Databricks workflow)
- Validation before acceptance (unittest; SHA-256; recorded workspace counts)
- Explicit reject notes (pad to 700, winutils, Faker, pandas, dbt, source LTV as actual, last-writer quality flags, claiming Cursor rendered the dashboard)
- Meaningful Git history (one descriptive commit per stage)

Weak patterns that were avoided:

- Vague one-line prompts as the only evidence
- Copying AI output without tests
- Fabricated prompt history or pass/fail numbers
- Silently resolving the 700-row and four-vs-five contradictions
- Treating earlier exploratory notebook cells as Cursor history

## Responsible AI (actual practice)

- Source data is synthetic. Names and emails are generated (`{first}.{last}{id}@example.com`). No real customer PII was supplied to Cursor.
- No Databricks personal access tokens, cloud keys, `.env` files, or production connection strings were placed in prompts or in git.
- Catalog, data path, and table format are runtime configuration (`MEDALLION_CATALOG`, `MEDALLION_DATA_PATH`, `MEDALLION_TABLE_FORMAT`).
- Dashboard sharing is documented as **Anyone in my account can view**, not as public internet access.

See `tool-workflow.md`.

## What remains outside AI-usage claims

Local Spark/parquet tests are not Databricks. The `src/databricks/` workflow **was** executed in a workspace; those PASS counts are Databricks evidence. Visual dashboard rendering remains a human UI operation. Remaining submission steps are git push (when asked) and the organizational account/email process.
