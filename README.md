# Databricks Medallion Pipeline — E-commerce Sales

This repository is the DE C1 AI Capability Exercise submission. It implements (when complete) a Databricks Medallion Architecture pipeline:

`CSV -> Bronze -> Silver -> Gold -> Dashboard`

The current commit initializes the required engineering structure, specifications, and stubs. **The pipeline is not implemented yet. Sample data has not been generated yet.**

Canonical requirements: [`DE_C1_REQUIREMENTS.md`](DE_C1_REQUIREMENTS.md).

## Status

| Area | Status |
|---|---|
| Repository structure | Initialized |
| Requirements analysis and technical spec | Written |
| Sample CSV data | Not generated (header-only placeholders) |
| Bronze / Silver / Gold / Dashboard code | Stubs only |
| Tests and validation evidence | Not run (nothing executable yet) |

Do not treat stub modules, placeholder SQL, or header-only CSVs as a working pipeline.

## What this exercise evaluates

The submission must demonstrate requirement analysis, architecture, AI-assisted implementation, data quality engineering, testing, validation of AI output, debugging, documentation, responsible AI usage, human ownership of technical decisions, meaningful Git history, and complete AI prompt history.

## Planned layers

- **Bronze:** raw, unchanged CSV ingest into Databricks tables, plus ingestion metadata.
- **Silver:** five quality modules (completeness, uniqueness, type validation, referential integrity, business logic). Bad rows are flagged, not deleted.
- **Gold:** business aggregations in SQL (sales by product, revenue by customer, daily/weekly trends, customer segmentation).
- **Dashboard:** Databricks SQL dashboard with at least three tiles and filters.

## Repository layout

See `cursor-workflow/spec.md` and `cursor-workflow/task-breakdown.md` for the staged plan. Required paths match `DE_C1_REQUIREMENTS.md`.

## Setup (planned — not verified)

Setup commands will be added after data generation and pipeline implementation exist. Intended runtime:

- Databricks workspace with Spark / PySpark
- Databricks SQL for Gold queries and the dashboard
- Python 3.x for the data-generation script

No real PII, credentials, secrets, tokens, or private production connection details belong in this repository.

## Working rules

Every meaningful change must be derived from the written spec, tested, reviewed against requirements, recorded in `ai-prompts/`, and committed with a descriptive message.

## Documentation map

- `requirements-analysis.md` — problem, requirements, ambiguities, decisions
- `design-notes.md` — architecture decisions
- `data-model.md` — planned Bronze / Silver / Gold contracts
- `data-quality-strategy.md` — quality checks and metrics
- `cursor-workflow/` — persistent context for Cursor-assisted work
- `ai-prompts/` — actual prompt history (not fabricated)
