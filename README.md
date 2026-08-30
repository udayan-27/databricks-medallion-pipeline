# Databricks Medallion Pipeline — E-commerce Sales

This repository is the DE C1 AI Capability Exercise submission. It implements (when complete) a Databricks Medallion Architecture pipeline:

`CSV -> Bronze -> Silver -> Gold -> Dashboard`

Requirements and architecture are written. **Stage 2 sample data has been generated.** Bronze / Silver / Gold / Dashboard code is still stubs.

Canonical requirements: [`DE_C1_REQUIREMENTS.md`](DE_C1_REQUIREMENTS.md).

## Status

| Area | Status |
|---|---|
| Repository structure | Initialized |
| Requirements analysis, architecture, data model, DQ strategy | Written (design stage) |
| Sample CSV data | Generated (10,010 / 100,020 / 500 rows; seed 42) |
| Bronze / Silver / Gold / Dashboard code | Stubs only |
| Tests and validation evidence | Generator tests run (`tests/test_generate_sample_data.py`) |

Do not treat stub Bronze/Silver/Gold modules or placeholder SQL as a working pipeline. The CSVs in `data/` are real generated inputs.

## What this exercise evaluates

The submission must demonstrate requirement analysis, architecture, AI-assisted implementation, data quality engineering, testing, validation of AI output, debugging, documentation, responsible AI usage, human ownership of technical decisions, meaningful Git history, and complete AI prompt history.

## Planned layers

- **Bronze:** raw, unchanged CSV ingest into Databricks tables, plus ingestion metadata.
- **Silver:** five quality modules (completeness, uniqueness, type validation, referential integrity, business logic). Bad rows are flagged, not deleted.
- **Gold:** business aggregations in SQL (sales by product, revenue by customer, daily/weekly trends, customer segmentation).
- **Dashboard:** Databricks SQL dashboard with at least three tiles and filters.

## Repository layout

See `cursor-workflow/spec.md` and `cursor-workflow/task-breakdown.md` for the staged plan. Required paths match `DE_C1_REQUIREMENTS.md`.

## Setup (partial — generation verified)

### Sample data

From the repository root (Python 3.12 used when this was run):

```
python src/data_generation/generate_sample_data.py --output-dir data --seed 42
python -m unittest tests.test_generate_sample_data -v
```

Default seed is **42**. The default `--output-dir` is the repo `data/` directory (resolved from the script path, not a hardcoded Windows or Databricks path). Same seed produces byte-identical UTF-8/LF CSVs. See `src/data_generation/DATA_GENERATION_NOTES.md`.

Databricks Spark / SQL warehouse setup remains planned and unverified until Bronze.

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
