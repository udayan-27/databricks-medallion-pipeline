# Candidate information

This file holds evaluator-facing candidate and environment facts that are **actually known** from the project. Unknown personal fields are explicit placeholders for the official submission form. They are not invented.

| Field | Value |
|---|---|
| Candidate name | Udayan Mahajan |
| Role | [To be completed in submission form] |
| Exercise | DE C1 Coding Evaluation — Databricks Medallion Pipeline |
| Project option | Data Pipeline (Medallion Architecture) |
| Primary AI tool | Cursor |
| Primary technology stack | Python / PySpark / SQL / Databricks |
| Languages and libraries | Python 3.11 (local venv); PySpark 3.5.6; Spark SQL / Databricks SQL; unittest; Python standard library for generation (`csv`, `random`, `decimal`, `argparse`) |
| Databricks environment | Databricks Free Edition / Serverless (runtime catalog `workspace`; Unity Catalog volume `/Volumes/workspace/de_c1/source_data`; default table format Delta). See `database/setup-notes.md`. |
| Assessment start date | [To be completed in submission form] |
| Submission date | [To be completed in submission form] |
| Documented implementation date in this repository | 2026-08-31 (frozen as-of date for seed-42 data; date recorded on prompt logs and commits). This is **not** a substitute for the official assessment/submission dates above. |
| Intended public repository | https://github.com/udayan-27/databricks-medallion-pipeline |

## Submission process (no personal identifiers in this file)

Submit this repository using the required organizational/TTN account and email process.

Do **not** publish in this file: personal phone, employee ID, internal account IDs, access tokens, credentials, private organizational metadata, or the exact organizational email address.

Contact email and the organizational Git account used for official submission are intentionally omitted here. Git commit author metadata is separate from this file and is not copied here.

## Setup summary

- Local: Python 3.11 virtualenv, JDK 17, PySpark 3.5.6 from `requirements.txt`. Bronze/Silver/Gold local tests use parquet (`--table-format parquet`). See `README.md`.
- Databricks: Git-folder repository root; `python src/databricks/run_pipeline.py` copies committed `data/*.csv` to the UC volume and runs existing Bronze/Silver/Gold modules as Delta. Visual dashboard tiles are created in the Databricks SQL UI, not by that command.
- No secrets in git. `.env` and `.venv/` are gitignored.

## Ownership

Technical decisions in this repository are owned by the candidate. AI output is treated as a draft until it is compared to `DE_C1_REQUIREMENTS.md`, reviewed, and accepted or rejected in `ai-prompts/`.

Cursor/AI assisted with requirement decomposition, architecture, implementation, tests, debugging, dashboard SQL, Databricks automation, and documentation. The candidate interpreted ambiguities, accepted or rejected proposals, decided quality-rule semantics, validated results, completed Databricks OAuth in the browser, and created/published the visual dashboard in the Databricks UI.
