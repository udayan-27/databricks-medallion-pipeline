# Project context (Cursor)

Use this file as persistent context for all later work in this repository.

## Assignment

DE C1 AI Capability Exercise: Databricks Medallion pipeline for synthetic e-commerce sales.

Canonical requirements: `DE_C1_REQUIREMENTS.md`  
Interpreted requirements: `requirements-analysis.md`  
Technical spec: `cursor-workflow/spec.md`  
Coding rules: `cursor-workflow/cursor-rules-or-instructions.md`  
Plan: `cursor-workflow/task-breakdown.md`

## Pipeline

`CSV -> Bronze (PySpark, raw) -> Silver (PySpark, five quality modules, flag not delete) -> Gold (SQL) -> Databricks SQL dashboard`

## Source entities

- customers: 10,000 rows planned; PK customer_id
- orders: 100,000 rows planned; PK order_id; FKs to customers and products
- products: 500 rows planned; PK product_id

Current `data/*.csv` files are **header-only placeholders**. Do not treat them as generated sample data.

## Mandatory injected issues (do not pad to 700)

- 50 NULL emails
- 10 duplicate customer_id rows
- 100 NULL order customer_id
- 200 NULL order product_id
- 50 orphan customer_id
- 30 orphan product_id
- 20 duplicate order_id rows

Listed sum = 460 issue instances. The guide’s “approximately 700 / 0.7%” does not match. Do not hide this. Do not invent extra rows to force 700.

## Silver modules (implement all five)

1. completeness
2. uniqueness
3. type validation
4. referential integrity
5. business logic

Narrative “four checks” vs five files is a recorded inconsistency. All five files are required.

## Optional / documented

Example prompt’s 30 future signup dates: optional business-logic defect. Record the decision at data-generation time. Do not silently add them to the mandatory list.

## Non-negotiable rules

- Do not implement work ahead of the current requested stage unless asked.
- Inspect relevant files; compare to spec; explain approach; list assumptions and edge cases.
- Implement; run tests; record **actual** results; update docs; update the matching `ai-prompts/*.md`; commit.
- Never claim a test passed unless executed.
- Never fabricate prompt history or validation results.
- Never silently resolve contradictory requirements.
- Never delete bad source records merely to make quality checks pass.
- Bronze remains raw/unchanged.
- PySpark for distributed processing; SQL for required Gold and dashboard queries.
- No real PII, credentials, secrets, passwords, tokens, or private production connection details in the repo or prompts.

## Current stage

Repository structure and engineering spec initialized. **Data generation has not started. Pipeline code is stubs only.**
