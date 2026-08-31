# Data-generation prompts

Stage 2 prompt history. Do not treat this file as a Stage 3 (Bronze) log.

Evaluator scan format: PROMPT SENT / AI RESPONSE SUMMARY / ACCEPTED / CHANGED / REJECTED / VALIDATION / FINAL DECISION / COMMIT / ARTIFACT. Historical FINAL DECISION wording is frozen at the time of that prompt. Current repository status is in `tool-workflow.md` and `FINAL_AUDIT.md`.

---

## P004 — Stage 2 generator implementation

### PROMPT SENT

The user asked to start Stage 2 of the DE C1 AI Capability Exercise. Git was already initialized; requirements/design were complete at HEAD `3fa1c570617404a988c6f9b71c55a7d42a4839d9`. Instructions were: do not reinitialize Git, do not recreate the repository structure, and do not modify completed requirements/design decisions unless a concrete contradiction with the canonical requirements was found.

Required reading: `DE_C1_REQUIREMENTS.md`, `requirements-analysis.md`, `design-notes.md`, `data-model.md`, `data-quality-strategy.md`, `cursor-workflow/project-context.md`, `cursor-workflow/spec.md`, `cursor-workflow/cursor-rules-or-instructions.md`, `cursor-workflow/task-breakdown.md`, `src/data_generation/DATA_GENERATION_NOTES.md`, plus the stub generator and header-only CSVs.

Objective: implement a deterministic, realistic synthetic e-commerce data generator producing:

- Customers: 10,000 unique IDs, 10,010 physical rows
- Orders: 100,000 unique order IDs, 100,020 physical rows
- Products: 500 unique products, 500 physical rows

Source schemas and field names as documented. Mandatory defects: 50 NULL emails; 10 duplicate customer_id rows; 100 NULL order customer_id; 200 NULL order product_id; 50 orphan customer_id; 30 orphan product_id; 20 duplicate order_id rows. Do not pad to ~700. Future signup dates optional and documented if injected. Deterministic seed (configurable, documented default). argparse for output directory and seed; no hardcoded Windows or Databricks paths. Built-in validation of counts, schema, nulls, duplicates, unique keys, RI, orphans, domains, dates, financial consistency, reproducibility, no schema drift. Tests for those contracts. Update notes, `ai-prompts/data-generation.md`, task-breakdown, README if generation instructions became concrete. Then `git status` / `git diff --stat` / `git diff --check`, then commit `feat: add deterministic e-commerce sample data generator`. Do not start Bronze.

### AI RESPONSE SUMMARY

Implemented `src/data_generation/generate_sample_data.py` as a stdlib-only generator (`random.Random`, `csv`, `decimal`, argparse). Default seed 42, as-of date 2026-08-31. Products then unique customers, then 100,000 valid orders, then disjoint NULL/orphan overlays, then exact duplicate copies of clean rows. Optional 30 future signups injected on a disjoint customer set and excluded from the order pool so they do not create accidental `order_before_signup` failures. Built-in validation distinguishes mandatory / optional / accidental checks and fails the run on mismatch. Added `tests/test_generate_sample_data.py` (unittest). Generated `data/*.csv`. First generator run and 14 unit tests passed; no padding to 700.

### ACCEPTED

- Frozen row contract 10,010 / 100,020 / 500 and unique-key counts 10,000 / 100,000 / 500.
- Listed defect counts only (460 issue instances). No extra 240 defects.
- NULL FK ≠ orphan; orphans use namespaces `90001–90050` and `9001–9030`.
- Exact-copy duplicates of clean rows only (one duplicate mechanism).
- `total_amount = quantity * unit_price` with DECIMAL(18,2) ROUND_HALF_EVEN.
- argparse `--output-dir` / `--seed` / `--as-of-date`; default output is `<repo>/data` derived from `__file__`.
- `tests/` directory (assignment requires tests; tree omitted them; §6.15).
- unittest instead of pytest so Stage 2 has no new dependency.
- Payment dates consistent with Completed / Cancelled / Pending rules.

### CHANGED

- Injected 30 future signup dates (`2026-09-01`..`2026-09-30`) as an **optional** business-logic defect, documented separately from the 460. Those customers are excluded from orders to avoid cascading BL failures.
- Duplicate extra rows appended at the end of each file rather than interleaved, so sources are easy to list.
- `unit_price` on NULL/orphan product rows keeps the originally sampled valid product price after the ID is overwritten (financial consistency without inventing a second price generator).
- Senior-review cleanup: removed a tautological validation check (`None and False`); added logging of NULL-email IDs. CSV bytes were not regenerated (data path unchanged).

### REJECTED

- Padding defects to ~700: listed counts are the testable contract.
- Faker: extra dependency and harder reproducibility; closed name lists plus `{first}.{last}{id}@example.com` are enough.
- pandas: unnecessary for 100k rows and against the “no extra deps” rule.
- Injecting future signups *and* still assigning them orders: would create accidental `order_not_before_signup` failures.
- A second fuzzy-duplicate mechanism (near-copies with changed emails): the spec asks for extra rows reusing the key.
- Hard-coded `D:\...` or Databricks `/Workspace/...` output paths.
- pytest as a Stage 2 dependency.
- Starting Bronze.

### VALIDATION

Commands actually run (2026-08-31):

1. `python src/data_generation/generate_sample_data.py --output-dir data --seed 42`

   Exit code 0. Validation PASSED: physical 10010 / 100020 / 500; unique 10000 / 100000 / 500; NULL email 50; NULL order customer_id 100; NULL order product_id 200; orphan customer 50; orphan product 30; extra duplicate rows 10 / 20; uniqueness FAIL rows 20 / 40; future signups 30; accidental extras 0. SHA-256 recorded in `DATA_GENERATION_NOTES.md`.

2. `python -m unittest tests.test_generate_sample_data -v`

   `Ran 14 tests in 67.360s` → **OK**. Included physical/unique/null/duplicate/orphan/schema tests, same-seed byte identity, and committed-file vs seed-42 regeneration.

No test was marked PASS without being executed. No validation was fabricated.

### FINAL DECISION

Retain this generator version: seed **42**, as-of **2026-08-31**, listed mandatory defects only, optional 30 future signups documented, LF UTF-8 CSVs in `data/`, unittest suite under `tests/`. Do not regenerate unless the seed or contract changes. Do not start Bronze until requested.

### COMMIT / ARTIFACT

`f5f3acf` — `feat: add deterministic e-commerce sample data generator`. Files: `src/data_generation/generate_sample_data.py`, `data/*.csv`, `tests/test_generate_sample_data.py`, `src/data_generation/DATA_GENERATION_NOTES.md`.
