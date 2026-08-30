# Documentation prompts

Meaningful AI interactions for documentation and repository structure. Entries are real; they are not invented after the fact.

---

## Prompt 1 — 2026-08-31 — Read requirements (no implementation)

### Prompt text (summary)

Act as the implementation agent for the DE C1 AI Capability Exercise. Before any implementation, read `DE_C1_REQUIREMENTS.md` completely and treat it as canonical. Do not implement the pipeline yet. First understand the assignment, including requirement analysis through complete AI prompt history. Record the ~700-row inconsistency, four-vs-five quality-check inconsistency, and the example prompt’s future signup dates. Confirm: full requirements read; repository structure understood; ambiguities identified; AI workflow evidence is part of the submission.

### AI response summary

The assistant read the full requirements file and restated the medallion pipeline, source schemas, Bronze/Silver/Gold/dashboard rules, required tree, and working constraints (PySpark, SQL Gold, no PII, no deleting bad rows, Bronze raw). It listed the three ambiguities with a 460 vs 700 count table and stated it would not hide them. It confirmed AI prompt history is scored and that this confirmation turn should later be recorded. No files were created.

### Accepted and why

- No coding on that turn, matching the instruction.
- Ambiguities named rather than smoothed over.
- Five Silver modules treated as required.

### Changed and why

None; there was no implementation.

### Rejected and why

Not applicable (no code suggestions).

### Validation performed

File read of `DE_C1_REQUIREMENTS.md` only. No tests. No pipeline validation.

### Final decision

Hold implementation until the user requests the next stage.

---

## Prompt 2 — 2026-08-31 — Initialize project structure

### Prompt text (summary)

Initialize the complete repository structure required by `DE_C1_REQUIREMENTS.md`. Do not implement the pipeline. Do not populate final sample CSV data. Create all listed root files, `src/*`, `data/`, `database/`, `ai-prompts/`, and `cursor-workflow/`. Write `requirements-analysis.md` with problem statement through acceptance criteria plus explicit 700-row and four-vs-five treatment. Write Cursor context, spec, rules, and task breakdown. Create `.gitignore`. Update this file with the actual interaction. If Git is not initialized, initialize and commit:

`chore: initialize project structure and engineering spec`

Show the file tree, verify required files, Git status, first commit, and unresolved items. Do not start data generation.

### AI response summary

The assistant created the required tree at the project root (fulfilling `databricks-medallion-pipeline/` layout without nesting an extra folder). Documentation was written as planned/honest status, not as fake results. Python/SQL modules are stubs (`NotImplementedError` or “NOT IMPLEMENTED” comments). CSVs are header-only. `requirements-analysis.md` records the 460 vs 700 gap and the five-module decision. Git was initialized and the requested commit was made after the files existed.

### Accepted and why

- Full required path list, including five Silver stubs.
- Header-only CSVs instead of generated 10k/100k/500 rows.
- Ambiguities documented as implementation decisions, not hidden.
- Prompt history limited to this real documentation/init work.

### Changed and why

- Files placed at workspace root rather than a nested `databricks-medallion-pipeline/` directory, because this folder is already the project root. Recorded in `requirements-analysis.md` assumption 1 and `README.md`.

### Rejected and why

- Implementing ingest/quality/Gold SQL bodies — out of scope.
- Generating sample rows — explicitly forbidden.
- Inventing test results, debugging stories, or reflection content.
- Padding defect counts to 700.

### Validation performed

- Required paths checked against the list in `DE_C1_REQUIREMENTS.md` after creation.
- Git status and `git log` inspected after the initial commit.
- No PySpark jobs, no pytest, no CSV row-count validation (data not generated).

### Final decision

Stage 1 (structure + spec) is the Git baseline. Stage 2 (data generation) waits for an explicit user request.
