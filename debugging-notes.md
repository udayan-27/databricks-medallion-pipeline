# Debugging notes

Stage 2 (data generation) produced no runtime defect/debug cycle.

The generator’s built-in validation passed on the first seed-42 run. Unit tests (`python -m unittest tests.test_generate_sample_data -v`) were 14/14 OK. A tautological validation check (`customer_id is None and False`) was removed in a senior review pass; it did not hide a real data bug and did not require regenerating CSVs.

Use this file during later stages to capture:

- Symptom
- Expected vs actual
- Root cause
- Files changed
- Tests re-run and **actual** results
- Whether the AI suggestion was accepted, changed, or rejected

Do not invent debugging sessions.
