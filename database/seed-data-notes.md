# Seed data notes

Status: Stage 2 sample CSVs are generated. They have **not** been loaded into Databricks Bronze tables yet.

## How `data/*.csv` were generated

```
python src/data_generation/generate_sample_data.py --output-dir data --seed 42
```

Details: `src/data_generation/DATA_GENERATION_NOTES.md`. Synthetic names/emails only.

## Exact counts (seed 42)

| File | Unique keys | Physical rows |
|---|---|---|
| customers.csv | 10,000 | 10,010 |
| orders.csv | 100,000 | 100,020 |
| products.csv | 500 | 500 |

Mandatory injected issues: 50 NULL emails; 10 extra duplicate customer rows; 100 NULL order customer_id; 200 NULL order product_id; 50 orphan customer_id; 30 orphan product_id; 20 extra duplicate order rows. Optional: 30 future signup dates (not in the 460).

Bronze ingest (Stage 4) must read these files without transformation.
