# Design notes

Status: architecture and design decisions for implementation. **No pipeline has been built yet. No runtime results exist.**

## Goal

A Databricks Medallion pipeline for synthetic e-commerce sales, with Bronze raw ingest, Silver quality flagging, Gold SQL aggregations, and a SQL dashboard.

## Architecture

```
data/*.csv
    -> Bronze ingest (PySpark, raw)
        -> Silver quality modules (PySpark, flag not delete)
            -> Gold aggregations (SQL)
                -> Databricks SQL dashboard
```

### Bronze

- One ingest module per entity, plus `ingest_all.py` as orchestrator.
- Read CSV with an explicit schema (not silent schema drift).
- Write Delta tables (planned): `bronze.customers`, `bronze.orders`, `bronze.products`.
- Write ingestion metadata (source path, row count, timestamp, ingest_id).
- No value-level cleaning.

### Silver

- Read Bronze tables.
- Apply five independent quality modules, then combine flags in `create_silver_tables.py`.
- Persist all rows with:
  - `quality_check_result` (`PASS` / `FAIL`)
  - `failed_checks` (list of failed check names)
  - optional per-check boolean columns for metrics
- Emit a quality-metrics table or log with pass/fail counts and percentages.
- Never drop rows to improve pass rate.

### Gold

- SQL only for the four required query files.
- `create_gold_tables.py` may orchestrate execution of those SQL files in Spark/Databricks; it must not replace the SQL with undocumented PySpark aggregations.
- Gold reads Silver (and dimension fields) according to filters documented per query.

### Dashboard

- Databricks SQL: bar, histogram, pie, plus filters.
- Queries live in `src/dashboard/dashboard_queries.sql`.
- Manual workspace click-path documented in `DASHBOARD_GUIDE.md` when built.

## Key decisions

| Decision | Choice | Why |
|---|---|---|
| Processing engine | PySpark for Bronze/Silver | Required distributed processing |
| Gold / dashboard | SQL | Required by spec |
| Bad rows | Flag, retain | Spec forbids deleting rows to pass checks |
| Bronze | Immutable raw | Spec |
| Quality modules | All five | Required tree + documented business logic |
| Defect counts | Listed 460 issue instances, not padded to 700 | See `requirements-analysis.md` |
| Catalog/schema names | Parameters / config, not personal workspace secrets | Responsible AI |
| Data | Synthetic only | Responsible AI |

## Open design items (not silently closed)

- Exact Unity Catalog catalog/schema names for the candidate’s Databricks workspace.
- Whether local PySpark tests use a temp warehouse or Databricks Connect.
- Final Gold filter predicates per metric.
- Whether to inject 30 future signup dates at generation time.
- Histogram binning for customer revenue on the dashboard.

## Non-goals for initialization

These notes do not include measured row counts, query runtimes, dashboard screenshots, or test pass/fail results.
