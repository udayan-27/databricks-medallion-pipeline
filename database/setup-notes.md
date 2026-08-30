# Database setup notes

Status: Databricks objects have not been created from this repository.

## Planned setup (not executed)

1. Create schemas `bronze`, `silver`, and `gold` (catalog name via `MEDALLION_CATALOG`, not a secret).
2. Apply `database/schema.sql` once, or let Bronze `CREATE SCHEMA IF NOT EXISTS` plus `saveAsTable` create the Bronze objects on first ingest.
3. Run Bronze ingest, then Silver, then Gold SQL.
4. Point Databricks SQL dashboard datasets at Gold tables/views.

## Bronze job (code exists; not run here)

On a Databricks cluster:

```
python src/bronze/ingest_all.py --data-path <volume-or-dbfs-or-s3-dir>
```

Optional: `--catalog`, `--bronze-schema` (default `bronze`), `--table-format delta`.

Local Spark without Delta: `--table-format parquet`. This environment had no PySpark and no JDK, so that path was not executed.

Rerun overwrites `bronze.customers|orders|products` and appends to `bronze.ingest_metadata`.

## Secrets

Do not put workspace tokens, personal access tokens, or production connection strings in this file. Use environment variables or Databricks secrets.
