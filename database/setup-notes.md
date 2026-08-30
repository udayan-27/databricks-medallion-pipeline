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

Local Spark without Delta: `--table-format parquet`.

This machine now has a project-local Python 3.11 `.venv` with PySpark 3.5.6 and Temurin JDK 17. An in-memory Spark smoke test passed. Local parquet Bronze ingest tests now pass on Windows: local input paths use `Path.as_posix()` (not percent-encoded `file:` URIs), and locally created SparkSessions install a Java FileSystem that avoids winutils/`hadoop.dll`. That is local-runtime evidence, not a Databricks/Delta/UC pass. Databricks objects still have not been created from this repository.

Rerun overwrites `bronze.customers|orders|products` and appends to `bronze.ingest_metadata`.

## Secrets

Do not put workspace tokens, personal access tokens, or production connection strings in this file. Use environment variables or Databricks secrets.
