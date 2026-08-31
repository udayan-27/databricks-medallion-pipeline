# Database setup notes

Status: Databricks objects have not been created from this repository. A code-level compatibility audit (2026-08-31) found the Databricks path already isolated from local Windows Spark workarounds. Execution has **not** started.

## Planned setup (not executed)

1. Create schemas `bronze`, `silver`, and `gold` (catalog name via `MEDALLION_CATALOG`, not a secret).
2. Apply `database/schema.sql` once, or let Bronze `CREATE SCHEMA IF NOT EXISTS` plus `saveAsTable` create the Bronze objects on first ingest.
3. Run Bronze ingest, then Silver, then Gold SQL.
4. Point Databricks SQL dashboard datasets at Gold tables/views.

## Databricks runtime parameters (placeholders — do not commit a workspace name)

Copy CSVs to a distributed path first. Then set these at job/cluster time. Do not invent or commit a personal/production catalog.

| Parameter | Purpose | How to set |
|---|---|---|
| `MEDALLION_DATA_PATH` / `--data-path` | Directory that already contains `customers.csv`, `orders.csv`, `products.csv` | `/Volumes/<catalog>/<schema>/<volume>`, `dbfs:/...`, `s3://...`, or `abfss://...`. Not `C:\` / `D:\`. |
| `MEDALLION_CATALOG` / `--catalog` | Unity Catalog catalog | Runtime value for this workspace. Omit on Hive metastore. Do not default to `main` in git. |
| `MEDALLION_BRONZE_SCHEMA` | Bronze schema | `bronze` |
| `MEDALLION_SILVER_SCHEMA` | Silver schema | `silver` |
| `MEDALLION_GOLD_SCHEMA` | Gold schema | `gold` |
| `MEDALLION_TABLE_FORMAT` / `--table-format` | Table format | **omit** (default `delta`). Do **not** pass `parquet`. |

Do **not** bring the local Windows stack onto Databricks:

- project `.venv`
- winutils / Hadoop `HADOOP_HOME`
- `NoWinutilsRawLocalFileSystem` (local Windows sessions only)
- laptop `JAVA_HOME` / PowerShell `TEMP` hacks
- `--table-format parquet`
- local `spark.sql.warehouse.dir`

`get_spark_session` reuses the cluster session. Gold SQL is executed by `create_gold_tables.py`, which substitutes `{silver_schema}` / `{gold_schema}`. Do not paste unsubstituted Gold files into the SQL editor. Dashboard queries: replace `{gold_schema}` with `gold` or `<catalog>.gold` as in `DASHBOARD_GUIDE.md`.

## Bronze job (code exists; not run here)

On a Databricks cluster:

```
python src/bronze/ingest_all.py --data-path <volume-or-dbfs-or-s3-dir>
```

Optional: `--catalog`, `--bronze-schema` (default `bronze`). Omit `--table-format` so the default `delta` is used.

Local Spark without Delta: `--table-format parquet`.

This machine now has a project-local Python 3.11 `.venv` with PySpark 3.5.6 and Temurin JDK 17. An in-memory Spark smoke test passed. Local parquet Bronze ingest tests now pass on Windows: local input paths use `Path.as_posix()` (not percent-encoded `file:` URIs), and locally created SparkSessions install a Java FileSystem that avoids winutils/`hadoop.dll`. That is local-runtime evidence, not a Databricks/Delta/UC pass. Databricks objects still have not been created from this repository.

Rerun overwrites `bronze.customers|orders|products` and appends to `bronze.ingest_metadata`.

## Gold job (code exists; local parquet tested; Databricks not run)

After Bronze and Silver tables exist:

```
python src/gold/create_gold_tables.py --table-format parquet
```

Databricks: omit `--table-format` (default `delta`) and set `--catalog` / `--silver-schema` / `--gold-schema` as needed. This environment has **not** written Gold Delta tables.

## Secrets

Do not put workspace tokens, personal access tokens, or production connection strings in this file. Use environment variables or Databricks secrets.
