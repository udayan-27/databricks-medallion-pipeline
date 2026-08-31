# Database setup notes

Status: Databricks objects have **not** been created from this environment. The official workspace process is the repository workflow in `src/databricks/`. Do not use ad-hoc notebook cells as the submission path.

## MANUAL vs AUTOMATED

### MANUAL — genuinely unavoidable

1. **Databricks login / account authorization** for the workspace.
2. **GitHub authorization** if Databricks Git folders require it.
3. **Git-folder creation/connection** if the Databricks UI/API requires it. Connect [this GitHub repository](https://github.com/udayan-27/databricks-medallion-pipeline.git) so the repo root (including `data/` and `src/`) is available as workspace files. Example location in one Free Edition workspace (do not hard-code this in jobs): `/Workspace/Users/<your-user>/databricks-medallion-pipeline`.
4. **Visual dashboard rendering** in the Databricks SQL UI after Gold tables exist. See `src/dashboard/DASHBOARD_GUIDE.md`. Query correctness is validated by the workflow; bar/histogram/pie widgets are not created by the repository command.

### AUTOMATED

`python src/databricks/run_pipeline.py` from the Git-folder repository root:

- `CREATE SCHEMA IF NOT EXISTS` for `{catalog}.bronze`, `{catalog}.silver`, `{catalog}.gold`, `{catalog}.de_c1`
- `CREATE VOLUME IF NOT EXISTS` `{catalog}.de_c1.source_data`
- Copy version-controlled `data/customers.csv`, `data/orders.csv`, `data/products.csv` from the Git folder into `/Volumes/{catalog}/de_c1/source_data/` (no regeneration, no edits)
- Query those files and assert row counts and intentional defects
- Run existing `ingest_all` → `create_silver_tables` → `create_gold_tables` with `PipelineConfig` (`data_path` = the volume, `catalog=workspace`, schemas `bronze`/`silver`/`gold`, `table_format=delta`, `spark_app_name=DE_C1_Databricks`)
- Validate Bronze, Silver, Gold, and `src/dashboard/dashboard_queries.sql`
- Print `CHECK / EXPECTED / ACTUAL / STATUS / NOTES` and `FINAL RESULT: PASS` or `FAIL` (non-zero exit on critical failure)

## Source-data transfer mechanism

Do not upload CSVs by hand unless every programmatic copy fails.

On Databricks Free Edition / Serverless, Git-folder files are **workspace files** (`/Workspace/...`). UC Volumes are **`/Volumes/{catalog}/{schema}/{volume}/`**. Official Databricks file docs:

- Workspace files support OSS Python (`os.listdir('/Workspace/...')`, `open()`, `pathlib`). Serverless environment 2+ supports programmatic workspace-file access.
- Volumes support OSS Python (`os.listdir('/Volumes/...')`) and `shutil.copyfile` onto a volume path. Spark and SQL read `/Volumes/...` natively.
- Databricks utilities (`dbutils.fs`) and Spark require the `file:/` scheme for workspace files.

`src/databricks/bootstrap.py` copies each CSV without recoding, in this order:

1. POSIX / FUSE Python byte copy (Git-folder `data/` → volume)
2. Copy via driver temp, then write the volume (documented volume-write pattern)
3. `dbutils.fs.cp("file:/Workspace/...", "/Volumes/...")`

After copy it compares size and SHA-256 to the Git-folder original. Databricks CLI and REST Files APIs are not used (they need extra credentials that must not live in this repo).

If all three mechanisms fail, the workflow raises and names the remaining manual action: upload the three unchanged Git-folder CSVs into the volume. Do not regenerate them.

Git-folder path is derived from the repository layout (`<repo>/data`), not from a hard-coded user folder.

## Exact Databricks command

Attach a serverless or cluster Python session to the Git folder. Working directory = repository root.

```
python src/databricks/run_pipeline.py
```

Equivalent explicit parameters:

```
python src/databricks/run_pipeline.py --catalog workspace --source-schema de_c1 --source-volume source_data --bronze-schema bronze --silver-schema silver --gold-schema gold --table-format delta --spark-app-name DE_C1_Databricks
```

Optional individual stages: `--stage bootstrap|source|bronze|silver|gold|dashboard`.

Do **not** bring the local Windows stack onto Databricks:

- project `.venv`
- winutils / Hadoop `HADOOP_HOME`
- `NoWinutilsRawLocalFileSystem` (local Windows sessions only)
- laptop `JAVA_HOME` / PowerShell `TEMP` hacks
- `--table-format parquet`

`get_spark_session` reuses the cluster session. Gold SQL is executed by `create_gold_tables.py`. Dashboard SQL validation substitutes `{gold_schema}` automatically.

## Resource policy

| Class | Objects |
|---|---|
| Safe `CREATE IF NOT EXISTS` | schemas `bronze`, `silver`, `gold`, `de_c1`; volume `source_data` |
| Overwritten each normal run | Bronze entity tables; all Silver tables including `quality_metrics`; all Gold tables; the three volume CSVs (replaced from Git `data/`) |
| Append-only | `bronze.ingest_metadata` |
| Preserved | Git repository `data/*.csv`; unrelated catalogs/schemas; the catalog itself |

Normal execution does **not** drop schemas or volumes.

## Optional RESET / RECREATE

Evaluation workspace only. Requires `--reset`. Reports the plan, then:

- `DROP TABLE IF EXISTS` known pipeline tables in `{catalog}.bronze`, `{catalog}.silver`, `{catalog}.gold`
- deletes only `customers.csv`, `orders.csv`, `products.csv` in `{catalog}.de_c1.source_data`

Never drops the catalog, never touches unrelated schemas, never modifies Git `data/`.

```
python src/databricks/run_pipeline.py --reset
```

## Local Spark (not Databricks)

Local parquet jobs remain:

```
python src/bronze/ingest_all.py --table-format parquet
python src/silver/create_silver_tables.py --table-format parquet
python src/gold/create_gold_tables.py --table-format parquet
```

That is local evidence, not a Databricks/Delta/UC pass.

## Secrets

Do not put workspace tokens, personal access tokens, or production connection strings in this file. Use environment variables or Databricks secrets.
