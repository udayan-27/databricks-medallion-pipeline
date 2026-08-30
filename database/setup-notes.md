# Database setup notes

Status: Databricks objects have not been created from this repository.

## Planned setup (not executed)

1. Create schemas `bronze`, `silver`, and `gold` (catalog name via config, not a secret).
2. Apply `database/schema.sql` once DDL is finalized.
3. Run Bronze ingest, then Silver, then Gold SQL.
4. Point Databricks SQL dashboard datasets at Gold tables/views.

## Local development

A later README section will describe any local PySpark test approach. Nothing here has been verified against a workspace.

## Secrets

Do not put workspace tokens, personal access tokens, or production connection strings in this file. Use environment variables or Databricks secrets.
