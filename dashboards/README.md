# Databricks dashboard definition

## Dashboard name

**DE C1 E-Commerce Sales Dashboard**

Workspace display name and the Lakeview object filename match that title. The serialized `.lvdash.json` stores page/widget titles rather than repeating the workspace display name as a top-level field.

## Purpose

Gold-only sales analytics for the DE C1 medallion pipeline: ranked product revenue, the shape of customer revenue, and exclusive customer-value segments.

## Source-controlled definition

The actual Databricks-serialized dashboard is:

[`DE_C1_E-Commerce_Sales_Dashboard.lvdash.json`](DE_C1_E-Commerce_Sales_Dashboard.lvdash.json)

It was retrieved **read-only** from the already-published workspace object (`databricks workspace export` of the Lakeview `.lvdash.json`). It was **not** hand-written and **not** reconstructed from `src/dashboard/dashboard_queries.sql`.

Query contract (Gold SQL used to design and validate tiles): [`../src/dashboard/dashboard_queries.sql`](../src/dashboard/dashboard_queries.sql). Workspace click-path notes: [`../src/dashboard/DASHBOARD_GUIDE.md`](../src/dashboard/DASHBOARD_GUIDE.md).

## Gold-only data contract

Every dataset in the exported definition reads Unity Catalog Gold:

| Dataset `displayName` | Query source |
|---|---|
| `top_10_products` | `workspace.gold.sales_by_product` |
| `customer_revenue_distribution` | `workspace.gold.revenue_by_customer` |
| `customer_segmentation` | `workspace.gold.customer_segmentation` |

Bronze and Silver tables are not referenced. Eligibility (`Completed` + `PASS`), `lifetime_value_actual`, and segmentation rules stay in Gold. The dashboard does not recompute them.

## Three visualizations

| Widget title | `widgetType` | Dataset |
|---|---|---|
| Top 10 Products by Revenue | `bar` | `top_10_products` |
| Customer Revenue Distribution | `histogram` | `customer_revenue_distribution` |
| Customer Segmentation | `pie` | `customer_segmentation` |

Histogram bins are a visualization encoding (`BIN_FLOOR` on `lifetime_value_actual`), not SQL `WIDTH_BUCKET` / `CASE` buckets.

## Two filters

| Filter field | `widgetType` | Bound dataset |
|---|---|---|
| `category` | `filter-single-select` | `top_10_products` |
| `customer_segment` | `filter-single-select` | `customer_revenue_distribution` |

These are Lakeview filter widgets in the serialized layout. The Top 10 dataset SQL is the unfiltered `ORDER BY … LIMIT 10` query; the category control is a dataset-field filter widget, not a checked-in `:category` bind parameter. Do not treat Git as rewriting how the live warehouse applies that widget.

## Databricks identity (safe identifiers)

| Item | Value |
|---|---|
| Dashboard ID | `01f1a504245c12b9807ea30300628445` |
| Workspace object path | `/Users/<workspace-user>/DE C1 E-Commerce Sales Dashboard.lvdash.json` |
| Lifecycle (when exported) | `ACTIVE` |
| Warehouse ID (API metadata, not in `.lvdash.json`) | `e1e059800e5adcdd` |

The personal workspace-user folder name is omitted here on purpose. Dashboard IDs are not secrets.

## Published state

The dashboard **was already published** before this export. `databricks lakeview get-published` returned the published display name, warehouse id, `embed_credentials`, and revision timestamp. That call is metadata only; this repository did not publish, unpublish, or update the dashboard.

## Account-level sharing

Sharing remains **Anyone in my account can view** (account-scoped). That is a Databricks UI/workspace ACL setting. It is **not** present in the `.lvdash.json`. It is not public internet access.

## Serialized definition vs runtime publishing

| In the Git `.lvdash.json` | Not in the Git `.lvdash.json` |
|---|---|
| Datasets and their SQL | Workspace display name as a top-level field |
| Pages, widget layout, viz encodings | Publish / unpublish |
| Filter widgets and field bindings | Account-level sharing / ACLs |
| Viz titles and chart types | SQL warehouse binding |
| | `embed_credentials` and other publish flags |
| | Schedules and subscriptions |

Git versions the **serialized Lakeview definition**. It does not control every Databricks publishing or sharing property.

## Screenshots

No genuine final-dashboard screenshot was found in the repository, Desktop, Downloads, or other obvious project locations. `dashboards/screenshots/` is therefore absent. Do not invent one.

## What this export did not do

- Did not create a second dashboard
- Did not modify the published dashboard
- Did not change Bronze, Silver, Gold, or `dashboard_queries.sql`
- Did not regenerate Stage 2 CSVs
- Did not push to GitHub
