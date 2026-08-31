-- Databricks SQL dashboard queries
-- Status: IMPLEMENTED (SQL + local Spark validation)
-- Engine: Spark SQL / Databricks SQL
-- Databricks SQL Dashboard UI: NOT rendered from this environment.
--
-- Reads Gold only. Does not query Bronze or Silver. Does not recompute
-- qualifying-order eligibility, lifetime_value_actual, or segmentation.
--
-- Placeholders substituted by local tests:
--   {gold_schema} = qualified Gold schema (catalog.gold or gold)
-- Databricks SQL Editor: replace {gold_schema} with gold or
-- <catalog>.gold before running. Do not hard-code a personal catalog.
--
-- Required tiles:
--   1. Top 10 products by revenue (bar)
--   2. Customer revenue distribution (histogram)
--   3. Customer segmentation (pie)
--
-- Histogram binning: Databricks visualization bins lifetime_value_actual.
-- This file does not pre-bin (no WIDTH_BUCKET / CASE buckets). See
-- requirements-analysis.md §6.17 and DASHBOARD_GUIDE.md.
--
-- Filters (Gold fields only; see DASHBOARD_GUIDE.md):
--   category          — Tile 1 query parameter, applied BEFORE LIMIT 10
--   customer_segment  — Tile 2 column/parameter on the customer population
-- Rejected as filters on these tiles: order date (no date grain on these
-- Gold tables), country (not on Gold), segment_type on the pie (would hide
-- the four exclusive buckets the tile exists to show).

-- DASHBOARD_QUERY: top_10_products
-- Gold source: gold.sales_by_product
-- Business question: Which products contribute the most qualifying revenue?
-- Dimensions: product_id, product_name, category
-- Measures: total_revenue (bar length); total_orders as supporting context
-- Ordering: total_revenue DESC, product_id ASC (deterministic on revenue ties)
-- Top-N: LIMIT 10. Fewer than 10 Gold product rows → fewer than 10 rows.
--         Tied revenue at the cutoff: lowest product_id wins the remaining slots.
-- Expected interpretation: ranking of products that already have at least one
--   qualifying order (Gold omits unused products). Revenue is Completed + PASS
--   only because Gold already applied that filter.
-- Filters: optional category parameter MUST be applied as WHERE before LIMIT.
--   Do not attach a dashboard widget to this result's category column after
--   LIMIT 10 — that would subset the global top 10, not recompute Top 10 inside
--   the category. Databricks binding example (not executed locally):
--     WHERE (:category IS NULL OR category <=> :category)
-- Null categories: included. A category filter of NULL uses NULL-safe equality
--   (<=>) if bound; the unfiltered query below keeps them.

SELECT
  product_id,
  product_name,
  category,
  total_revenue,
  total_orders
FROM {gold_schema}.sales_by_product
ORDER BY
  total_revenue DESC,
  product_id ASC
LIMIT 10

-- DASHBOARD_QUERY: customer_revenue_distribution
-- Gold source: gold.revenue_by_customer
-- Business question: How is qualifying customer revenue distributed?
-- Population: ALL canonical customers in gold.revenue_by_customer, including
--   zero-revenue / Inactive customers (lifetime_value_actual = 0.00). This
--   matches the Gold contract. Do not silently drop zeros.
-- Grain: one row per customer_id (no dashboard re-aggregation).
-- Dimensions: customer_id (grain), customer_segment (source Premium/Standard/Basic)
-- Measures: lifetime_value_actual (= Gold total_revenue; not source lifetime_value)
-- Ordering: none required for a histogram. customer_id ASC is deterministic only.
-- Expected interpretation: Databricks histogram bins lifetime_value_actual.
--   A spike at 0.00 is Inactive customers, not a query defect.
-- Filters: optional customer_segment on this result is valid (slices the Gold
--   customer population). Do not filter total_orders > 0 unless a documented
--   "active only" variant is added — that would leave the Gold population.
-- Binning: visualization setting, not SQL.

SELECT
  customer_id,
  customer_segment,
  lifetime_value_actual
FROM {gold_schema}.revenue_by_customer
ORDER BY
  customer_id ASC

-- DASHBOARD_QUERY: customer_segmentation
-- Gold source: gold.customer_segmentation
-- Business question: How many customers sit in each exclusive value segment?
-- Dimensions: segment_type
-- Measures: customer_count
-- Ordering: none required for a pie. Gold already stores one row per type.
-- Expected interpretation: High-Value / Repeat / One-Time / Inactive are
--   mutually exclusive. Empty buckets remain as customer_count = 0 (Gold
--   still emits four rows). Databricks pie charts may hide zero slices; that
--   is a UI limitation, not a reason to drop those rows in SQL.
-- Filters: do not filter this tile by segment_type. The tile's purpose is the
--   four-way mix. customer_segment (Premium/Standard/Basic) is a different
--   field and is not on this Gold table — do not join Silver to add it.
-- Do not recalculate the CASE / 1000.00 threshold here.

SELECT
  segment_type,
  customer_count
FROM {gold_schema}.customer_segmentation

-- DASHBOARD_QUERY: filter_values_category
-- Gold source: gold.sales_by_product.category
-- Purpose: dropdown values for the Tile 1 category parameter.
-- Not a dashboard tile. Distinct categories that appear on Gold products
-- (products with at least one qualifying order). Null category is included.

SELECT DISTINCT
  category
FROM {gold_schema}.sales_by_product
ORDER BY
  category ASC NULLS LAST

-- DASHBOARD_QUERY: filter_values_customer_segment
-- Gold source: gold.revenue_by_customer.customer_segment
-- Purpose: dropdown values for the Tile 2 customer_segment filter.
-- Not a dashboard tile. Distinct source segments on the Gold customer population.

SELECT DISTINCT
  customer_segment
FROM {gold_schema}.revenue_by_customer
ORDER BY
  customer_segment ASC NULLS LAST
