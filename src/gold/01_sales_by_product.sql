-- Gold: Sales by Product
-- Status: IMPLEMENTED
-- Engine: Spark SQL / Databricks SQL
-- Populated by: src/gold/create_gold_tables.py (executes this SELECT; does not
-- reimplement the aggregation in PySpark).
--
-- GOLD_OUTPUT: sales_by_product
-- Grain: one row per product_id that has at least one qualifying order.
--   Products with zero qualifying orders are omitted (top-10 tile; data-model.md).
--
-- Eligibility (qualifying order) — frozen; do not silently change:
--   order_status = 'Completed'
--   AND quality_check_result = 'PASS'
--   AND product_id IS NOT NULL
-- Combined PASS already requires uniqueness, completeness, type, RI, and
-- business logic. Duplicate order_id copies cannot be PASS, so they cannot
-- inflate total_orders or total_revenue. FAIL rows stay in Bronze/Silver for
-- audit; they are not analytical facts.
--
-- Measures:
--   total_orders      = COUNT(*) of qualifying orders
--                       (not a sum of quantity units; not a count of
--                       physical source rows)
--   total_revenue      = SUM(total_amount), DECIMAL(18,2)
--   avg_order_value    = total_revenue / NULLIF(total_orders, 0)
--                       CAST to DECIMAL(18,2); NULL if total_orders = 0
--                       (cannot happen at this grain because zero-order
--                       products are omitted)
--
-- Join safety: aggregate qualifying orders first, then join canonical
-- product attributes (one row per product_id, min _ingest_row_id). A
-- duplicated product dimension cannot fan out measures.
--
-- Money: never convert to DOUBLE/FLOAT. Spark CAST to DECIMAL(18,2) uses
-- half-up rounding to scale 2.
--
-- Placeholders substituted by create_gold_tables.py:
--   {silver_schema} = qualified Silver schema (catalog.silver or silver)
--
-- Reconciliation:
--   SUM(total_revenue) = SUM(total_amount) of qualifying Silver orders
--   SUM(total_orders)   = COUNT(*) of qualifying Silver orders

WITH qualifying_orders AS (
  SELECT
    product_id,
    total_amount
  FROM {silver_schema}.orders
  WHERE order_status = 'Completed'
    AND quality_check_result = 'PASS'
    AND product_id IS NOT NULL
),
canonical_products AS (
  SELECT
    product_id,
    product_name,
    category
  FROM (
    SELECT
      product_id,
      product_name,
      category,
      ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY _ingest_row_id
      ) AS _rn
    FROM {silver_schema}.products
  ) ranked
  WHERE _rn = 1
),
aggregated AS (
  SELECT
    product_id,
    CAST(COUNT(*) AS BIGINT) AS total_orders,
    CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue
  FROM qualifying_orders
  GROUP BY product_id
)
SELECT
  a.product_id,
  p.product_name,
  p.category,
  a.total_orders,
  a.total_revenue,
  CAST(
    CAST(a.total_revenue AS DECIMAL(18, 2))
    / CAST(NULLIF(a.total_orders, 0) AS DECIMAL(18, 2))
    AS DECIMAL(18, 2)
  ) AS avg_order_value
FROM aggregated a
INNER JOIN canonical_products p
  ON a.product_id = p.product_id
