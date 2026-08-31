-- Gold: Revenue by Customer
-- Status: IMPLEMENTED
-- Engine: Spark SQL / Databricks SQL
-- Populated by: src/gold/create_gold_tables.py
--
-- GOLD_OUTPUT: revenue_by_customer
-- Grain: one row per canonical customer_id (min _ingest_row_id per id).
-- Population: ALL canonical customers, including those with zero qualifying
-- orders (Inactive source). This is the documented design; do not silently
-- switch to "customers with orders only".
--
-- Eligibility (qualifying order) — frozen:
--   order_status = 'Completed'
--   AND quality_check_result = 'PASS'
--   AND customer_id IS NOT NULL
-- FAIL / Pending / Cancelled / duplicate-key copies do not contribute revenue.
-- Bronze/Silver retain those rows; Gold facts do not.
--
-- Duplicate customer_id profiles: Silver flags every copy uniqueness FAIL.
-- Gold still emits one dimension row for that id (canonical attributes) so a
-- real purchaser is not dropped because their profile was duplicated.
-- Customer-level FAIL (NULL email, future signup) does not exclude the id from
-- this table; order eligibility is independent.
--
-- Measures:
--   total_orders           = COUNT(*) of qualifying orders (0 if none)
--   total_revenue           = SUM(total_amount), DECIMAL(18,2); 0 if none
--   avg_order_value         = total_revenue / NULLIF(total_orders, 0)
--                             NULL when total_orders = 0 (no division by zero)
--   lifetime_value_actual   = that customer's total_revenue
--                             NOT source customers.lifetime_value
--
-- Join safety: aggregate orders first; left-join onto canonical customers so
-- zero-order customers remain and duplicate parent keys cannot fan out.
--
-- Money: DECIMAL(18,2) throughout; CAST after division (Spark half-up).
--
-- Placeholders: {silver_schema}
--
-- Reconciliation:
--   SUM(total_revenue) = SUM(total_amount) of qualifying Silver orders
--   SUM(total_orders)   = COUNT(*) of qualifying Silver orders
--   COUNT(*)             = distinct non-null Silver customer_id values

WITH qualifying_orders AS (
  SELECT
    customer_id,
    total_amount
  FROM {silver_schema}.orders
  WHERE order_status = 'Completed'
    AND quality_check_result = 'PASS'
    AND customer_id IS NOT NULL
),
canonical_customers AS (
  SELECT
    customer_id,
    customer_name,
    customer_segment
  FROM (
    SELECT
      customer_id,
      customer_name,
      customer_segment,
      ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY _ingest_row_id
      ) AS _rn
    FROM {silver_schema}.customers
    WHERE customer_id IS NOT NULL
  ) ranked
  WHERE _rn = 1
),
order_agg AS (
  SELECT
    customer_id,
    CAST(COUNT(*) AS BIGINT) AS total_orders,
    CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue
  FROM qualifying_orders
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_segment,
  CAST(COALESCE(a.total_orders, 0) AS BIGINT) AS total_orders,
  CAST(COALESCE(a.total_revenue, 0) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(
    CAST(COALESCE(a.total_revenue, 0) AS DECIMAL(18, 2))
    / CAST(NULLIF(COALESCE(a.total_orders, 0), 0) AS DECIMAL(18, 2))
    AS DECIMAL(18, 2)
  ) AS avg_order_value,
  CAST(COALESCE(a.total_revenue, 0) AS DECIMAL(18, 2)) AS lifetime_value_actual
FROM canonical_customers c
LEFT JOIN order_agg a
  ON c.customer_id = a.customer_id
