-- Gold: Customer Segmentation
-- Status: IMPLEMENTED
-- Engine: Spark SQL / Databricks SQL
-- Populated by: src/gold/create_gold_tables.py
--
-- GOLD_OUTPUT: customer_segmentation
-- Grain: exactly four rows, one per segment_type, including empty buckets
-- (customer_count = 0, total_revenue = 0, avg_revenue NULL).
--
-- Population: the same canonical customer set as gold.revenue_by_customer
-- (one row per distinct non-null customer_id). Recomputed from Silver so this
-- file is independently reviewable; it must not drift from 02's grain.
--
-- Eligibility for order measures — frozen:
--   order_status = 'Completed'
--   AND quality_check_result = 'PASS'
--   AND customer_id IS NOT NULL
--
-- Mutually exclusive priority (design-notes.md; do not overlap):
--   1. Inactive   — qualifying order count = 0
--   2. High-Value — qualifying revenue >= 1000.00
--                   (includes one-order and repeat customers at/above threshold)
--   3. Repeat     — qualifying order count >= 2 and not High-Value
--   4. One-Time    — qualifying order count = 1 and not High-Value
--
-- High-Value threshold: 1000.00 (DECIMAL(18,2)). Boundary 1000.00 is
-- High-Value; 999.99 is not. Do not change this constant unless data-model.md
-- records a documented revision after generation.
--
-- A customer with only FAIL / Pending / Cancelled orders is Inactive (zero
-- qualifying orders), not dropped.
--
-- Outputs:
--   segment_type    High-Value / Repeat / One-Time / Inactive
--   customer_count  canonical customers in the bucket
--   avg_revenue     total_revenue / NULLIF(customer_count, 0)
--   total_revenue   SUM of those customers' lifetime_value_actual (order revenue)
--
-- Placeholders: {silver_schema}
--
-- Reconciliation:
--   SUM(customer_count) = distinct canonical customer_id count
--   SUM(total_revenue)  = SUM of qualifying Silver order total_amount
--   High-Value ∩ Repeat ∩ One-Time ∩ Inactive = empty (CASE is exclusive)

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
    customer_id
  FROM (
    SELECT
      customer_id,
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
),
customer_metrics AS (
  SELECT
    c.customer_id,
    CAST(COALESCE(a.total_orders, 0) AS BIGINT) AS total_orders,
    CAST(COALESCE(a.total_revenue, 0) AS DECIMAL(18, 2)) AS total_revenue
  FROM canonical_customers c
  LEFT JOIN order_agg a
    ON c.customer_id = a.customer_id
),
labeled AS (
  SELECT
    customer_id,
    total_orders,
    total_revenue,
    CASE
      WHEN total_orders = 0 THEN 'Inactive'
      WHEN total_revenue >= CAST(1000.00 AS DECIMAL(18, 2)) THEN 'High-Value'
      WHEN total_orders >= 2 THEN 'Repeat'
      WHEN total_orders = 1 THEN 'One-Time'
    END AS segment_type
  FROM customer_metrics
),
segment_catalog AS (
  SELECT 'Inactive' AS segment_type, 1 AS segment_priority
  UNION ALL
  SELECT 'High-Value', 2
  UNION ALL
  SELECT 'Repeat', 3
  UNION ALL
  SELECT 'One-Time', 4
)
SELECT
  s.segment_type,
  CAST(COUNT(l.customer_id) AS BIGINT) AS customer_count,
  CAST(
    CAST(COALESCE(SUM(l.total_revenue), 0) AS DECIMAL(18, 2))
    / CAST(NULLIF(COUNT(l.customer_id), 0) AS DECIMAL(18, 2))
    AS DECIMAL(18, 2)
  ) AS avg_revenue,
  CAST(COALESCE(SUM(l.total_revenue), 0) AS DECIMAL(18, 2)) AS total_revenue
FROM segment_catalog s
LEFT JOIN labeled l
  ON s.segment_type = l.segment_type
GROUP BY s.segment_type, s.segment_priority
ORDER BY s.segment_priority
