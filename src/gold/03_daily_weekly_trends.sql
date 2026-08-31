-- Gold: Daily / weekly trends
-- Status: IMPLEMENTED
-- Engine: Spark SQL / Databricks SQL
-- Populated by: src/gold/create_gold_tables.py (two GOLD_OUTPUT statements)
--
-- Eligibility (qualifying order) — same frozen policy as other Gold facts:
--   order_status = 'Completed'
--   AND quality_check_result = 'PASS'
-- Dates are civil order_date values. No timezone conversion. Session TZ is
-- documented as UTC for local Spark; Databricks should use the same civil
-- dates already stored as DATE.
--
-- Week-start convention (data-model.md / requirements-analysis.md §6.16):
--   Monday. Spark date_trunc('WEEK', ...) truncates to Monday (ISO).
--   CAST back to DATE so the grain is a date, not a timestamp.
--   Tests assert Sunday 2026-08-30 → week_start 2026-08-24 and
--   Monday 2026-08-31 → week_start 2026-08-31.
--
-- Measures (both grains):
--   total_orders    = COUNT(*) of qualifying orders
--   total_revenue    = SUM(total_amount), DECIMAL(18,2)
--   avg_order_value  = total_revenue / NULLIF(total_orders, 0)
--
-- Grain:
--   daily_trends:  one row per distinct qualifying order_date
--                  (dates with no qualifying orders are omitted)
--   weekly_trends: one row per Monday week_start_date that has ≥1 qualifying order
--
-- Reconciliation:
--   SUM(daily total_revenue)  = SUM(weekly total_revenue)
--                             = SUM(qualifying Silver order total_amount)
--   SUM(daily total_orders)   = SUM(weekly total_orders)
--                             = COUNT(*) of qualifying Silver orders

-- GOLD_OUTPUT: daily_trends
WITH qualifying_orders AS (
  SELECT
    order_date,
    total_amount
  FROM {silver_schema}.orders
  WHERE order_status = 'Completed'
    AND quality_check_result = 'PASS'
    AND order_date IS NOT NULL
)
SELECT
  order_date AS trend_date,
  CAST(COUNT(*) AS BIGINT) AS total_orders,
  CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(
    CAST(SUM(total_amount) AS DECIMAL(18, 2))
    / CAST(NULLIF(COUNT(*), 0) AS DECIMAL(18, 2))
    AS DECIMAL(18, 2)
  ) AS avg_order_value
FROM qualifying_orders
GROUP BY order_date

-- GOLD_OUTPUT: weekly_trends
WITH qualifying_orders AS (
  SELECT
    CAST(date_trunc('WEEK', order_date) AS DATE) AS week_start_date,
    total_amount
  FROM {silver_schema}.orders
  WHERE order_status = 'Completed'
    AND quality_check_result = 'PASS'
    AND order_date IS NOT NULL
)
SELECT
  week_start_date,
  CAST(COUNT(*) AS BIGINT) AS total_orders,
  CAST(SUM(total_amount) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(
    CAST(SUM(total_amount) AS DECIMAL(18, 2))
    / CAST(NULLIF(COUNT(*), 0) AS DECIMAL(18, 2))
    AS DECIMAL(18, 2)
  ) AS avg_order_value
FROM qualifying_orders
GROUP BY week_start_date
