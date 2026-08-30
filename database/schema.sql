-- Planned Databricks / Spark SQL schema for the medallion pipeline.
-- Status: NOT APPLIED. These statements are the intended contract, not evidence of created tables.

-- CREATE SCHEMA IF NOT EXISTS bronze;
-- CREATE SCHEMA IF NOT EXISTS silver;
-- CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze (raw; columns match CSV headers)
-- CREATE TABLE bronze.customers (
--   customer_id INT,
--   customer_name STRING,
--   email STRING,
--   country STRING,
--   signup_date DATE,
--   customer_segment STRING,
--   lifetime_value DECIMAL(18, 2)
-- );

-- CREATE TABLE bronze.orders (
--   order_id INT,
--   customer_id INT,
--   order_date DATE,
--   product_id INT,
--   quantity INT,
--   unit_price DECIMAL(18, 2),
--   total_amount DECIMAL(18, 2),
--   order_status STRING,
--   payment_date DATE
-- );

-- CREATE TABLE bronze.products (
--   product_id INT,
--   product_name STRING,
--   category STRING,
--   price DECIMAL(18, 2),
--   cost DECIMAL(18, 2),
--   stock_quantity INT,
--   reorder_level INT
-- );

-- CREATE TABLE bronze.ingest_metadata (
--   source_file STRING,
--   row_count BIGINT,
--   ingested_at TIMESTAMP,
--   ingest_id STRING
-- );

-- Silver tables copy Bronze columns and add quality_check_result and related flags.
-- Exact DDL will be updated when create_silver_tables.py is implemented.

-- Gold tables/views match src/gold/*.sql outputs.
-- Exact DDL will be updated when Gold SQL is implemented.
