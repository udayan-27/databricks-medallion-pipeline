# DE C1 Coding Evaluation — Working Requirements

Source: Participant Guide — Build & Grow Your AI Workflow (16 pages).

## Objective
Build a complete Databricks Medallion Architecture pipeline for e-commerce sales:
CSV/S3/DBFS -> Bronze -> Silver -> Gold -> Dashboard.

The exercise evaluates not only whether the pipeline works, but also:
- requirement understanding
- AI prompting/context-setting
- AI-assisted design and implementation
- validation/testing
- debugging
- data quality thinking
- data contracts/schema thinking
- documentation/ownership
- responsible AI
- reflection

## Mandatory Source Data

### customers.csv
10,000 rows; fields:
- customer_id INT, primary key
- customer_name STRING
- email STRING
- country STRING
- signup_date DATE
- customer_segment STRING: Premium/Standard/Basic
- lifetime_value DECIMAL

### orders.csv
100,000 rows; fields:
- order_id INT, primary key
- customer_id INT, FK -> customers
- order_date DATE
- product_id INT, FK -> products
- quantity INT
- unit_price DECIMAL
- total_amount DECIMAL
- order_status STRING: Pending/Completed/Cancelled
- payment_date DATE nullable

### products.csv
500 rows; fields:
- product_id INT, primary key
- product_name STRING
- category STRING
- price DECIMAL
- cost DECIMAL
- stock_quantity INT
- reorder_level INT

## Intentional Data Quality Issues

Customers:
- 50 NULL email rows
- 10 duplicate customer_id rows

Orders:
- 100 NULL customer_id rows
- 200 NULL product_id rows
- 50 customer_id values absent from customers
- 30 product_id values absent from products
- 20 duplicate order_id rows

The guide calls this approximately 700 problematic rows / 0.7%, but the individually listed issues do not mathematically total 700. The prompt workflow must record this as an explicit requirement ambiguity rather than silently changing the source.

The guide's prompt example also mentions 30 future signup dates. Treat this as an additional optional/explicitly documented business-logic check rather than changing the mandatory counts without documenting the decision.

## Bronze
- Read CSVs from S3/DBFS into Databricks
- Create Bronze tables
- Raw/unchanged data; no cleaning/transformation
- Handle schema inference and types
- Log ingestion metadata including row counts and timestamp

## Silver
The core acceptance criteria repeatedly refer to four quality checks. The repository structure explicitly includes five modules:
1. completeness
2. uniqueness
3. type validation
4. referential integrity
5. business logic

Implement all five so the repository satisfies the explicit structure and exceeds the minimum core interpretation.

Requirements:
- completeness: no NULLs in critical fields (email, customer_id, product_id)
- uniqueness: duplicate order_id and customer_id handling
- referential integrity: customer_id and product_id exist in parent tables
- type validation: explicit/valid types and malformed values handled
- business logic: sensible rules such as quantity/amount/date/status/payment consistency, documented before implementation
- NEVER delete bad rows merely to make checks pass
- flag bad rows with quality_check_result (or a clearly documented equivalent)
- generate quality metrics with pass/fail counts and percentages

## Gold
Required business-ready aggregations:
A. Sales by Product
- product_id, product_name, category
- total_orders, total_revenue, avg_order_value

B. Revenue by Customer
- customer_id, customer_name, customer_segment
- total_orders, total_revenue, avg_order_value, lifetime_value_actual

C. Customer Segmentation
- segment_type: High-Value / Repeat / One-Time / Inactive
- customer_count, avg_revenue, total_revenue

Repository structure also calls for:
- daily/weekly trends SQL

## Dashboard
Databricks SQL Dashboard with 3+ tiles:
- Top 10 products by revenue (bar)
- Customer revenue distribution (histogram)
- Customer segmentation (pie)
Also configure filters.

## Required Repository Structure

databricks-medallion-pipeline/
- README.md
- candidate-info.md
- tool-workflow.md
- requirements-analysis.md
- design-notes.md
- data-model.md
- data-quality-strategy.md
- src/data_generation/generate_sample_data.py
- src/data_generation/DATA_GENERATION_NOTES.md
- src/bronze/01_ingest_customers.py
- src/bronze/02_ingest_orders.py
- src/bronze/03_ingest_products.py
- src/bronze/ingest_all.py
- src/silver/01_quality_completeness.py
- src/silver/02_quality_uniqueness.py
- src/silver/03_quality_type_validation.py
- src/silver/04_quality_referential_integrity.py
- src/silver/05_quality_business_logic.py
- src/silver/create_silver_tables.py
- src/gold/01_sales_by_product.sql
- src/gold/02_revenue_by_customer.sql
- src/gold/03_daily_weekly_trends.sql
- src/gold/04_customer_segmentation.sql
- src/gold/create_gold_tables.py
- src/dashboard/dashboard_queries.sql
- src/dashboard/DASHBOARD_GUIDE.md
- data/customers.csv
- data/orders.csv
- data/products.csv
- database/schema.sql
- database/seed-data-notes.md
- database/setup-notes.md
- debugging-notes.md
- reflection.md
- final-ai-usage-summary.md
- ai-prompts/data-generation.md
- ai-prompts/bronze-layer.md
- ai-prompts/silver-layer.md
- ai-prompts/gold-layer.md
- ai-prompts/dashboard.md
- ai-prompts/debugging.md
- ai-prompts/documentation.md

Cursor-specific:
- cursor-workflow/project-context.md
- cursor-workflow/spec.md
- cursor-workflow/cursor-rules-or-instructions.md
- cursor-workflow/task-breakdown.md

## Prompt-history requirements
For meaningful prompts, record:
- prompt text or summary
- AI response summary/key excerpt
- what was accepted and why
- what was changed and why
- what was rejected and why
- validation performed
- final decision

## Quality of AI Use
Strong:
- persistent project context
- detailed specifications
- specific prompts
- multiple refinement cycles
- validation before acceptance
- rejection of mismatched AI suggestions
- meaningful Git history

Weak:
- vague one-line prompts
- copying without understanding
- no testing
- no design/context
- shallow Git history
- no accept/reject reasoning

## Completion / Submission
Need:
- end-to-end working pipeline
- intentional quality issues
- all required quality checks
- Gold aggregations
- dashboard
- schema/setup and seed data
- README setup instructions
- meaningful tests
- full prompt history
- requirement/design/test artifacts
- debugging/code-review notes
- reflection

Submit a Git repository link using the required organizational account/email process, plus short written responses. No hosting/deployment is required.

## Responsible AI
Do not provide real customer PII, secrets, passwords, tokens, private production connection strings, or unnecessary sensitive information to AI tools. Synthetic/sample data should be used.

## Important Working Rule
Every major implementation change must:
1. be derived from the written requirements/spec;
2. be tested;
3. be reviewed against the requirements;
4. have the meaningful AI interaction captured in ai-prompts/;
5. be committed to Git with a descriptive message.
