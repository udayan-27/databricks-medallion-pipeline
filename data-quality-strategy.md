# Data quality strategy

Status: strategy and intended checks. **No quality code has been run. No metrics have been produced.**

## Principles

1. Bronze stays raw. Source defects remain visible there.
2. Silver never deletes rows just to pass a check.
3. Every row is flagged: `quality_check_result` plus `failed_checks`.
4. Metrics report pass/fail counts and percentages per check and per table.
5. Distinct failing rows and issue-instance counts are both reported when they differ.
6. Mandatory injected issues follow `requirements-analysis.md` (listed counts, not a padded 700).

## Five Silver modules (all required)

### 1. Completeness (`01_quality_completeness.py`)

Critical fields named in the spec:

- customers.email must not be NULL
- orders.customer_id must not be NULL
- orders.product_id must not be NULL

Additional completeness rules, if added, will be listed here before coding.

### 2. Uniqueness (`02_quality_uniqueness.py`)

- customers.customer_id should be unique
- orders.order_id should be unique

Handling: retain all copies; flag duplicates. Canonical-row choice for Gold (e.g. first ingest order, or exclude all duplicate keys) will be documented in Gold SQL, not by deleting Silver rows.

### 3. Type validation (`03_quality_type_validation.py`)

- Values must match declared types (INT, DECIMAL, DATE, STRING domains).
- Malformed dates, non-numeric amounts, and illegal enum values fail this check.
- Extra malformed rows will not be injected unless data-generation notes say so.

### 4. Referential integrity (`04_quality_referential_integrity.py`)

- orders.customer_id, when present, must exist in customers.customer_id
- orders.product_id, when present, must exist in products.product_id
- NULL FKs are completeness failures; they may also be recorded as referential failures if the module treats NULL as “not found.” That classification will be explicit in the module docstring so metrics are not double-counted without explanation.

### 5. Business logic (`05_quality_business_logic.py`)

Rules will be documented here **before** implementation. Intended candidates (not yet coded or tested):

- quantity > 0
- unit_price >= 0 and total_amount >= 0
- total_amount equals quantity * unit_price (documented decimal tolerance)
- order_status in {Pending, Completed, Cancelled}
- customer_segment in {Premium, Standard, Basic}
- Completed orders should have payment_date
- Cancelled orders should not have a misleading completed payment pattern (exact rule TBD)
- payment_date, when present, should be on or after order_date
- signup_date should not be after the documented as-of date (covers optional future-signup check)
- order_date should not precede customer signup_date when the customer exists

These rules are not implemented in this commit.

## Metrics

For each check and table:

- pass_count
- fail_count
- pass_pct
- fail_pct
- computed_at

Do not “fix” source data to hit 700 failing rows. Expected injected issue instances (mandatory list) total **460**, which will not necessarily equal distinct FAIL rows.

## Four-vs-five

Core narrative says four checks; the repo requires five modules. This strategy implements five. See `requirements-analysis.md` sections 6.2 and 9.
