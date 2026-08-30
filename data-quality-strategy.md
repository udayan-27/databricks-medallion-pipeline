# Data quality strategy

Status: **strategy frozen for implementation**. No quality code has been run. No metrics have been produced. Do not delete bad rows.

This file is the source of truth for Silver modules. Implementation must copy these rules, not invent new ones silently.

## Principles

1. Bronze stays raw. Source defects remain visible there.
2. Silver never deletes rows just to pass a check.
3. Every row is flagged: per-module booleans, `failed_checks`, and `quality_check_result`.
4. Metrics report pass/fail counts and percentages per check and per table.
5. Distinct failing rows and issue-instance counts are both reported when they differ.
6. Mandatory injected issues follow `requirements-analysis.md` (listed counts, not a padded 700).
7. Multiple failures **accumulate**. No module overwrites another module’s codes.

## Failure class dictionary

These classes must stay distinct in `failed_checks` codes and in metrics.

| Class | Meaning | Module | Example code |
|---|---|---|---|
| NULL foreign key | FK column is SQL NULL | Completeness | `completeness:orders.customer_id` |
| Orphan foreign key | FK is non-null and not in the parent key set | Referential integrity | `ri:orders.customer_id_orphan` |
| Duplicate key | Natural key appears on more than one row | Uniqueness | `uniqueness:orders.order_id` |
| Malformed value | Value does not match declared type or closed enum | Type validation | `type:orders.order_status_domain` |
| Business-rule violation | Cross-field or policy rule on otherwise typed values | Business logic | `business:orders.quantity_positive` |

NULL PK (`customers.customer_id`, `orders.order_id`, `products.product_id`) is completeness (`completeness:<table>.<pk>`), not uniqueness. Uniqueness only evaluates keys that are non-null (a NULL key is not “duplicated” with other NULLs).

## How multiple failed checks are represented

Each module outputs `_ingest_row_id`, a boolean, and an array of codes for **that module only**.

`create_silver_tables.py` concatenates the five arrays:

```
failed_checks = completeness_codes ∪ uniqueness_codes ∪ type_codes ∪ ri_codes ∪ business_codes
quality_check_result = FAIL if failed_checks is non-empty else PASS
```

Union is concatenation of arrays, then `array_distinct` if a module accidentally emits the same code twice. Modules must not emit codes owned by another module.

A single order row can carry, at the same time:

- `completeness:orders.customer_id` (NULL FK)
- `type:orders.order_status_domain` (illegal status)
- `business:orders.amount_equals_qty_price` (amount mismatch)

Gold’s default filter uses the **combined** `quality_check_result`, not a single module column. Metrics still break out each module so the 100 NULL `customer_id` rows remain visible even if some of those rows also fail business logic (generation will try to keep mandatory classes disjoint; overlap is still allowed by the combiner).

## Thresholds (assignment meaning)

These are **detection targets**, not job-fail SLOs. The pipeline job succeeds when checks run and write flags. Tests fail if detected counts diverge from the contract.

| Check | Table | Expected fail_count after generation (mandatory) | Notes |
|---|---|---|---|
| completeness email | customers | 50 | Exact |
| uniqueness customer_id | customers | 20 rows (10 ids × 2) if both copies flagged | All copies FAIL; 10 extra rows ⇒ 20 rows with duplicated ids |
| completeness customer_id | orders | 100 | Exact |
| completeness product_id | orders | 200 | Exact |
| uniqueness order_id | orders | 40 rows (20 ids × 2) if both copies flagged | Same interpretation as customers |
| RI customer orphan | orders | 50 | Non-null only |
| RI product orphan | orders | 30 | Non-null only |
| type / business | all | 0 from mandatory list unless extra defects injected | May be > 0 if optional future signups or accidental generator bugs |

Percentages: `fail_pct = fail_count / table_row_count`. Do not “fix” data to hit 700 failing rows. Listed issue instances total **460**; distinct FAIL rows will differ because each duplicate key flags **two** rows (10+20 extra rows plus their originals).

Uniqueness expected rows: 10 duplicate *customer* extras mean 10 ids have two rows → **20** uniqueness FAIL rows on customers, not 10. Same for orders: **40** uniqueness FAIL rows. Document this in generation notes so tests assert 20/40 **rows**, matching “flag all copies,” while issue-instance counts remain 10 and 20 **extra rows**.

---

## 1. Completeness (`01_quality_completeness.py`)

### Fields

| Table | Field | Rule |
|---|---|---|
| customers | email | IS NOT NULL |
| customers | customer_id | IS NOT NULL |
| orders | customer_id | IS NOT NULL |
| orders | product_id | IS NOT NULL |
| orders | order_id | IS NOT NULL |
| products | product_id | IS NOT NULL |

Guide-named critical fields are email, order `customer_id`, order `product_id`. PK null checks are the documented extension in `requirements-analysis.md` assumption 11.

Not completeness (owned elsewhere): `payment_date` NULL, invalid enums, orphans.

### Detection method

Row-level Spark `IS NULL` on the typed Bronze column. After PERMISSIVE ingest, unparsable values are also null and fail completeness if the field is in this list.

### Row-level result

- `completeness_pass = false` if any listed field on that row is null.
- Append one code per null field, e.g. `completeness:orders.customer_id`.

### Aggregate metrics

Per table, per field, plus module rollup `completeness` (row fails the module if any field failed).

### Thresholds

Match injected NULL counts: 50 emails, 100 order customer_id, 200 order product_id. PK nulls expected 0 unless generator bugs.

### Intentional failure scenarios

- 50 NULL emails
- 100 NULL order customer_id
- 200 NULL order product_id

### Test cases

- Count `email IS NULL` in Silver = 50 and those rows have `completeness:customers.email`.
- NULL FK rows have completeness FAIL and **do not** require RI FAIL.
- A fully populated valid row has `completeness_pass = true`.

### Edge cases

- Empty string `""` is **not** NULL. Completeness passes; type/business may still fail if we later add “non-blank email” (not in this strategy). Do not treat `""` as NULL.
- Duplicate rows with a populated email still pass completeness.

### False-positive risks

- Counting empty string as NULL.
- Counting Spark `NaN` on decimals as completeness of email (wrong field).
- Using `COALESCE` that hides nulls before the check.

---

## 2. Uniqueness (`02_quality_uniqueness.py`)

### Fields

| Table | Field |
|---|---|
| customers | customer_id |
| orders | order_id |
| products | product_id |

### Detection method

Window `COUNT(*) OVER (PARTITION BY key)` for non-null keys. `uniqueness_pass = false` when count > 1.

NULL keys: skip uniqueness (completeness already failed). Do not group all NULLs as one duplicate key.

### Row-level result

Every row in a duplicated key group fails. Codes: `uniqueness:customers.customer_id`, `uniqueness:orders.order_id`, `uniqueness:products.product_id`.

No canonical survivor in Silver.

### Aggregate metrics

- fail_count = rows with uniqueness_pass false
- optional metric `duplicate_key_count` = number of distinct keys with count > 1 (10 and 20)

### Thresholds

Customers: 10 duplicate keys, 20 FAIL rows. Orders: 20 duplicate keys, 40 FAIL rows. Products: 0.

### Intentional failure scenarios

- 10 extra customer rows reusing an existing id
- 20 extra order rows reusing an existing id

### Test cases

- Distinct duplicated customer_ids = 10.
- Rows with those ids all have uniqueness FAIL.
- Unique ids have uniqueness PASS.
- Two NULL order_ids (if ever present) are not flagged as duplicates of each other.

### Edge cases

- Duplicate key plus NULL email: both uniqueness and completeness codes present.
- Duplicate extra row that is an otherwise perfect copy: uniqueness only.

### False-positive risks

- Flagging only the “second” copy and letting Gold count the first (inconsistent with this strategy).
- Partitioning by a non-key column.
- Joining uniqueness results back on `order_id` and exploding the combiner.

---

## 3. Type validation (`03_quality_type_validation.py`)

### Fields

Typed Bronze columns are already Spark types. This module catches:

- Closed-domain strings not in the allowlist
- Nulls on fields that must be typed values for numeric/date **business columns that are not completeness-critical**, only if they indicate parse failure — **decision: do not double-count completeness-critical nulls as type failures**
- Values that are non-null but impossible for the type at CSV level (should already be null after PERMISSIVE)

Closed domains:

| Field | Allowlist |
|---|---|
| customer_segment | Premium, Standard, Basic |
| order_status | Pending, Completed, Cancelled |

Also fail type if a non-null DATE/INT/DECIMAL column received a value Spark could not parse (column is null **and** the field is not in the completeness list). Example: malformed `unit_price` → null, code `type:orders.unit_price`. Completeness does not list `unit_price`, so type owns it.

If extra malformed tokens are **not** injected, expected type fail_count is 0 aside from accidental generator bugs.

Country, category, names, email format: **not** type-checked against an allowlist or RFC.

### Detection method

- Domain: `column IS NOT NULL AND column NOT IN (allowlist)` (case-sensitive exact match).
- Parse-null on non-completeness typed fields: `IS NULL` on quantity, unit_price, total_amount, order_date, payment_date (payment_date NULL is **allowed** — do **not** type-fail legitimate NULL payment_date).

`payment_date` NULL is valid. Type-fail `payment_date` only if we later keep a raw string column (we will not). No extra raw string columns.

Numeric overflow beyond INT/DECIMAL(18,2): Spark may null or throw; treat resulting null on required numeric fields (`quantity`, `unit_price`, `total_amount`, `price`, `cost`, `stock_quantity`, `reorder_level`, `lifetime_value`) as `type:<table>.<field>`.

### Row-level result

`type_validation_pass = false` if any type rule failed. Codes prefixed `type:`.

### Aggregate metrics

Per field + module rollup.

### Thresholds

Expected 0 on mandatory data. Optional malformed injection must be documented first.

### Intentional failure scenarios

None mandatory. Do not invent bad enums just to give this module work.

### Test cases

- Fixture row with `order_status = 'Shipped'` → type FAIL, completeness may PASS.
- `customer_segment = 'premium'` (wrong case) → type FAIL.
- Valid enums → type PASS.
- NULL `payment_date` on Pending → type PASS.

### Edge cases

- Leading/trailing spaces (`'Completed '`) fail domain (not trimmed; trimming would be cleaning).
- Completeness-critical NULL email is **not** a type failure.

### False-positive risks

- Treating NULL payment_date as malformed.
- Double-counting NULL customer_id as type and completeness.
- Locale decimal commas if the generator emits them (generator must emit `.` decimals).

---

## 4. Referential integrity (`04_quality_referential_integrity.py`)

### Fields

| Child | Field | Parent |
|---|---|---|
| orders | customer_id | bronze.customers.customer_id |
| orders | product_id | bronze.products.product_id |

Customers and products have no FKs: every row `referential_integrity_pass = true`, metric 100% pass.

### Detection method

Build distinct parent key sets from **Bronze** (all non-null parent ids, including duplicates).

Anti-join children where `fk IS NOT NULL AND fk NOT IN parent_set`.

NULL FKs: **RI pass** for that FK. If the other FK is orphan, the row can still RI-fail on the other field.

### Row-level result

Codes: `ri:orders.customer_id_orphan`, `ri:orders.product_id_orphan`.
`referential_integrity_pass = false` if either orphan code is present.

### Aggregate metrics

Separate fail_count for each orphan code. Tests assert 50 and 30, **not** 150 or 230.

### Thresholds

50 customer orphans, 30 product orphans.

### Intentional failure scenarios

- 50 orders with non-null customer_id outside the 10,000 real ids
- 30 orders with non-null product_id outside the 500 real ids

Generation must not use NULL for these rows.

### Test cases

- NULL customer_id row: `ri:orders.customer_id_orphan` absent; completeness code present.
- Orphan customer_id: RI FAIL; completeness PASS for that field.
- Valid FK: RI PASS even if the parent email is NULL.
- Duplicate parent id: child still RI PASS.

### Edge cases

- Orphan customer_id and NULL product_id on the same row (generation should avoid; combiner still supports both codes).
- Type mismatch prevented by explicit INT schema.

### False-positive risks

- `NOT IN` with a parent set that contains NULL (SQL three-valued logic). Use `LEFT ANTI JOIN` on non-null keys or filter parent nulls out of the set.
- Counting NULL FKs as orphans.
- Looking up Silver parents that were filtered to PASS only, dropping duplicate ids incorrectly (existence should use Bronze distinct ids).

---

## 5. Business logic (`05_quality_business_logic.py`)

Rules apply to **typed values**. If a field is null, skip rules that need that field (completeness/type already failed). Do not also emit business codes for “quantity null” — that is type/completeness.

### Fields and rules (frozen)

| Code | Rule | Tables |
|---|---|---|
| `business:orders.quantity_positive` | `quantity > 0` when quantity is not null | orders |
| `business:orders.unit_price_non_negative` | `unit_price >= 0` when not null | orders |
| `business:orders.total_amount_non_negative` | `total_amount >= 0` when not null | orders |
| `business:orders.amount_equals_qty_price` | `abs(total_amount - quantity * unit_price) <= 0.01` when all three not null | orders |
| `business:orders.completed_has_payment` | if `order_status = 'Completed'` then `payment_date IS NOT NULL` | orders |
| `business:orders.cancelled_without_payment` | if `order_status = 'Cancelled'` then `payment_date IS NULL` | orders |
| `business:orders.payment_on_or_after_order` | if both dates not null then `payment_date >= order_date` | orders |
| `business:orders.order_not_before_signup` | if customer exists and both dates not null then `order_date >= signup_date` | orders + customers |
| `business:customers.signup_not_future` | `signup_date <= as_of_date` when signup_date not null | customers |
| `business:customers.lifetime_value_non_negative` | `lifetime_value >= 0` when not null | customers |
| `business:products.price_non_negative` | `price >= 0` when not null | products |
| `business:products.cost_non_negative` | `cost >= 0` when not null | products |
| `business:products.stock_non_negative` | `stock_quantity >= 0` when not null | products |
| `business:products.reorder_non_negative` | `reorder_level >= 0` when not null | products |

**As-of date:** frozen at generation/implementation in one place (planned `2026-08-31`). Not `current_date()` at job time, which would make historical signups “future” after a later rerun or flip optional defects on and off.

Enums are type, not business.

Pending orders: `payment_date` may be null or set; no extra pending rule.

### Detection method

Spark column predicates; order-vs-signup uses a lookup to canonical/min-row parent by `customer_id` (Bronze). If `customer_id` is NULL or orphan, skip `order_not_before_signup` (cannot evaluate).

### Row-level result

One code per failed rule. `business_logic_pass = false` if any business code present.

### Aggregate metrics

Per rule + module rollup.

### Thresholds

Mandatory injected list does not include business defects. Expected 0 unless:

- Optional 30 future signup dates are injected, then `business:customers.signup_not_future` = 30.
- Generator accidentally violates amount = qty * price.

### Intentional failure scenarios

- Optional: 30 future `signup_date` values (example prompt). Decision at Stage 2.
- No mandatory quantity/amount sabotage.

### Test cases

- quantity = 0 → quantity_positive FAIL.
- Completed and payment_date NULL → completed_has_payment FAIL.
- Cancelled with payment_date set → cancelled_without_payment FAIL.
- payment_date before order_date → payment_on_or_after_order FAIL.
- signup_date after as_of_date → signup_not_future FAIL.
- amount 20.00 vs 2 * 10.00 → amount rule PASS.
- amount 20.02 vs 2 * 10.00 → amount rule FAIL (outside 0.01).
- NULL customer_id order does not get order_not_before_signup.

### Edge cases

- Duplicate customer profiles with different signup_dates: use `min(_ingest_row_id)` parent for the lookup; document if they disagree.
- DECIMAL multiplication producing 19.999… : tolerance 0.01.
- Timezone-free dates: equal dates pass `payment_date >= order_date`.

### False-positive risks

- Using `current_date()` for future signup.
- Emitting business codes for NULL quantity.
- Inner-joining customers and dropping NULL FK orders from Silver (forbidden; skip the rule instead).
- Treating Pending + NULL payment as a failure.

---

## Metrics table

For each check and table:

- pass_count
- fail_count
- pass_pct
- fail_pct
- computed_at

Also table-level `quality_check_result` FAIL distinct rows.

Do not “fix” source data to hit 700 failing rows. Expected mandatory **issue instances** total **460**. Uniqueness **row** flags are higher (all copies). Distinct FAIL rows is a third number and must be reported honestly.

## Four-vs-five

Core narrative says four checks; the repo requires five modules. This strategy implements five. See `requirements-analysis.md` sections 6.2 and 14.

## What this strategy refuses

- Deleting FAIL rows
- Last-writer `quality_check_result`
- Classifying NULL FK as orphan
- Padding defects to 700
- Using Great Expectations or other platforms (out of scope)
