# Agent guide — dbt and analytical modeling

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

Read this guide before working on: dbt models in any layer, dimensional
modeling (facts, dimensions, SCD), marts, or analytical SQL style.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md sections 20–21 and 44. Section numbers are
preserved so existing references of the form "AGENTS §N" keep resolving.
General naming conventions for SQL objects and identifiers remain in the
`AGENTS.md` core (§43).

---

## 20. dbt rules

dbt owns relational analytical transformations.

Expected model layers:

```text
staging
intermediate
core
marts
```

### 20.1 Staging

Responsibilities:

- source renaming;
- basic casting;
- standard naming;
- minimal cleanup.

Do not place large business logic here.

### 20.2 Intermediate

Responsibilities:

- deduplication;
- joins;
- normalization;
- reusable business preparation.

### 20.3 Core

Responsibilities:

- facts;
- dimensions;
- conformed entities;
- SCD logic.

### 20.4 Marts

Responsibilities:

- business-facing datasets;
- KPI-ready structures;
- BI-oriented grain.

### 20.5 Model contracts

Every important model must document:

- purpose;
- grain;
- primary key or uniqueness expectation;
- important dimensions;
- important measures;
- upstream dependencies.

### 20.6 dbt tests

Use built-in tests where appropriate:

- `unique`;
- `not_null`;
- `relationships`;
- `accepted_values`.

Add custom business tests such as:

- non-negative financial values;
- order/payment reconciliation;
- valid temporal ordering;
- uniqueness at declared grain.

A failing business-critical test should fail the relevant pipeline unless the expected policy explicitly quarantines the data.

---

## 21. Analytical modeling rules

The primary Gold model is Kimball-style.

Expected examples:

Dimensions:

- `dim_customer`;
- `dim_product`;
- `dim_date`;
- `dim_campaign`.

Facts:

- `fact_orders`;
- `fact_order_items`;
- `fact_payments`;
- `fact_shipments`.

Marts:

- `mart_daily_sales`;
- `mart_customer_ltv`;
- `mart_marketing_roi`;
- `mart_delivery_performance`.

Every fact table must define its grain explicitly.

Do not mix grains in one fact table without a documented reason.

### 21.1 SCD Type 2

`dim_customer` should demonstrate SCD Type 2.

Expected concepts:

- surrogate key;
- business key;
- valid-from;
- valid-to;
- current-row indicator.

Tests should verify:

- no overlapping validity intervals;
- one current row per business key;
- historical versions are retained.

### 21.2 Alternate modeling domains

Data Vault is not implemented and is outside this repository's final scope.
Do not introduce it through routine maintenance or replace the delivered Gold
Kimball model with it. An alternate modeling domain requires a new ADR that
explicitly supersedes ADR 0009.

---

## 44. SQL style

Prefer readable SQL.

Example:

```sql
select
    order_id,
    customer_id,
    order_total,
    created_at
from {{ ref('stg_orders') }}
where is_deleted = false
```

Guidelines:

- one selected column per line for non-trivial queries;
- explicit aliases;
- meaningful CTE names;
- avoid deeply nested subqueries when CTEs improve readability;
- document non-obvious business logic;
- avoid dialect-specific tricks unless justified;
- do not use ordinal `GROUP BY 1,2,3` in persistent models.

Every analytical model must have a clear grain.
