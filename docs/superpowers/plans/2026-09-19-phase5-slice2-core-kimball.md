# Phase 5 / Slice 2 — API Staging + Intermediate + Core Kimball + dbt Tests + Data-Model Docs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the analytical core of the lakehouse: stage the three API bronze tables, derive current-state intermediate views, model the Kimball gold layer (4 dimensions incl. SCD2 `dim_customer`, 4 facts), cover it with built-in and singular business tests (incl. orders↔payments reconciliation), document the analytical model, and prove the full `dbt build` on a seeded live stack.

**Architecture:** Pure dbt over the existing bronze sources (Phase 5 slice 1). Staging/intermediate are `silver` views (cheap at laptop scale, debuggable); core dims/facts are `gold` tables rebuilt fully on every run — deterministic, no incremental state (spec §5, AGENTS §30). SCD2 is an explicit model over the append-only bronze history using `_batch_date` as the version grain, exactly as decided in the approved spec (§2 "SCD2 dim_customer": explicit model, not dbt snapshots).

**Tech Stack:** dbt-core 1.10 + dbt-trino 1.9 (already pinned); Trino 483 + Iceberg (Polaris); pytest for the integration harness.

**Spec:** `docs/superpowers/specs/2026-09-18-phase5-dbt-lakehouse-design.md` — §5 (dbt project), §9 (testing), §10 (slice 2), §12 (acceptance criteria mapping). The plan argues from the spec; executors read both.

## Verified Trino facts (probed live on 483 before writing this plan)

- `sequence(min_date, max_date, interval '1' day)` + `unnest(...) as t(d)` with date bounds works; `cast(d as date)` needed (returns timestamps); null bounds yield an empty set (safe on empty bronze).
- `date_format(cast(<date> as timestamp), '%Y%m%d'|'%M'|'%W')` → int-formattable key, month name, weekday name.
- `dow(<date>)` → 1 (Monday) .. 7 (Sunday); `dow in (6,7)` = weekend.
- `<date> - interval '1' day` cast back to date gives the previous day.
- `substr(cast(<date> as varchar), 1, 10)` → `'yyyy-mm-dd'` (used for the readable SCD2 surrogate key).
- Value domains for accepted_values tests: orders `pending/paid/shipped/delivered/cancelled/refunded`; payments `pending/authorized/captured/failed/refunded/cancelled`; shipments `pending/in_transit/delivered/cancelled`; customers status `active/inactive/churned`, segment `standard/premium/vip`; campaigns status `active/paused/completed/scheduled`, channel `search/display/social/email/video` (from `infrastructure/mock_api/src/mock_api/data.py` and `docs/data-model.md`).

## Global Constraints

- Python 3.12; uv only; no new Python dependencies in this slice (dbt already pinned).
- ruff `E,F,I,UP,B,SIM`, line 100; `mypy --strict` — no new Python source in this slice except the integration test (fully typed, follows `tests/integration/test_bronze_load.py` patterns).
- Branch `feature/phase5-slice2-core-kimball` off `main`; Conventional Commits (`feat(dbt):`, `test(dbt):`, `test(integration):`, `docs:`); separate PR from the infra follow-ups.
- Layer schemas via `dbt_project.yml` + the committed `generate_schema_name` macro: staging/intermediate → `silver`, core → `gold`. Marts (slice 3) → `analytics` — NOT in this slice.
- No SQL business transformations inside Airflow DAGs (none touched here); no changes to the bronze loader.
- Every model gets a `-- Grain:` header comment; every core model additionally gets a YAML contract (purpose/grain/PK/measures/upstream) — AGENTS §20.5, §44.
- Offline `dbt parse` after every model task; live `dbt build` validation happens in the integration task (requires `make up`).
- Deterministic SQL only: no wall-clock `now()`, no random; ordering ties broken explicitly.

---

### Task 1: staging models for the three API sources

**Files:**
- Modify: `dbt/models/staging/sources.yml`
- Create: `dbt/models/staging/stg_fx_rates.sql`
- Create: `dbt/models/staging/stg_campaigns.sql`
- Create: `dbt/models/staging/stg_deliveries.sql`

**Interfaces:**
- Produces: staging views over `bronze.fx_rates`, `bronze.campaigns`, `bronze.deliveries` with the same explicit-column pattern as the seven OLTP staging models (business columns + `_batch_id`, `_batch_date`, `_source_object`, `_ingested_at`).

- [ ] **Step 1: Register the API sources**

In `dbt/models/staging/sources.yml`, extend the `tables:` list of source `bronze` with:

```yaml
      - name: fx_rates
      - name: campaigns
      - name: deliveries
```

- [ ] **Step 2: Create the staging views**

`dbt/models/staging/stg_fx_rates.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per (currency, raw API page) of one logical batch.
select
    currency,
    rate,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'fx_rates') }}
```

`dbt/models/staging/stg_campaigns.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per campaign per raw API page (daily campaign snapshots).
select
    campaign_id,
    name,
    channel,
    status,
    start_date,
    end_date,
    budget_eur,
    spend_eur,
    impressions,
    clicks,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'campaigns') }}
```

`dbt/models/staging/stg_deliveries.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per carrier delivery record per raw API page.
-- NOTE: `order_id` is the delivery partner's synthetic reference ("ORD-…")
-- and intentionally does NOT join the OLTP orders (spec §2); this feed
-- serves mart_delivery_performance only.
select
    delivery_id,
    order_id,
    carrier,
    status,
    shipped_at,
    delivered_at,
    updated_at,
    _batch_id,
    _batch_date,
    _source_object,
    _ingested_at
from {{ source('bronze', 'deliveries') }}
```

- [ ] **Step 3: Offline parse**

```bash
uv run dbt parse --project-dir dbt --profiles-dir dbt
```
Expected: no errors; the ten staging models resolve.

- [ ] **Step 4: Commit**

```bash
git add dbt/models/staging
git commit -m "feat(dbt): staging views for fx, campaigns, deliveries bronze sources (Phase 5 slice 2)"
```

---

### Task 2: intermediate current-state views

**Files:**
- Modify: `dbt/dbt_project.yml`
- Create: `dbt/models/intermediate/` — `int_categories.sql`, `int_products.sql`, `int_customers.sql`, `int_orders.sql`, `int_order_items.sql`, `int_payments.sql`, `int_shipments.sql`, `int_campaigns.sql`, `int_deliveries.sql`, `int_fx_rates_daily.sql`

**Interfaces:**
- Consumes: all ten `stg_*` models.
- Produces: one current row per business entity (columns WITHOUT the bronze service columns, except `int_fx_rates_daily` which exposes `rate_date`); `int_deliveries` keeps the partner `order_id`.

- [ ] **Step 1: Configure the layer**

In `dbt/dbt_project.yml`, extend the `models: omni_retail:` block to:

```yaml
models:
  omni_retail:
    staging:
      +materialized: view
      +schema: silver
    intermediate:
      +materialized: view
      +schema: silver
```

- [ ] **Step 2: Create the entity current-state views**

Shared pattern (documented once here; repeat it concretely in every file): rank every bronze row of the entity by `(_batch_date desc, _ingested_at desc)` within the business key and keep rank 1. Incremental snapshot batches contain only changed rows, so the surviving row is the latest known version. Hard deletes are invisible to snapshot extraction and close only with CDC (Phase 8) — a deleted source row keeps its last version by design.

`dbt/models/intermediate/int_categories.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per category (latest known version).
-- Hard deletes are invisible to snapshot extraction until CDC (Phase 8).
with ranked as (
    select
        category_id,
        name,
        parent_category_id,
        created_at,
        updated_at,
        row_number() over (
            partition by category_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_categories') }}
)
select
    category_id,
    name,
    parent_category_id,
    created_at,
    updated_at
from ranked
where version_rank = 1
```

`dbt/models/intermediate/int_products.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per product (latest known version).
with ranked as (
    select
        product_id,
        sku,
        name,
        category_id,
        brand,
        unit_price,
        unit_cost,
        is_active,
        created_at,
        updated_at,
        row_number() over (
            partition by product_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_products') }}
)
select
    product_id,
    sku,
    name,
    category_id,
    brand,
    unit_price,
    unit_cost,
    is_active,
    created_at,
    updated_at
from ranked
where version_rank = 1
```

`dbt/models/intermediate/int_customers.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per customer (latest known version).
with ranked as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        region,
        city,
        status,
        segment,
        registered_at,
        created_at,
        updated_at,
        row_number() over (
            partition by customer_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_customers') }}
)
select
    customer_id,
    email,
    first_name,
    last_name,
    region,
    city,
    status,
    segment,
    registered_at,
    created_at,
    updated_at
from ranked
where version_rank = 1
```

`dbt/models/intermediate/int_orders.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per order (latest known version).
with ranked as (
    select
        order_id,
        customer_id,
        status,
        currency,
        shipping_cost,
        order_total,
        created_at,
        updated_at,
        row_number() over (
            partition by order_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_orders') }}
)
select
    order_id,
    customer_id,
    status,
    currency,
    shipping_cost,
    order_total,
    created_at,
    updated_at
from ranked
where version_rank = 1
```

`dbt/models/intermediate/int_order_items.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per order line (lines are immutable after creation).
with ranked as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        created_at,
        updated_at,
        row_number() over (
            partition by order_item_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_order_items') }}
)
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    created_at
from ranked
where version_rank = 1
```

`dbt/models/intermediate/int_payments.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per payment (latest known version; status changes over time).
with ranked as (
    select
        payment_id,
        order_id,
        method,
        status,
        amount,
        transaction_id,
        created_at,
        updated_at,
        row_number() over (
            partition by payment_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_payments') }}
)
select
    payment_id,
    order_id,
    method,
    status,
    amount,
    transaction_id,
    created_at
from ranked
where version_rank = 1
```

`dbt/models/intermediate/int_shipments.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per shipment (latest known version).
with ranked as (
    select
        shipment_id,
        order_id,
        carrier,
        tracking_number,
        status,
        shipped_at,
        delivered_at,
        created_at,
        updated_at,
        row_number() over (
            partition by shipment_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_shipments') }}
)
select
    shipment_id,
    order_id,
    carrier,
    tracking_number,
    status,
    shipped_at,
    delivered_at,
    created_at
from ranked
where version_rank = 1
```

`dbt/models/intermediate/int_campaigns.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per campaign (the API returns the full campaign list
-- daily; the latest logical batch wins).
with ranked as (
    select
        campaign_id,
        name,
        channel,
        status,
        start_date,
        end_date,
        budget_eur,
        spend_eur,
        impressions,
        clicks,
        row_number() over (
            partition by campaign_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_campaigns') }}
)
select
    campaign_id,
    name,
    channel,
    status,
    start_date,
    end_date,
    budget_eur,
    spend_eur,
    impressions,
    clicks
from ranked
where version_rank = 1
```

`dbt/models/intermediate/int_deliveries.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per carrier delivery (latest known version).
-- `order_id` is the delivery partner's synthetic reference (not OLTP).
with ranked as (
    select
        delivery_id,
        order_id,
        carrier,
        status,
        shipped_at,
        delivered_at,
        updated_at,
        row_number() over (
            partition by delivery_id
            order by _batch_date desc, _ingested_at desc
        ) as version_rank
    from {{ ref('stg_deliveries') }}
)
select
    delivery_id,
    order_id,
    carrier,
    status,
    shipped_at,
    delivered_at,
    updated_at
from ranked
where version_rank = 1
```

`dbt/models/intermediate/int_fx_rates_daily.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per (currency, calendar day): the latest page loaded for
-- that day wins (re-runs of a logical date replace the bronze partition).
with ranked as (
    select
        currency,
        _batch_date,
        rate,
        row_number() over (
            partition by currency, _batch_date
            order by _ingested_at desc
        ) as version_rank
    from {{ ref('stg_fx_rates') }}
)
select
    currency,
    _batch_date as rate_date,
    rate
from ranked
where version_rank = 1
```

- [ ] **Step 3: Offline parse**

```bash
uv run dbt parse --project-dir dbt --profiles-dir dbt
```
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add dbt/dbt_project.yml dbt/models/intermediate
git commit -m "feat(dbt): intermediate current-state views with keyset dedup (Phase 5 slice 2)"
```

---

### Task 3: dim_date, dim_product, dim_campaign

**Files:**
- Modify: `dbt/dbt_project.yml`
- Create: `dbt/models/core/dim_date.sql`
- Create: `dbt/models/core/dim_product.sql`
- Create: `dbt/models/core/dim_campaign.sql`

**Interfaces:**
- Consumes: `int_orders`, `int_products`, `int_categories`, `int_campaigns`.
- Produces: gold tables `dim_date` (PK `date_key`; spans created_at ∪ updated_at of orders), `dim_product` (PK `product_id`, enriched with category + parent category names), `dim_campaign` (PK `campaign_id`, renamed attributes).

- [ ] **Step 1: Configure the core layer**

In `dbt/dbt_project.yml`, add under `models: omni_retail:`:

```yaml
    core:
      +materialized: table
      +schema: gold
```

- [ ] **Step 2: Create the models**

`dbt/models/core/dim_date.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per calendar day covering the observed order-activity
-- window (created_at UNION updated_at). PK: date_key (yyyymmdd integer).
-- Upstream: int_orders (bounds only). Empty upstream yields an empty
-- dimension (null sequence bounds).
with activity as (
    select date(created_at) as activity_date
    from {{ ref('int_orders') }}
    union all
    select date(updated_at) as activity_date
    from {{ ref('int_orders') }}
),
bounds as (
    select
        min(activity_date) as min_date,
        max(activity_date) as max_date
    from activity
),
calendar as (
    select cast(day as date) as full_date
    from bounds
    cross join unnest(sequence(min_date, max_date, interval '1' day)) as days(day)
)
select
    cast(date_format(cast(full_date as timestamp), '%Y%m%d') as integer) as date_key,
    full_date,
    year(full_date) as year_number,
    quarter(full_date) as quarter_number,
    month(full_date) as month_number,
    date_format(cast(full_date as timestamp), '%M') as month_name,
    day(full_date) as day_number,
    dow(full_date) as day_of_week,
    date_format(cast(full_date as timestamp), '%W') as day_name,
    dow(full_date) in (6, 7) as is_weekend
from calendar
where full_date is not null
```

`dbt/models/core/dim_product.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per product. SCD Type 1 (current state; natural key).
-- Upstream: int_products, int_categories (self-referencing hierarchy).
select
    p.product_id,
    p.sku,
    p.name as product_name,
    p.brand,
    p.unit_price,
    p.unit_cost,
    p.is_active,
    p.category_id,
    c.name as category_name,
    c.parent_category_id,
    pc.name as parent_category_name
from {{ ref('int_products') }} as p
join {{ ref('int_categories') }} as c
    on c.category_id = p.category_id
left join {{ ref('int_categories') }} as pc
    on pc.category_id = c.parent_category_id
```

`dbt/models/core/dim_campaign.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per marketing campaign (current API snapshot; SCD Type 1).
-- budget/spend/impressions/clicks are current-snapshot attributes; derived
-- KPIs (CTR/CPC/CPM/ROAS) are computed in mart_marketing_roi (slice 3).
select
    campaign_id,
    name as campaign_name,
    channel,
    status as campaign_status,
    start_date,
    end_date,
    budget_eur,
    spend_eur,
    impressions,
    clicks
from {{ ref('int_campaigns') }}
```

- [ ] **Step 3: Offline parse + commit**

```bash
uv run dbt parse --project-dir dbt --profiles-dir dbt
git add dbt/dbt_project.yml dbt/models/core
git commit -m "feat(dbt): dim_date calendar, dim_product, dim_campaign (Phase 5 slice 2)"
```

---

### Task 4: dim_customer with SCD Type 2

**Files:**
- Create: `dbt/models/core/dim_customer.sql`

**Interfaces:**
- Consumes: `stg_customers` (full bronze history, needs `_batch_date`/`_ingested_at`).
- Produces: `customer_key varchar` = `'<customer_id>_<yyyy-mm-dd of valid_from>'`; `valid_from date`; `valid_to date` (inclusive, `9999-12-31` for open); `is_current boolean` (exactly one per customer).

- [ ] **Step 1: Create the model**

`dbt/models/core/dim_customer.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per customer version (SCD Type 2, daily version grain).
-- valid_from  = logical batch date (_batch_date) that first carried the
--               version; incremental batches contain only changed rows, so
--               every bronze row of a customer IS a distinct version;
-- valid_to    = day before the next version's batch date (inclusive
--               [valid_from, valid_to] intervals, overlap-tested);
-- is_current  = the open-ended version (valid_to = 9999-12-31).
-- Multiple source changes within one day collapse into that day's batch
-- (re-runs replace the partition) — documented daily-grain limitation.
with batch_versions as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        region,
        city,
        status,
        segment,
        registered_at,
        _batch_date as valid_from,
        row_number() over (
            partition by customer_id, _batch_date
            order by _ingested_at desc
        ) as batch_rank
    from {{ ref('stg_customers') }}
),
sequenced as (
    select
        customer_id,
        email,
        first_name,
        last_name,
        region,
        city,
        status,
        segment,
        registered_at,
        valid_from,
        lead(valid_from) over (
            partition by customer_id
            order by valid_from
        ) as next_valid_from
    from batch_versions
    where batch_rank = 1
)
select
    cast(customer_id as varchar) || '_' || substr(cast(valid_from as varchar), 1, 10)
        as customer_key,
    customer_id,
    email,
    first_name,
    last_name,
    region,
    city,
    status as customer_status,
    segment,
    registered_at,
    valid_from,
    coalesce(
        cast(next_valid_from - interval '1' day as date),
        date '9999-12-31'
    ) as valid_to,
    next_valid_from is null as is_current
from sequenced
```

- [ ] **Step 2: Offline parse + commit**

```bash
uv run dbt parse --project-dir dbt --profiles-dir dbt
git add dbt/models/core/dim_customer.sql
git commit -m "feat(dbt): dim_customer SCD Type 2 over bronze version history (Phase 5 slice 2)"
```

---

### Task 5: facts

**Files:**
- Create: `dbt/models/core/fact_orders.sql`
- Create: `dbt/models/core/fact_order_items.sql`
- Create: `dbt/models/core/fact_payments.sql`
- Create: `dbt/models/core/fact_shipments.sql`

**Interfaces:**
- Consumes: `int_orders`, `int_order_items`, `int_payments`, `int_shipments`, `dim_customer`.
- Produces: `fact_orders` (PK `order_id`, FK `customer_key` point-in-time resolved), `fact_order_items` (PK `order_item_id`, FKs `order_id`, `product_id`), `fact_payments` (PK `payment_id`, FK `order_id`), `fact_shipments` (PK `shipment_id`, FK `order_id`). Status/method columns renamed to domain-specific names; `transaction_id`/`tracking_number` kept as degenerate dimensions.

- [ ] **Step 1: Create the models**

`dbt/models/core/fact_orders.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per order (current state). PK: order_id.
-- Measures: shipping_cost, order_total (source currency; FX normalization
-- happens in marts, slice 3). Degenerate natural key: customer_id.
-- customer_key resolves the SCD2 version valid on the order's creation
-- date (point-in-time join); orders predating a customer's first observed
-- version fall back to that earliest version (defensive — the bootstrap
-- full snapshot covers all pre-existing customers).
with matched_keys as (
    select
        o.order_id,
        d.customer_key
    from {{ ref('int_orders') }} as o
    join {{ ref('dim_customer') }} as d
        on d.customer_id = o.customer_id
       and d.valid_from <= date(o.created_at)
       and date(o.created_at) <= d.valid_to
),
earliest_keys as (
    select
        customer_id,
        min(valid_from) as first_valid_from
    from {{ ref('dim_customer') }}
    group by customer_id
)
select
    o.order_id,
    coalesce(
        m.customer_key,
        cast(o.customer_id as varchar) || '_'
            || substr(cast(e.first_valid_from as varchar), 1, 10)
    ) as customer_key,
    o.customer_id,
    o.status as order_status,
    o.currency,
    o.shipping_cost,
    o.order_total,
    o.created_at,
    o.updated_at
from {{ ref('int_orders') }} as o
left join matched_keys as m
    on m.order_id = o.order_id
left join earliest_keys as e
    on e.customer_id = o.customer_id
```

`dbt/models/core/fact_order_items.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per order line. PK: order_item_id.
-- Measures: quantity, unit_price, line_total (line_total = quantity *
-- unit_price is business-tested). Upstream: int_order_items.
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    created_at
from {{ ref('int_order_items') }}
```

`dbt/models/core/fact_payments.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per payment. PK: payment_id.
-- Measures: payment_amount. Degenerate: transaction_id.
-- Reconciled against fact_orders by a singular business test.
select
    payment_id,
    order_id,
    method as payment_method,
    status as payment_status,
    amount as payment_amount,
    transaction_id,
    created_at
from {{ ref('int_payments') }}
```

`dbt/models/core/fact_shipments.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per shipment. PK: shipment_id.
-- Degenerate: tracking_number. Temporal ordering is business-tested.
select
    shipment_id,
    order_id,
    carrier,
    tracking_number,
    status as shipment_status,
    shipped_at,
    delivered_at,
    created_at
from {{ ref('int_shipments') }}
```

- [ ] **Step 2: Offline parse + commit**

```bash
uv run dbt parse --project-dir dbt --profiles-dir dbt
git add dbt/models/core
git commit -m "feat(dbt): fact tables with point-in-time SCD2 join (Phase 5 slice 2)"
```

---

### Task 6: core model contracts and built-in tests

**Files:**
- Create: `dbt/models/core/schema.yml`

**Interfaces:**
- Consumes: all core models.
- Produces: per-model YAML contracts (purpose, grain, PK, measures, upstream) and built-in tests: `unique`/`not_null` PKs, `relationships` FKs, `accepted_values` domains.

- [ ] **Step 1: Write the contracts**

`dbt/models/core/schema.yml`:

```yaml
version: 2

models:
  - name: dim_date
    description: >
      Calendar dimension. Purpose: standardized date attributes for drill-downs.
      Grain: one row per calendar day in the observed order-activity window
      (created_at/updated_at of orders). PK: date_key.
      Upstream: int_orders (bounds only).
    columns:
      - name: date_key
        description: Integer surrogate key in yyyymmdd form.
        tests: [unique, not_null]
      - name: full_date
        description: The calendar day.
        tests: [unique, not_null]

  - name: dim_product
    description: >
      Product dimension (SCD Type 1: current state; natural key).
      Grain: one row per product. PK: product_id.
      Measures carried as attributes: unit_price, unit_cost.
      Upstream: int_products, int_categories.
    columns:
      - name: product_id
        tests: [unique, not_null]
      - name: category_id
        tests:
          - not_null
          - relationships:
              to: ref('int_categories')
              field: category_id

  - name: dim_campaign
    description: >
      Marketing campaign dimension from the campaign API snapshot
      (SCD Type 1). Grain: one row per campaign. PK: campaign_id.
      Attributes: budget_eur, spend_eur, impressions, clicks (current
      snapshot; derived KPIs live in mart_marketing_roi, slice 3).
      Upstream: int_campaigns.
    columns:
      - name: campaign_id
        tests: [unique, not_null]
      - name: channel
        tests:
          - accepted_values:
              values: ["search", "display", "social", "email", "video"]
      - name: campaign_status
        tests:
          - accepted_values:
              values: ["active", "paused", "completed", "scheduled"]

  - name: dim_customer
    description: >
      Customer dimension, SCD Type 2 with daily version grain.
      Grain: one row per customer version. PK: customer_key
      ('<customer_id>_<valid_from>'). Semantics: valid_from = logical batch
      date carrying the version; valid_to = day before the next version
      (inclusive; 9999-12-31 while open); is_current marks the single open
      version. Upstream: stg_customers (full Bronze history).
    columns:
      - name: customer_key
        tests: [unique, not_null]
      - name: customer_id
        tests: [not_null]
      - name: customer_status
        tests:
          - accepted_values:
              values: ["active", "inactive", "churned"]
      - name: segment
        tests:
          - accepted_values:
              values: ["standard", "premium", "vip"]

  - name: fact_orders
    description: >
      Order fact (current state). Grain: one row per order. PK: order_id.
      Measures: shipping_cost, order_total (source currency). Degenerate
      natural key: customer_id. FK: customer_key — the SCD2 version valid on
      date(created_at) via point-in-time join with earliest-version
      fallback. Upstream: int_orders, dim_customer.
    columns:
      - name: order_id
        tests: [unique, not_null]
      - name: customer_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_key
      - name: order_status
        tests:
          - accepted_values:
              values: ["pending", "paid", "shipped", "delivered", "cancelled", "refunded"]

  - name: fact_order_items
    description: >
      Order-line fact. Grain: one row per order line. PK: order_item_id.
      Measures: quantity, unit_price, line_total.
      Upstream: int_order_items; FKs to fact_orders and dim_product.
    columns:
      - name: order_item_id
        tests: [unique, not_null]
      - name: order_id
        tests:
          - not_null
          - relationships:
              to: ref('fact_orders')
              field: order_id
      - name: product_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_product')
              field: product_id

  - name: fact_payments
    description: >
      Payment fact. Grain: one row per payment. PK: payment_id.
      Measures: payment_amount. Degenerate: transaction_id.
      Reconciliation against fact_orders is a singular test.
      Upstream: int_payments.
    columns:
      - name: payment_id
        tests: [unique, not_null]
      - name: order_id
        tests:
          - not_null
          - relationships:
              to: ref('fact_orders')
              field: order_id
      - name: payment_status
        tests:
          - accepted_values:
              values: ["pending", "authorized", "captured", "failed", "refunded", "cancelled"]

  - name: fact_shipments
    description: >
      Shipment fact. Grain: one row per shipment. PK: shipment_id.
      Degenerate: tracking_number. shipped_at <= delivered_at is
      business-tested. Upstream: int_shipments.
    columns:
      - name: shipment_id
        tests: [unique, not_null]
      - name: order_id
        tests:
          - not_null
          - relationships:
              to: ref('fact_orders')
              field: order_id
      - name: shipment_status
        tests:
          - accepted_values:
              values: ["pending", "in_transit", "delivered", "cancelled"]
```

- [ ] **Step 2: Offline parse (validates test wiring) + commit**

```bash
uv run dbt parse --project-dir dbt --profiles-dir dbt
git add dbt/models/core/schema.yml
git commit -m "test(dbt): core model contracts and built-in tests (Phase 5 slice 2)"
```

---

### Task 7: singular business tests

**Files:**
- Create: `dbt/tests/recon_orders_vs_payments.sql`
- Create: `dbt/tests/scd2_intervals_no_overlap.sql`
- Create: `dbt/tests/scd2_one_current_per_customer.sql`
- Create: `dbt/tests/amounts_not_negative.sql`
- Create: `dbt/tests/temporal_ordering.sql`
- Create: `dbt/tests/line_total_arithmetic.sql`

**Interfaces:**
- Consumes: core models + `int_fx_rates_daily`.
- Produces: singular tests that FAIL (`dbt build` red) when any row violates the invariant; empty result = pass.

- [ ] **Step 1: Write the tests**

`dbt/tests/recon_orders_vs_payments.sql`:

```sql
-- Orders <-> payments reconciliation (generator invariants, enforced as a
-- business contract): every order has exactly one payment, amounts match,
-- and the status pair is consistent. Orphan payments violate too.
with order_side as (
    select
        order_id,
        order_status,
        order_total
    from {{ ref('fact_orders') }}
),
payment_side as (
    select
        order_id,
        count(*) as payment_count,
        max(payment_amount) as payment_amount,
        max(payment_status) as payment_status
    from {{ ref('fact_payments') }}
    group by order_id
)
select
    coalesce(o.order_id, p.order_id) as order_id,
    o.order_status,
    o.order_total,
    p.payment_count,
    p.payment_amount,
    p.payment_status
from order_side as o
full outer join payment_side as p
    on p.order_id = o.order_id
where o.order_id is null
   or p.order_id is null
   or p.payment_count <> 1
   or p.payment_amount <> o.order_total
   or not (
        (o.order_status = 'pending' and p.payment_status = 'pending')
        or (o.order_status in ('paid', 'shipped', 'delivered')
            and p.payment_status = 'captured')
        or (o.order_status = 'refunded' and p.payment_status = 'refunded')
        or (o.order_status = 'cancelled'
            and p.payment_status in ('cancelled', 'refunded'))
   )
```

`dbt/tests/scd2_intervals_no_overlap.sql`:

```sql
-- SCD2 validity intervals per customer must not overlap, must be well
-- formed, and consecutive versions must be contiguous.
with sequenced as (
    select
        customer_id,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by customer_id
            order by valid_from
        ) as next_valid_from
    from {{ ref('dim_customer') }}
)
select
    customer_id,
    valid_from,
    valid_to,
    next_valid_from
from sequenced
where valid_from > valid_to
   or (next_valid_from is not null and next_valid_from <= valid_to)
```

`dbt/tests/scd2_one_current_per_customer.sql`:

```sql
-- Exactly one open (is_current) version per customer, and no closed
-- version may use the open-ended valid_to sentinel.
with per_customer as (
    select
        customer_id,
        count_if(is_current) as current_count,
        count_if(not is_current and valid_to = date '9999-12-31') as open_old_count
    from {{ ref('dim_customer') }}
    group by customer_id
)
select
    customer_id,
    current_count,
    open_old_count
from per_customer
where current_count <> 1
   or open_old_count <> 0
```

`dbt/tests/amounts_not_negative.sql`:

```sql
-- Financial measures must be non-negative; FX rates strictly positive.
select
    'fact_orders' as model_name,
    cast(order_id as varchar) as row_key
from {{ ref('fact_orders') }}
where order_total < 0 or shipping_cost < 0
union all
select
    'fact_order_items',
    cast(order_item_id as varchar)
from {{ ref('fact_order_items') }}
where quantity < 0 or unit_price < 0 or line_total < 0
union all
select
    'fact_payments',
    cast(payment_id as varchar)
from {{ ref('fact_payments') }}
where payment_amount < 0
union all
select
    'dim_campaign',
    campaign_id
from {{ ref('dim_campaign') }}
where budget_eur < 0 or spend_eur < 0 or impressions < 0 or clicks < 0
union all
select
    'int_fx_rates_daily',
    currency || '_' || substr(cast(rate_date as varchar), 1, 10)
from {{ ref('int_fx_rates_daily') }}
where rate <= 0
```

`dbt/tests/temporal_ordering.sql`:

```sql
-- Event times must be internally ordered (generator invariants).
select
    'fact_shipments' as model_name,
    cast(shipment_id as varchar) as row_key
from {{ ref('fact_shipments') }}
where delivered_at is not null and shipped_at > delivered_at
union all
select
    'fact_orders',
    cast(order_id as varchar)
from {{ ref('fact_orders') }}
where created_at > updated_at
union all
select
    'fact_payments',
    cast(p.payment_id as varchar)
from {{ ref('fact_payments') }} as p
join {{ ref('fact_orders') }} as o
    on o.order_id = p.order_id
where p.created_at < o.created_at
union all
select
    'dim_campaign',
    campaign_id
from {{ ref('dim_campaign') }}
where end_date < start_date
```

`dbt/tests/line_total_arithmetic.sql`:

```sql
-- Line arithmetic invariant: line_total = quantity * unit_price.
select
    order_item_id
from {{ ref('fact_order_items') }}
where line_total <> quantity * unit_price
```

- [ ] **Step 2: Offline parse + commit**

```bash
uv run dbt parse --project-dir dbt --profiles-dir dbt
git add dbt/tests
git commit -m "test(dbt): singular business tests incl. orders/payments reconciliation (Phase 5 slice 2)"
```

---

### Task 8: analytical data-model documentation

**Files:**
- Rewrite: `docs/data-model.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: the implemented model tree.

- [ ] **Step 1: Extend `docs/data-model.md`**

Keep the existing "OLTP source data model (Phase 2)" section verbatim at the top; append the following sections after it:

```markdown
## Bronze layer (Phase 5 slice 1)

Ten Iceberg tables (`bronze` schema) mirror the raw archive objects one
logical day at a time, partitioned by `_batch_date`:

| Table | Source | Grain |
|---|---|---|
| `orders`, `order_items`, `customers`, `products`, `categories`, `payments`, `shipments` | PG snapshots (Parquet) | one row per source record per logical batch |
| `fx_rates`, `campaigns`, `deliveries` | API pages (JSON, flattened) | one row per source record per raw page |

Service columns on every table: `_batch_id` (deterministic
`<source>-<yyyymmdd>`), `_batch_date` (partition), `_source_object`,
`_ingested_at`. Loads are idempotent per (table, logical date):
`DELETE` partition → batched `INSERT`, row-count-verified against the raw
manifest.

## Silver layer (Phase 5 slice 2)

Staging (`stg_*`) is a typed pass-through of bronze. Intermediate
(`int_*`) derives the **current state** of each entity: append-only bronze
history is deduplicated by `row_number() ... order by _batch_date desc,
_ingested_at desc` on the business key. `int_fx_rates_daily` keeps the
latest rate per (currency, day); `int_campaigns` keeps the latest daily
campaign snapshot; `int_deliveries` keeps the latest carrier status.

Known limitation (until CDC, Phase 8): snapshot extraction cannot see hard
deletes, so a deleted source row retains its last version in `int_*` and
downstream facts/dimensions.

## Gold layer — Kimball (Phase 5 slice 2)

### Dimensions

| Model | Grain | PK | Type | Upstream |
|---|---|---|---|---|
| `dim_date` | calendar day | `date_key` (yyyymmdd int) | generated | `int_orders` bounds (created/updated) |
| `dim_product` | product | `product_id` | SCD1 | `int_products`, `int_categories` |
| `dim_campaign` | campaign | `campaign_id` | SCD1 | `int_campaigns` |
| `dim_customer` | customer **version** | `customer_key` | SCD2 | `stg_customers` history |

`dim_customer` SCD2 semantics:

- every bronze row of a customer is one version (incremental batches carry
  only changed rows);
- `valid_from` = `_batch_date` of the version; `valid_to` = the day before
  the next version (inclusive); open versions use `9999-12-31`;
- `customer_key = '<customer_id>_<valid_from>'` — deterministic, rebuildable;
- `is_current` marks the single open version (tested);
- daily grain: multiple same-day source changes collapse into that day's
  version.

### Facts

| Model | Grain | PK | FKs | Measures | Notes |
|---|---|---|---|---|---|
| `fact_orders` | order | `order_id` | `customer_key` → `dim_customer` (point-in-time on `created_at`, earliest-version fallback) | `shipping_cost`, `order_total` (source currency) | degenerate `customer_id` |
| `fact_order_items` | order line | `order_item_id` | `order_id` → `fact_orders`; `product_id` → `dim_product` | `quantity`, `unit_price`, `line_total` | immutable |
| `fact_payments` | payment | `payment_id` | `order_id` → `fact_orders` | `payment_amount` | degenerate `transaction_id`; reconciled vs orders by test |
| `fact_shipments` | shipment | `shipment_id` | `order_id` → `fact_orders` | — | degenerate `tracking_number` |

### Business tests (dbt singular)

- orders ↔ payments: exactly one payment per order, equal amounts,
  consistent status pairs, no orphans;
- SCD2: non-overlapping contiguous intervals; exactly one current version;
- non-negative amounts; strictly positive FX rates;
- temporal ordering (created ≤ updated; shipped ≤ delivered; payment
  created ≥ order created; campaign start ≤ end);
- `line_total = quantity × unit_price`.

### Known limitations

- delivery-API `order_id` values are the partner's synthetic references
  ("ORD-…") and never join OLTP orders; `int_deliveries` feeds
  `mart_delivery_performance` (slice 3) only;
- currency normalization to EUR happens in marts (slice 3) via
  `int_fx_rates_daily`; facts keep source currency;
- snapshot extracts hide hard deletes until CDC (Phase 8);
- SCD2 versions have daily granularity (see above).
```

- [ ] **Step 2: Update README**

In `README.md`, after the "Bronze layer (Phase 5 slice 1)" section, add:

```markdown
## Silver and Gold layers (Phase 5 slice 2)

dbt builds the analytical model over Bronze: `silver.stg_*` (typed
pass-through) and `silver.int_*` (current entity state via keyset dedup),
then the Kimball `gold` layer — `dim_date`, `dim_product`, `dim_campaign`,
SCD2 `dim_customer`, and the `fact_orders` / `fact_order_items` /
`fact_payments` / `fact_shipments` facts. Every core model carries a YAML
contract (grain, PK, measures, upstream) and is covered by built-in and
singular business tests, including the orders ↔ payments reconciliation.
Run against the live core stack with `make dbt-build`; the model reference
lives in `docs/data-model.md`.
```

- [ ] **Step 3: Commit**

```bash
git add docs/data-model.md README.md
git commit -m "docs: analytical data model reference for silver/gold (Phase 5 slice 2)"
```

---

### Task 9: live integration — full dbt build over a seeded world

**Files:**
- Create: `tests/integration/test_dbt_core_build.py`

**Interfaces:**
- Consumes: `load`, `DbapiTrinoExecutor`, `TrinoConfig`, `spec_by_source`, `all_sources`, `TABLES` (lakehouse specs); `table_by_name` (snapshot specs); `BatchManifest`; `postgres_snapshot_key`, `api_page_key`, `manifest_key`, `BUCKET_ARCHIVE`; `BotoObjectStorage`; the `live_storage` fixture from `tests/integration/conftest.py`.
- Produces: one integration test that seeds a deterministic two-day world into bronze, runs full `dbt build`, and asserts SCD2/point-in-time/Kimball behavior on marker entities.

**Seeded dataset (fixed marker ids far above generator ranges):**

- DAY_1 = 2026-09-20, DAY_2 = 2026-09-21.
- categories 90001 (root) + 90002 (leaf, parent 90001); products 910001/910002 (25.00/40.00).
- customers 990001 (`it-customer@example.com`, premium, region `it-region`) and 990002 (standard, `it-region-b`); DAY_2 re-issues 990001 with region `it-region-2` (new SCD2 version).
- orders: 990001 (delivered, USD, 2×25.00 + 1×40.00 + 3.00 shipping = 93.00) and 990002 (refunded, EUR, 1×40.00 + 2.00 = 42.00); DAY_2 re-issues 990002 with bumped `updated_at`.
- payments: 980001 captured 93.00 / 980002 refunded 42.00 (amount == order_total).
- shipments: 970001 delivered for order 990001 (created ≤ shipped ≤ delivered).
- fx: DAY_1 page [USD 1.0912, GBP 0.8501]; DAY_2 page [USD 1.1020].
- campaigns: `CMP-IT-0001` (search, active, 2026-09-01..2026-09-30, budget 10000.00, spend 2500.50, impressions 100000, clicks 3200) on both days (daily snapshot).
- deliveries: `DLV-IT-000001` (DHL, partner order `ORD-IT-0001`) `in_transit` DAY_1 → `delivered` DAY_2.
- Empty (table, day) combinations produce no objects and no manifests — the loader treats them as warning no-ops.

**Test-owns-bronze policy:** `dbt build` joins across all ten sources, so the test first deletes EVERY partition of the ten bronze tables (guarded by table existence; bronze is rebuildable by design — `make bronze-load` restores local data) and purges only the seeded raw objects/manifests of DAY_1/DAY_2 in teardown. Existing gold/silver models are simply rebuilt.

- [ ] **Step 1: Write the test file**

`tests/integration/test_dbt_core_build.py`:

```python
"""Live integration: full dbt build over a seeded, consistent Bronze world.

Requires the core profile (MinIO, Polaris, Trino) and OMNI_INTEGRATION=1
(``make up`` then ``make integration``). The test OWNS the bronze content
while it runs: existing partitions of the ten bronze tables are purged
first (bronze is rebuildable by design — re-run ``make bronze-load`` to
restore local data), a deterministic two-day dataset is loaded, full
``dbt build`` (models + built-in + singular tests) must succeed, and
SCD2 / point-in-time / Kimball semantics are asserted on marker entities.
"""

import hashlib
import json
import os
import subprocess
from collections.abc import Generator
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import trino

from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    api_page_key,
    manifest_key,
    postgres_snapshot_key,
)
from omni_retail.ingestion.common.storage import BotoObjectStorage
from omni_retail.ingestion.postgres_snapshot.tables import table_by_name
from omni_retail.lakehouse.bronze.loader import DbapiTrinoExecutor, TrinoConfig, load
from omni_retail.lakehouse.bronze.specs import TABLES as BRONZE_TABLES
from omni_retail.lakehouse.bronze.specs import all_sources, spec_by_source

REPO_ROOT = Path(__file__).resolve().parents[2]
DAY_1 = date(2026, 9, 20)
DAY_2 = date(2026, 9, 21)
OLTP_TABLES = (
    "categories",
    "products",
    "customers",
    "orders",
    "order_items",
    "payments",
    "shipments",
)
API_SOURCES = ("fx-rates", "marketing-campaigns", "deliveries")


def trino_scalar(sql: str) -> object:
    """First column of the first row; None for DDL/no-row statements."""
    config = TrinoConfig.from_env()
    connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host=config.host, port=config.port, user=config.user, catalog=config.catalog
    )
    cursor = connection.cursor()
    cursor.execute(sql)
    row = cursor.fetchone()
    connection.close()
    if row is None:
        return None
    return row[0]


def bronze_table_exists(table: str) -> bool:
    count = trino_scalar(
        "select count(*) from iceberg.information_schema.tables "
        f"where table_schema = 'bronze' and table_name = '{table}'"
    )
    return bool(count == 1)


def reset_bronze() -> None:
    """Delete every partition of every bronze table (test owns bronze)."""
    for name in BRONZE_TABLES:
        if bronze_table_exists(name):
            trino_scalar(f"delete from iceberg.bronze.{name}")


def purge_test_raw(storage: BotoObjectStorage) -> None:
    """Remove only the seeded raw objects and manifests of both days."""
    prefixes = [
        f"postgres/{name}/{day:%Y/%m/%d}/" for name in OLTP_TABLES for day in (DAY_1, DAY_2)
    ]
    prefixes += [f"api/{source}/{day:%Y%m%d}/" for source in API_SOURCES for day in (DAY_1, DAY_2)]
    prefixes += [
        f"_manifests/postgres-{name}/postgres-{name}-{day:%Y%m%d}.json"
        for name in OLTP_TABLES
        for day in (DAY_1, DAY_2)
    ]
    prefixes += [
        f"_manifests/{source}/{source}-{day:%Y%m%d}.json"
        for source in API_SOURCES
        for day in (DAY_1, DAY_2)
    ]
    for prefix in prefixes:
        for key in storage.list_object_keys(BUCKET_ARCHIVE, prefix):
            storage.delete_object(BUCKET_ARCHIVE, key)


@pytest.fixture()
def seeded_world(live_storage: BotoObjectStorage) -> Generator[date, None, None]:
    reset_bronze()
    purge_test_raw(live_storage)
    seed_day_1(live_storage)
    seed_day_2(live_storage)
    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        for logical_date in (DAY_1, DAY_2):
            for source in all_sources():
                load(live_storage, executor, spec_by_source(source), logical_date=logical_date)
    yield DAY_1
    for name in BRONZE_TABLES:
        if bronze_table_exists(name):
            partition_dates = (DAY_1, DAY_2)
            for day in partition_dates:
                trino_scalar(
                    f'delete from iceberg.bronze.{name} where "_batch_date" = '
                    f"DATE '{day:%Y-%m-%d}'"
                )
    purge_test_raw(live_storage)


def put_parquet(
    storage: BotoObjectStorage, table: str, logical_date: date, rows: list[tuple[object, ...]]
) -> None:
    spec = table_by_name(table)
    arrays = [
        pa.array([row[index] for row in rows], type=column.type)
        for index, column in enumerate(spec.columns)
    ]
    arrow = pa.Table.from_arrays(arrays, schema=spec.arrow_schema())
    sink = pa.BufferOutputStream()
    pq.write_table(arrow, sink)
    body = sink.getvalue().to_pybytes()
    storage.put_object(BUCKET_ARCHIVE, postgres_snapshot_key(table, logical_date), body)
    put_postgres_manifest(
        storage,
        table=table,
        logical_date=logical_date,
        row_count=len(rows),
        object_key=postgres_snapshot_key(table, logical_date),
        checksum=hashlib.sha256(body).hexdigest(),
        size_bytes=len(body),
    )


def put_postgres_manifest(
    storage: BotoObjectStorage,
    *,
    table: str,
    logical_date: date,
    row_count: int,
    object_key: str,
    checksum: str,
    size_bytes: int,
) -> None:
    source = f"postgres-{table}"
    batch_id = f"{source}-{logical_date:%Y%m%d}"
    manifest = BatchManifest(
        batch_id=batch_id,
        source=source,
        source_kind="postgres",
        status="completed",
        object_key=object_key,
        checksum=checksum,
        size_bytes=size_bytes,
        ingested_at=datetime.combine(logical_date, time(8, 0), tzinfo=UTC),
        logical_date=logical_date,
        row_count=row_count,
        rejected_row_count=0,
        schema_version="1.0",
    )
    storage.put_object(BUCKET_ARCHIVE, manifest_key(source, batch_id), manifest.to_json().encode())


def put_api_pages(
    storage: BotoObjectStorage,
    *,
    source: str,
    logical_date: date,
    pages: list[dict[str, object]],
    row_count: int,
) -> None:
    checksum = hashlib.sha256()
    size = 0
    for number, payload in enumerate(pages, start=1):
        body = json.dumps(payload).encode()
        checksum.update(body)
        size += len(body)
        storage.put_object(BUCKET_ARCHIVE, api_page_key(source, logical_date, number), body)
    batch_id = f"{source}-{logical_date:%Y%m%d}"
    manifest = BatchManifest(
        batch_id=batch_id,
        source=source,
        source_kind="api",
        status="completed",
        object_key=f"api/{source}/{logical_date:%Y%m%d}/",
        checksum=checksum.hexdigest(),
        size_bytes=size,
        ingested_at=datetime.combine(logical_date, time(8, 0), tzinfo=UTC),
        logical_date=logical_date,
        row_count=row_count,
        rejected_row_count=0,
        schema_version="1.0",
    )
    storage.put_object(BUCKET_ARCHIVE, manifest_key(source, batch_id), manifest.to_json().encode())


def seed_day_1(storage: BotoObjectStorage) -> None:
    cat_created = datetime(2026, 9, 1, tzinfo=UTC)
    put_parquet(
        storage,
        "categories",
        DAY_1,
        [
            (90001, "it-root-cat", None, cat_created, cat_created),
            (90002, "it-leaf-cat", 90001, cat_created, cat_created),
        ],
    )
    put_parquet(
        storage,
        "products",
        DAY_1,
        [
            (
                910001,
                "IT-SKU-0001",
                "it-product-a",
                90002,
                "it-brand",
                Decimal("25.00"),
                Decimal("15.00"),
                True,
                cat_created,
                cat_created,
            ),
            (
                910002,
                "IT-SKU-0002",
                "it-product-b",
                90001,
                "it-brand",
                Decimal("40.00"),
                Decimal("22.00"),
                True,
                cat_created,
                cat_created,
            ),
        ],
    )
    put_parquet(
        storage,
        "customers",
        DAY_1,
        [
            (
                990001,
                "it-customer@example.com",
                "Ita",
                "One",
                "it-region",
                "it-city",
                "active",
                "premium",
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 9, 20, 9, 0, tzinfo=UTC),
            ),
            (
                990002,
                "it-customer2@example.com",
                "Itb",
                "Two",
                "it-region-b",
                "it-city-b",
                "active",
                "standard",
                datetime(2026, 8, 2, tzinfo=UTC),
                datetime(2026, 8, 2, tzinfo=UTC),
                datetime(2026, 9, 20, 9, 0, tzinfo=UTC),
            ),
        ],
    )
    put_parquet(
        storage,
        "orders",
        DAY_1,
        [
            (
                990001,
                990001,
                "delivered",
                "USD",
                Decimal("3.00"),
                Decimal("93.00"),
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
            ),
            (
                990002,
                990002,
                "refunded",
                "EUR",
                Decimal("2.00"),
                Decimal("42.00"),
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 13, 0, tzinfo=UTC),
            ),
        ],
    )
    put_parquet(
        storage,
        "order_items",
        DAY_1,
        [
            (
                9000001,
                990001,
                910001,
                2,
                Decimal("25.00"),
                Decimal("50.00"),
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
            ),
            (
                9000002,
                990001,
                910002,
                1,
                Decimal("40.00"),
                Decimal("40.00"),
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 10, 0, tzinfo=UTC),
            ),
            (
                9000003,
                990002,
                910002,
                1,
                Decimal("40.00"),
                Decimal("40.00"),
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
            ),
        ],
    )
    put_parquet(
        storage,
        "payments",
        DAY_1,
        [
            (
                980001,
                990001,
                "card",
                "captured",
                Decimal("93.00"),
                "TXN-IT-0001",
                datetime(2026, 9, 20, 10, 5, tzinfo=UTC),
                datetime(2026, 9, 20, 10, 5, tzinfo=UTC),
            ),
            (
                980002,
                990002,
                "paypal",
                "refunded",
                Decimal("42.00"),
                "TXN-IT-0002",
                datetime(2026, 9, 20, 11, 5, tzinfo=UTC),
                datetime(2026, 9, 20, 11, 5, tzinfo=UTC),
            ),
        ],
    )
    put_parquet(
        storage,
        "shipments",
        DAY_1,
        [
            (
                970001,
                990001,
                "DHL",
                "TRK-IT-0001",
                "delivered",
                datetime(2026, 9, 20, 15, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 18, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 12, 0, tzinfo=UTC),
                datetime(2026, 9, 20, 18, 0, tzinfo=UTC),
            ),
        ],
    )
    put_api_pages(
        storage,
        source="fx-rates",
        logical_date=DAY_1,
        pages=[
            {"rates": [{"currency": "USD", "rate": 1.0912}, {"currency": "GBP", "rate": 0.8501}]}
        ],
        row_count=2,
    )
    put_api_pages(
        storage,
        source="marketing-campaigns",
        logical_date=DAY_1,
        pages=[
            {
                "campaigns": [
                    {
                        "campaign_id": "CMP-IT-0001",
                        "name": "It Sale",
                        "channel": "search",
                        "status": "active",
                        "start_date": "2026-09-01",
                        "end_date": "2026-09-30",
                        "budget_eur": 10000.0,
                        "spend_eur": 2500.5,
                        "impressions": 100000,
                        "clicks": 3200,
                    }
                ]
            }
        ],
        row_count=1,
    )
    put_api_pages(
        storage,
        source="deliveries",
        logical_date=DAY_1,
        pages=[
            {
                "deliveries": [
                    {
                        "delivery_id": "DLV-IT-000001",
                        "order_id": "ORD-IT-0001",
                        "carrier": "DHL",
                        "status": "in_transit",
                        "shipped_at": "2026-09-20T15:00:00+00:00",
                        "delivered_at": None,
                        "updated_at": "2026-09-20T16:00:00+00:00",
                    }
                ]
            }
        ],
        row_count=1,
    )


def seed_day_2(storage: BotoObjectStorage) -> None:
    put_parquet(
        storage,
        "customers",
        DAY_2,
        [
            (
                990001,
                "it-customer@example.com",
                "Ita",
                "One",
                "it-region-2",
                "it-city-2",
                "active",
                "premium",
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 8, 1, tzinfo=UTC),
                datetime(2026, 9, 21, 9, 0, tzinfo=UTC),
            ),
        ],
    )
    put_parquet(
        storage,
        "orders",
        DAY_2,
        [
            (
                990002,
                990002,
                "refunded",
                "EUR",
                Decimal("2.00"),
                Decimal("42.00"),
                datetime(2026, 9, 20, 11, 0, tzinfo=UTC),
                datetime(2026, 9, 21, 10, 0, tzinfo=UTC),
            ),
        ],
    )
    put_api_pages(
        storage,
        source="fx-rates",
        logical_date=DAY_2,
        pages=[{"rates": [{"currency": "USD", "rate": 1.1020}]}],
        row_count=1,
    )
    put_api_pages(
        storage,
        source="marketing-campaigns",
        logical_date=DAY_2,
        pages=[
            {
                "campaigns": [
                    {
                        "campaign_id": "CMP-IT-0001",
                        "name": "It Sale",
                        "channel": "search",
                        "status": "active",
                        "start_date": "2026-09-01",
                        "end_date": "2026-09-30",
                        "budget_eur": 10000.0,
                        "spend_eur": 2500.5,
                        "impressions": 100000,
                        "clicks": 3200,
                    }
                ]
            }
        ],
        row_count=1,
    )
    put_api_pages(
        storage,
        source="deliveries",
        logical_date=DAY_2,
        pages=[
            {
                "deliveries": [
                    {
                        "delivery_id": "DLV-IT-000001",
                        "order_id": "ORD-IT-0001",
                        "carrier": "DHL",
                        "status": "delivered",
                        "shipped_at": "2026-09-20T15:00:00+00:00",
                        "delivered_at": "2026-09-21T09:30:00+00:00",
                        "updated_at": "2026-09-21T09:30:00+00:00",
                    }
                ]
            }
        ],
        row_count=1,
    )


def test_full_dbt_build_with_kimball_semantics(seeded_world: date) -> None:
    completed = subprocess.run(
        [
            "uv",
            "run",
            "dbt",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ],
        cwd=REPO_ROOT,
        check=False,
        timeout=900,
        env={**os.environ, "TRINO_HOST": os.environ.get("TRINO_HOST", "127.0.0.1")},
    )
    assert completed.returncode == 0, "dbt build (models + tests) must succeed"

    # SCD2: two versions for the mutated customer, intervals contiguous.
    assert (
        trino_scalar("select count(*) from iceberg.gold.dim_customer where customer_id = 990001")
        == 2
    )
    assert (
        trino_scalar(
            "select region from iceberg.gold.dim_customer where customer_id = 990001 and is_current"
        )
        == "it-region-2"
    )
    assert trino_scalar(
        "select valid_to from iceberg.gold.dim_customer "
        "where customer_id = 990001 and not is_current"
    ) == date(2026, 9, 20)
    # Unchanged customer keeps exactly one (current) version.
    assert (
        trino_scalar("select count(*) from iceberg.gold.dim_customer where customer_id = 990002")
        == 1
    )

    # Point-in-time join: order created on DAY_1 binds the DAY_1 version.
    assert (
        trino_scalar("select customer_key from iceberg.gold.fact_orders where order_id = 990001")
        == "990001_2026-09-20"
    )

    # Kimball shapes on marker entities.
    assert (
        trino_scalar(
            "select count(*) from iceberg.gold.fact_order_items where order_id in (990001, 990002)"
        )
        == 3
    )
    assert trino_scalar("select count(*) from iceberg.gold.fact_payments") == 2
    assert trino_scalar("select count(*) from iceberg.gold.fact_shipments") == 1
    assert (
        trino_scalar("select count(*) from iceberg.gold.dim_campaign") == 1
    )  # dedup across the two daily snapshots
    assert (
        trino_scalar("select category_name from iceberg.gold.dim_product where product_id = 910002")
        == "it-root-cat"
    )
    # Latest-wins semantics for daily feeds.
    assert (
        trino_scalar(
            "select count(*) from iceberg.silver.int_fx_rates_daily where currency = 'USD'"
        )
        == 2
    )
    assert (
        trino_scalar(
            "select status from iceberg.silver.int_deliveries where delivery_id = 'DLV-IT-000001'"
        )
        == "delivered"
    )
    # Calendar dimension spans the order-activity window.
    assert trino_scalar("select min(full_date) from iceberg.gold.dim_date") == DAY_1
    assert trino_scalar("select max(full_date) from iceberg.gold.dim_date") == DAY_2
```

Notes for the implementer:
- `purge_test_raw` uses exact object keys for manifests (not prefixes) — `list_object_keys` with the full key as prefix returns exactly that key when present.
- If `trino.dbapi.connect` needs `no_proxy` handling in the environment, `make integration` already exports `no_proxy=127.0.0.1,localhost` (see Makefile).
- If dbt emits a deprecation about `tests:` vs `data_tests:` keys, keep `tests:` (dbt 1.10 still accepts it); do not rename mid-slice.

- [ ] **Step 2: Run the integration test (live core stack)**

```bash
make up
uv run pytest tests/integration/test_dbt_core_build.py -v
```
Expected: PASS (first run ~1–3 minutes: 20 bronze loads + full dbt build).

If a singular/built-in test fails, debug the seeded data first (the world is deterministic); fix the seed, not the test invariant. If a Trino SQL construct errors, adjust the model SQL and re-run — the probes in the plan header cover the risky constructs.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_dbt_core_build.py
git commit -m "test(integration): full dbt build with SCD2 and reconciliation assertions (Phase 5 slice 2)"
```

---

### Task 10: final validation, branch, PR

- [ ] **Step 1: Full local validation**

```bash
make lint && make test
uv run dbt parse --project-dir dbt --profiles-dir dbt
make dbt-build
make integration
```
Expected: all PASS (`make dbt-build` rebuilds gold/silver from whatever bronze currently holds — on the test-purged stack it will rebuild near-empty models; that is expected and harmless, or re-load a real date first with `make ingest-api ARGS="run --source fx-rates --date 2026-09-18"` etc.).

- [ ] **Step 2: Push and open PR**

```bash
git checkout -b feature/phase5-slice2-core-kimball   # if not already on it
git push -u origin feature/phase5-slice2-core-kimball
gh pr create --title "feat(dbt): intermediate + core Kimball with SCD2 and business tests (Phase 5 slice 2)" \
  --body "Implements Phase 5 slice 2 per docs/superpowers/specs/2026-09-18-phase5-dbt-lakehouse-design.md §10. See docs/superpowers/plans/2026-09-19-phase5-slice2-core-kimball.md." || true
```

If `gh` is unavailable, report the branch name for the user to open the PR.

- [ ] **Step 3: Report**

Produce the AGENTS §49 completion report.

---

## Self-review notes (already applied)

- Spec §10 slice 2 coverage: API staging (Task 1) ✓, intermediate (Task 2) ✓, core Kimball SCD2 (Tasks 3–5) ✓, model contracts (Task 6) ✓, built-in + singular tests incl. reconciliation (Tasks 6–7) ✓, `docs/data-model.md` (Task 8) ✓, full `dbt build` integration on fixed data (Task 9, spec §9) ✓.
- Out of scope (spec §11, slice 3): marts, Airflow DAG `transform_lakehouse`, dbt-in-Airflow image work, dbt docs generate.
- Type consistency: `customer_key` construction identical in `dim_customer` and `fact_orders` fallback (`'<id>_<yyyy-mm-dd>'`); singular tests reference renamed columns (`payment_amount`, `order_status`, `campaign_status`) exactly as the models define them.
