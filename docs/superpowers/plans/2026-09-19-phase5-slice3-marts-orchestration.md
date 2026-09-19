# Phase 5 Slice 3 — Marts + lakehouse orchestration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the four analytics marts (`mart_daily_sales`, `mart_customer_ltv`, `mart_marketing_roi`, `mart_delivery_performance`) with EUR normalization, wire the dataset-triggered Airflow DAG `transform_lakehouse` (raw → Bronze → dbt build), generate dbt docs, and prove the whole chain with a live integration test.

**Architecture:** dbt marts live in the new `analytics` schema and read the slice-2 Kimball core; a new intermediate model `int_orders_fx` is the single order-grain FX-normalization primitive shared by the revenue marts. Airflow ingestion DAGs (3 API + PG snapshot) publish the `lakehouse://bronze` dataset; the new `transform_lakehouse` DAG is triggered by it and runs a **watermark-driven** Bronze load (`run-new`: raw days not yet in Bronze) followed by `dbt build` inside the Airflow image.

**Tech Stack:** dbt-core 1.10 / dbt-trino 1.9 (already pinned main deps), Trino 483, Airflow 2.11.2 Datasets (TaskFlow), Docker Compose. **No new dependencies** (no dbt-utils — tests stay hand-written SQL).

**Spec:** `docs/superpowers/specs/2026-09-18-phase5-dbt-lakehouse-design.md` (§5 marts, §10 slice 3, §2 fixed decisions). Branch: `feature/phase5-slice3-marts-orchestration` from `main` @ faa94b3.

**Spec deviation (flagged for review):** the spec sketch says `load_bronze (run-all за logical date)`. That is incorrect for dataset-triggered runs: the triggering run's logical date is the *trigger timestamp*, not the producers' logical dates (an ingest run for D completes ~D+1T00:00 → transform `ds` = D+1 while raw data sits under D; manual/backfilled runs break the offset entirely). This plan replaces it with a watermark-driven `run-new` Bronze command that loads exactly the raw days missing from Bronze — idempotent, backfill-safe, no-op on re-triggers. Same primitive, correct trigger semantics.

## Global Constraints

- Spec §2 decisions are fixed: marts materialized as **tables** in schema **`analytics`**; `mart_marketing_roi` computes **no fake ROAS** (no revenue attribution exists; ROAS arrives with clickstream in Phase 9); `mart_delivery_performance` is fed by the **delivery-API feed only** (synthetic `ORD-…` references never join OLTP orders).
- Every mart model gets a contract in `dbt/models/marts/schema.yml`: purpose, grain, PK/uniqueness, measures, upstream (AGENTS §20.5).
- No new Python/dbt dependencies (AGENTS §11). No business SQL in DAGs (§19.1) — the DAG only shells out to `dbt build`.
- SQL style per AGENTS §44: one selected column per line, explicit aliases, meaningful CTE names, no `select *` in models, no ordinal GROUP BY.
- Airflow image tag bumps **0.1.0 → 0.2.0** (required: `dbt` CLI must be on PATH — Task 8); pin stays `apache/airflow:2.11.2-python3.12`.
- Realized-revenue policy (fixed for all marts/tests): financial measures **exclude** `cancelled` and `refunded` orders; order **counts** include all orders.
- Conventional Commits; squash-merge PR; scope stays on spec §10 slice 3 (spec §11 items are out of scope).
- dbt models/tests cannot execute offline: per-task gate is `dbt parse` (refs to missing models fail parsing); live `make dbt-build` validation happens in Task 5, DAG-level in Task 9.

## Review Focus

Failure modes the spec implies but no runtime input exercises — each pinned to its owning task's tests:

1. **Non-EUR order with no FX rate on/before its order date** (fx API not ingested that day, or order predates all fx data): the mart must keep the row and fail loudly, not silently drop it (inner join) or convert at rate 1 (coalesce). → Task 1 left-join + `marts_fx_rate_coverage` singular test; Task 9 seeds full coverage so build passes.
2. **Zero-impression campaigns** (scheduled/not-yet-started: mock API yields `impressions = 0, clicks = 0, spend = 0`): ctr/cpc/cpm division must be null-safe, and `not_null` tests must be scoped with `where impressions > 0`. → Task 4 `nullif` + where-scoped tests + `mart_marketing_metrics_valid` (clicks ≤ impressions).
3. **`dbt` binary not on PATH inside the Airflow image**: `uv pip install --target` puts console scripts in `<site-packages>/bin`, which is not on PATH. → Task 8 Dockerfile `ENV PATH` + CI `dbt --version` step (build fails visibly otherwise).
4. **Dataset-triggered logical date ≠ producer logical date**: `run-all --date ds` would load the wrong day. → Task 6 `run-new` watermark loader with unit tests covering pending-date selection (holes, replays, missing table, empty archive); Task 9 proves a full day flows through despite the trigger's own date.
5. **Fan-out double counting**: an order spanning 2 categories counts in 2 mart rows; aggregating `orders_count` across rows overcounts. Documented as an additive-grain caveat; correctness is pinned by a **date-grain revenue reconciliation against order-grain truth** (different source grain, non-circular). → Task 2 `mart_daily_sales_revenue_reconcile` + contract wording; Task 9 asserts exact 3-row scenario values.

---

### Task 1: `int_orders_fx` — order-grain FX normalization primitive

**Files:**
- Create: `dbt/models/intermediate/int_orders_fx.sql`
- Create: `dbt/tests/marts_fx_rate_coverage.sql`

**Interfaces:**
- Consumes: `fact_orders` (order_id, customer_id, customer_key, order_status, currency, order_total, shipping_cost, created_at), `int_fx_rates_daily` (currency, rate_date, rate). Rate semantics: **units of the order currency per 1 EUR** (fx API base EUR; no EUR row exists).
- Produces: view `silver.int_orders_fx` — one row per order: `order_id, customer_id, customer_key, currency, order_status, order_total, shipping_cost, created_at, rate_to_eur (double, NULL when a non-EUR order has no rate on/before its date; 1.0 for EUR), order_total_eur, shipping_cost_eur` (NULL when `rate_to_eur` is NULL — deliberate, see Review Focus 1).

- [ ] **Step 1: Write the failing singular test**

`dbt/tests/marts_fx_rate_coverage.sql`:

```sql
-- FX coverage business test (Phase 5 slice 3, Review Focus 1):
-- every non-EUR order must have a usable rate (latest rate on or before the
-- order date). NULL here means silently un-convertible revenue — fail loud.
select
    fx.order_id,
    fx.currency,
    fx.order_date
from (
    select
        order_id,
        currency,
        date(created_at) as order_date,
        rate_to_eur
    from {{ ref('int_orders_fx') }}
) as fx
where fx.currency <> 'EUR'
  and fx.rate_to_eur is null
```

- [ ] **Step 2: Run `dbt parse` to verify it fails**

Run: `make dbt-parse`
Expected: FAIL — `ref('int_orders_fx')` points to a node that does not exist.

- [ ] **Step 3: Write the model**

`dbt/models/intermediate/int_orders_fx.sql`:

```sql
{{ config(materialized='view') }}

-- Grain: one row per order. Order-grain FX normalization primitive shared
-- by mart_daily_sales (line-level division) and mart_customer_ltv.
-- rate semantics: units of the order currency per 1 EUR (fx API, base EUR);
-- the API quotes no EUR row, so EUR orders carry rate 1.0. "Latest rate on
-- or before the order date" is chosen over "exact day" so ingestion gaps do
-- not strand orders; an order older than every rate keeps rate NULL and is
-- surfaced by the marts_fx_rate_coverage business test (fail-loud policy).
with matched as (
    select
        o.order_id,
        o.customer_id,
        o.customer_key,
        o.currency,
        o.order_status,
        o.order_total,
        o.shipping_cost,
        o.created_at,
        f.rate,
        f.rate_date,
        row_number() over (
            partition by o.order_id
            order by f.rate_date desc
        ) as version_rank
    from {{ ref('fact_orders') }} as o
    left join {{ ref('int_fx_rates_daily') }} as f
        on f.currency = o.currency
       and f.rate_date <= date(o.created_at)
),
normalized as (
    select
        order_id,
        customer_id,
        customer_key,
        currency,
        order_status,
        order_total,
        shipping_cost,
        created_at,
        case when currency = 'EUR' then 1.0 else rate end as rate_to_eur
    from matched
    where version_rank = 1
)
select
    order_id,
    customer_id,
    customer_key,
    currency,
    order_status,
    order_total,
    shipping_cost,
    created_at,
    rate_to_eur,
    order_total / rate_to_eur as order_total_eur,
    shipping_cost / rate_to_eur as shipping_cost_eur
from normalized
```

- [ ] **Step 4: Run `dbt parse` to verify it passes**

Run: `make dbt-parse`
Expected: PASS (manifest contains `int_orders_fx`).

- [ ] **Step 5: Commit**

```bash
git add dbt/models/intermediate/int_orders_fx.sql dbt/tests/marts_fx_rate_coverage.sql
git commit -m "feat(dbt): int_orders_fx order-grain FX normalization primitive (Phase 5 slice 3)"
```

---

### Task 2: `mart_daily_sales`

**Files:**
- Create: `dbt/models/marts/mart_daily_sales.sql`
- Create: `dbt/models/marts/schema.yml` (first contract block; Tasks 3–5 append)
- Modify: `dbt/dbt_project.yml` (add `marts:` layer config)
- Create: `dbt/tests/mart_daily_sales_revenue_reconcile.sql`

**Interfaces:**
- Consumes: `int_orders_fx` (Task 1), `fact_order_items`, `dim_product` (category_name, unit_cost), `dim_customer` (region, via point-in-time `customer_key`), `dim_date` (date_key).
- Produces: table `analytics.mart_daily_sales`, grain **(order_date, category_name, region)**, columns `date_key int, order_date date, category_name varchar, region varchar, orders_count bigint, items_sold bigint, revenue_eur double, margin_eur double`.

- [ ] **Step 1: Add the marts layer to the project config**

In `dbt/dbt_project.yml`, after the `core:` block inside `models: omni_retail:`, add:

```yaml
    marts:
      +materialized: table
      +schema: analytics
```

- [ ] **Step 2: Write the failing tests first**

`dbt/tests/mart_daily_sales_revenue_reconcile.sql`:

```sql
-- Cross-grain reconciliation (Review Focus 5): per order date, the mart's
-- item-line revenue (EUR, realized orders only) must equal the order-grain
-- truth (order_total - shipping_cost) / rate from int_orders_fx. Different
-- source grain catches fan-out/join bugs; both sides exclude cancelled and
-- refunded orders (realized-revenue policy, Global Constraints).
with mart_side as (
    select
        order_date,
        sum(revenue_eur) as revenue_eur
    from {{ ref('mart_daily_sales') }}
    group by order_date
),
truth_side as (
    select
        date(created_at) as order_date,
        sum((order_total - shipping_cost) / rate_to_eur) as revenue_eur
    from {{ ref('int_orders_fx') }}
    where order_status not in ('cancelled', 'refunded')
      and rate_to_eur is not null
    group by date(created_at)
)
select
    coalesce(m.order_date, t.order_date) as order_date,
    m.revenue_eur as mart_revenue_eur,
    t.revenue_eur as truth_revenue_eur
from mart_side as m
full join truth_side as t
    on t.order_date = m.order_date
where m.order_date is null
   or t.order_date is null
   or abs(m.revenue_eur - t.revenue_eur) > 0.01
```

First contract block in `dbt/models/marts/schema.yml`:

```yaml
version: 2

models:
  - name: mart_daily_sales
    description: >
      Daily sales mart. Purpose: GMV/revenue/margin roll-ups for the Revenue
      and Sales dashboards. Grain: one row per (order_date, category_name,
      region). PK: the combination (order_date, category_name, region).
      Measures: orders_count (distinct orders in the group — additive across
      dates, NOT across category/region: a multi-category order counts once
      per row), items_sold, revenue_eur (item-line revenue, excl. shipping),
      margin_eur. Financial measures exclude cancelled and refunded orders
      (realized-revenue policy); orders_count includes all orders. Shipping
      is an order-grain measure and stays in fact_orders/int_orders_fx.
      Upstream: int_orders_fx, fact_order_items, dim_product, dim_customer
      (point-in-time region), dim_date. Known simplification (mock source):
      product prices are currency-naive; line amounts are normalized with
      the order currency's FX rate.
    columns:
      - name: date_key
        description: yyyymmdd integer key; joins dim_date.
        tests: [not_null]
      - name: order_date
        tests: [not_null]
      - name: category_name
        tests: [not_null]
      - name: region
        tests: [not_null]
      - name: orders_count
        tests: [not_null]
      - name: items_sold
        tests: [not_null]
      - name: revenue_eur
        tests: [not_null]
      - name: margin_eur
        tests: [not_null]
```

- [ ] **Step 3: Run `dbt parse` to verify it fails**

Run: `make dbt-parse`
Expected: FAIL — `mart_daily_sales` referenced by test/contract but not modeled.

- [ ] **Step 4: Write the model**

`dbt/models/marts/mart_daily_sales.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per (order date, product category, customer region).
-- Financial measures (revenue_eur, margin_eur, items_sold) cover realized
-- orders only (excludes cancelled/refunded); orders_count counts every
-- order of the group. Region is the customer's point-in-time version
-- (order customer_key -> dim_customer). See schema.yml for the additive
-- orders_count caveat across category/region rows.
with lines as (
    select
        i.order_id,
        fx.customer_key,
        fx.order_status,
        fx.rate_to_eur,
        fx.order_status not in ('cancelled', 'refunded') as is_realized,
        date(fx.created_at) as order_date,
        i.quantity,
        i.line_total / fx.rate_to_eur as line_total_eur,
        (i.unit_price - p.unit_cost) * i.quantity / fx.rate_to_eur as line_margin_eur,
        p.category_name
    from {{ ref('fact_order_items') }} as i
    join {{ ref('int_orders_fx') }} as fx
        on fx.order_id = i.order_id
    join {{ ref('dim_product') }} as p
        on p.product_id = i.product_id
),
enriched as (
    select
        l.order_id,
        l.order_status,
        l.is_realized,
        l.order_date,
        l.category_name,
        c.region,
        l.quantity,
        l.line_total_eur,
        l.line_margin_eur
    from lines as l
    join {{ ref('dim_customer') }} as c
        on c.customer_key = l.customer_key
)
select
    d.date_key,
    e.order_date,
    e.category_name,
    e.region,
    count(distinct e.order_id) as orders_count,
    coalesce(sum(e.quantity) filter (where e.is_realized), 0) as items_sold,
    coalesce(sum(e.line_total_eur) filter (where e.is_realized), 0) as revenue_eur,
    coalesce(sum(e.line_margin_eur) filter (where e.is_realized), 0) as margin_eur
from enriched as e
join {{ ref('dim_date') }} as d
    on d.full_date = e.order_date
group by
    d.date_key,
    e.order_date,
    e.category_name,
    e.region
```

- [ ] **Step 5: Run `dbt parse` to verify it passes**

Run: `make dbt-parse`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dbt/dbt_project.yml dbt/models/marts/ dbt/tests/mart_daily_sales_revenue_reconcile.sql
git commit -m "feat(dbt): mart_daily_sales with EUR normalization (Phase 5 slice 3)"
```

---

### Task 3: `mart_customer_ltv`

**Files:**
- Create: `dbt/models/marts/mart_customer_ltv.sql`
- Modify: `dbt/models/marts/schema.yml` (append contract)
- Create: `dbt/tests/mart_customer_ltv_totals_reconcile.sql`

**Interfaces:**
- Consumes: `int_orders_fx` (Task 1), `dim_customer` (current version: `is_current`).
- Produces: table `analytics.mart_customer_ltv`, grain **customer_id**, columns `customer_id, customer_key (current version key), region, segment (current attributes), first_order_date, last_order_date, orders_count (all orders), gmv_eur (realized order totals incl. shipping, EUR), avg_order_value_eur (gmv / realized orders count)`.

- [ ] **Step 1: Write the failing tests first**

`dbt/tests/mart_customer_ltv_totals_reconcile.sql`:

```sql
-- LTV totals reconcile against order-grain truth: total orders_count equals
-- all fact orders; total gmv_eur equals realized order totals (EUR). LTV
-- gmv uses full order_total (incl. shipping) — distinct from
-- mart_daily_sales.revenue_eur (item lines excl. shipping); both policies
-- are documented in their contracts.
with mart_side as (
    select
        sum(orders_count) as orders_count,
        sum(gmv_eur) as gmv_eur
    from {{ ref('mart_customer_ltv') }}
),
truth_side as (
    select
        count(*) as orders_count,
        coalesce(
            sum(order_total_eur)
            filter (where order_status not in ('cancelled', 'refunded')),
            0
        ) as gmv_eur
    from {{ ref('int_orders_fx') }}
    where rate_to_eur is not null
)
select
    m.orders_count as mart_orders_count,
    t.orders_count as truth_orders_count,
    m.gmv_eur as mart_gmv_eur,
    t.gmv_eur as truth_gmv_eur
from mart_side as m
cross join truth_side as t
where m.orders_count <> t.orders_count
   or abs(m.gmv_eur - t.gmv_eur) > 0.01
```

Contract appended to `dbt/models/marts/schema.yml` (inside `models:`):

```yaml
  - name: mart_customer_ltv
    description: >
      Customer lifetime value mart. Purpose: retention/LTV/activation
      analysis; feeds the Customer dashboard. Grain: one row per customer
      with at least one order. PK: customer_id. Measures: orders_count (all
      orders), gmv_eur (realized order totals incl. shipping, EUR),
      avg_order_value_eur. Attributes region/segment are the customer's
      CURRENT dim_customer version — LTV reports today's segmentation, not
      point-in-time (documented choice). Upstream: int_orders_fx,
      dim_customer (current versions).
    columns:
      - name: customer_id
        tests: [unique, not_null]
      - name: customer_key
        tests: [not_null]
      - name: region
        tests: [not_null]
      - name: segment
        tests:
          - not_null
          - accepted_values:
              values: ["standard", "premium", "vip"]
      - name: first_order_date
        tests: [not_null]
      - name: last_order_date
        tests: [not_null]
      - name: orders_count
        tests: [not_null]
      - name: gmv_eur
        tests: [not_null]
      - name: avg_order_value_eur
        description: NULL only for customers whose every order is cancelled/refunded.
```

- [ ] **Step 2: Run `dbt parse` to verify it fails**

Run: `make dbt-parse`
Expected: FAIL — `mart_customer_ltv` does not exist.

- [ ] **Step 3: Write the model**

`dbt/models/marts/mart_customer_ltv.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per customer with at least one order. region/segment come
-- from the customer's CURRENT SCD2 version (see schema.yml rationale).
-- gmv_eur is realized order value (order_total incl. shipping, EUR);
-- orders_count includes cancelled/refunded orders (activity).
with orders as (
    select
        customer_id,
        order_status,
        order_total_eur,
        date(created_at) as order_date
    from {{ ref('int_orders_fx') }}
),
current_customers as (
    select
        customer_key,
        customer_id,
        region,
        segment
    from {{ ref('dim_customer') }}
    where is_current
),
agg as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(*) as orders_count,
        count(*)
            filter (where order_status not in ('cancelled', 'refunded'))
            as realized_orders_count,
        coalesce(
            sum(order_total_eur)
                filter (where order_status not in ('cancelled', 'refunded')),
            0
        ) as gmv_eur
    from orders
    group by customer_id
)
select
    a.customer_id,
    c.customer_key,
    c.region,
    c.segment,
    a.first_order_date,
    a.last_order_date,
    a.orders_count,
    a.gmv_eur,
    case
        when a.realized_orders_count > 0
            then a.gmv_eur / a.realized_orders_count
    end as avg_order_value_eur
from agg as a
join current_customers as c
    on c.customer_id = a.customer_id
```

- [ ] **Step 4: Run `dbt parse` to verify it passes**

Run: `make dbt-parse`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dbt/models/marts/ dbt/tests/mart_customer_ltv_totals_reconcile.sql
git commit -m "feat(dbt): mart_customer_ltv over current customer attributes (Phase 5 slice 3)"
```

---

### Task 4: `mart_marketing_roi`

**Files:**
- Create: `dbt/models/marts/mart_marketing_roi.sql`
- Modify: `dbt/models/marts/schema.yml` (append contract)
- Create: `dbt/tests/mart_marketing_metrics_valid.sql`

**Interfaces:**
- Consumes: `dim_campaign` (campaign_id, campaign_name, channel, campaign_status, start_date, end_date, budget_eur, spend_eur, impressions, clicks).
- Produces: table `analytics.mart_marketing_roi`, grain **campaign_id**, columns `…, ctr, cpc_eur, cpm_eur, budget_utilization` (all NULL for zero-denominator campaigns; mock source guarantees `clicks <= impressions` and `0 <= spend`, but `spend` may exceed budget by up to 5 % — `budget_utilization` can exceed 1.0).

- [ ] **Step 1: Write the failing tests first**

`dbt/tests/mart_marketing_metrics_valid.sql`:

```sql
-- Media-metric validity (Review Focus 2): clicks cannot exceed impressions;
-- ctr must be a rate in (0, 1]; spend cannot be negative. Zero-impression
-- campaigns (scheduled, day 0) legitimately carry NULL ctr/cpc/cpm and are
-- excluded here — the not_null tests in schema.yml are where-scoped.
select
    campaign_id,
    clicks,
    impressions,
    ctr,
    spend_eur
from {{ ref('mart_marketing_roi') }}
where clicks > impressions
   or clicks < 0
   or spend_eur < 0
   or (ctr is not null and (ctr <= 0 or ctr > 1))
```

Contract appended to `dbt/models/marts/schema.yml`:

```yaml
  - name: mart_marketing_roi
    description: >
      Marketing campaign efficiency mart. Purpose: spend/CTR/CPC/CPM
      monitoring; feeds the Marketing dashboard. Grain: one row per
      campaign. PK: campaign_id. Measures: spend/budget/impressions/clicks
      (current snapshot) and derived ctr, cpc_eur, cpm_eur,
      budget_utilization (can exceed 1.0: the source overspends up to 5%).
      LIMITATION (spec §2): campaigns are not linked to orders, so revenue
      attribution is impossible with current data — no ROAS is computed and
      none may be faked; ROAS arrives with clickstream attribution
      (Phase 9). Upstream: dim_campaign.
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
      - name: impressions
        tests: [not_null]
      - name: clicks
        tests: [not_null]
      - name: spend_eur
        tests: [not_null]
      - name: ctr
        description: clicks / impressions; NULL while a campaign has 0 impressions.
        tests:
          - not_null:
              config:
                where: impressions > 0
      - name: cpc_eur
        tests:
          - not_null:
              config:
                where: clicks > 0
      - name: cpm_eur
        tests:
          - not_null:
              config:
                where: impressions > 0
```

- [ ] **Step 2: Run `dbt parse` to verify it fails**

Run: `make dbt-parse`
Expected: FAIL — `mart_marketing_roi` does not exist.

- [ ] **Step 3: Write the model**

`dbt/models/marts/mart_marketing_roi.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per marketing campaign (current API snapshot).
-- Derived media KPIs are null-safe: scheduled/day-0 campaigns have zero
-- impressions and clicks (division yields NULL, never an error). No ROAS:
-- revenue attribution does not exist in this data (spec §2) — see
-- schema.yml limitation note.
select
    campaign_id,
    campaign_name,
    channel,
    campaign_status,
    start_date,
    end_date,
    budget_eur,
    spend_eur,
    impressions,
    clicks,
    cast(clicks as double) / nullif(impressions, 0) as ctr,
    spend_eur / nullif(cast(clicks as double), 0) as cpc_eur,
    spend_eur * 1000.0 / nullif(cast(impressions as double), 0) as cpm_eur,
    spend_eur / nullif(budget_eur, 0) as budget_utilization
from {{ ref('dim_campaign') }}
```

- [ ] **Step 4: Run `dbt parse` to verify it passes**

Run: `make dbt-parse`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dbt/models/marts/ dbt/tests/mart_marketing_metrics_valid.sql
git commit -m "feat(dbt): mart_marketing_roi with honest (no-ROAS) media KPIs (Phase 5 slice 3)"
```

---

### Task 5: `mart_delivery_performance` + live marts validation

**Files:**
- Create: `dbt/models/marts/mart_delivery_performance.sql`
- Modify: `dbt/models/marts/schema.yml` (append contract)
- Create: `dbt/tests/mart_delivery_status_counts_reconcile.sql`

**Interfaces:**
- Consumes: `int_deliveries` (delivery_id, order_id (synthetic), carrier, status, shipped_at, delivered_at, updated_at). Status domain: delivered / in_transit / delayed / returned.
- Produces: table `analytics.mart_delivery_performance`, grain **carrier**, columns `carrier, delivery_count, delivered_count, in_transit_count, delayed_count, returned_count, avg_transit_hours (over rows with both timestamps), last_update_at`.

- [ ] **Step 1: Write the failing tests first**

`dbt/tests/mart_delivery_status_counts_reconcile.sql`:

```sql
-- Status columns must sum to delivery_count so a NEW delivery-API status
-- cannot disappear silently (a group-by would hide it; fixed columns make
-- the gap observable). Transit cannot be negative.
select
    carrier,
    delivery_count,
    delivered_count + in_transit_count + delayed_count + returned_count
        as status_sum,
    avg_transit_hours
from {{ ref('mart_delivery_performance') }}
where delivered_count + in_transit_count + delayed_count + returned_count
        <> delivery_count
   or avg_transit_hours < 0
```

Contract appended to `dbt/models/marts/schema.yml`:

```yaml
  - name: mart_delivery_performance
    description: >
      Carrier delivery performance mart from the delivery-partner API feed.
      Purpose: carrier scorecard (transit times, status mix) for the
      delivery dashboard. Grain: one row per carrier. PK: carrier.
      Measures: delivery_count, per-status counts (sum-tested), and
      avg_transit_hours averaged over deliveries with both shipped_at and
      delivered_at (delivered/returned). NOTE: the feed's order_id values
      are the partner's synthetic references ("ORD-...") and never join
      OLTP orders (spec §2); this mart is independent of fact_shipments
      (OLTP shipments serve fact_shipments). Upstream: int_deliveries.
    columns:
      - name: carrier
        tests: [unique, not_null]
      - name: delivery_count
        tests: [not_null]
      - name: delivered_count
        tests: [not_null]
      - name: in_transit_count
        tests: [not_null]
      - name: delayed_count
        tests: [not_null]
      - name: returned_count
        tests: [not_null]
      - name: avg_transit_hours
        description: NULL only when the carrier has no completed deliveries yet.
```

- [ ] **Step 2: Run `dbt parse` to verify it fails**

Run: `make dbt-parse`
Expected: FAIL — `mart_delivery_performance` does not exist.

- [ ] **Step 3: Write the model**

`dbt/models/marts/mart_delivery_performance.sql`:

```sql
{{ config(materialized='table') }}

-- Grain: one row per carrier (delivery-partner API feed; synthetic order
-- references — never joined to OLTP orders, see schema.yml). Fixed status
-- columns follow the API status domain; the counts-sum business test makes
-- an unknown new status loud instead of silently dropped.
with deliveries as (
    select
        carrier,
        status,
        updated_at,
        case
            when shipped_at is not null and delivered_at is not null
                then date_diff('minute', shipped_at, delivered_at)
        end as transit_minutes
    from {{ ref('int_deliveries') }}
)
select
    carrier,
    count(*) as delivery_count,
    count(*) filter (where status = 'delivered') as delivered_count,
    count(*) filter (where status = 'in_transit') as in_transit_count,
    count(*) filter (where status = 'delayed') as delayed_count,
    count(*) filter (where status = 'returned') as returned_count,
    avg(transit_minutes) / 60.0 as avg_transit_hours,
    max(updated_at) as last_update_at
from deliveries
group by carrier
```

- [ ] **Step 4: Run `dbt parse` to verify it passes**

Run: `make dbt-parse`
Expected: PASS.

- [ ] **Step 5: Live validation of all marts (core profile must be up)**

Run: `make up && make dbt-build`
Expected: PASS — full `dbt build` including staging → intermediate → core → marts plus all built-in and singular tests. If Bronze is empty locally, first restore a data day, e.g. `make ingest-api ARGS="run --source fx-rates --date 2026-09-18"` and `make bronze-load ARGS="run-all --date 2026-09-18"` (or re-run a PG snapshot day). A near-empty build is also acceptable as a wiring check, but prefer a real day.

- [ ] **Step 6: Commit**

```bash
git add dbt/models/marts/ dbt/tests/mart_delivery_status_counts_reconcile.sql
git commit -m "feat(dbt): mart_delivery_performance carrier scorecard (Phase 5 slice 3)"
```

---

### Task 6: Bronze watermark loader — `run-new`

**Files:**
- Modify: `src/omni_retail/lakehouse/bronze/loader.py` (add `fetch_all` to the executor protocol/impl, `loaded_batch_dates`, `raw_batch_dates`, `load_pending`, `PendingLoadSummary`)
- Modify: `src/omni_retail/lakehouse/bronze/cli.py` (add `run-new` subcommand)
- Test: `tests/unit/lakehouse/bronze/test_loader.py`, `tests/unit/lakehouse/bronze/test_cli.py` (extend existing files; read them first and follow their fake patterns)

**Interfaces:**
- Consumes: `ObjectStorage` (`list_object_keys`, `get_object`), `TrinoExecutor` (gains `fetch_all(sql) -> list[tuple[object, ...]]`), existing `load()`, `TABLES`.
- Produces:
  - `loader.pending_raw_dates(storage, executor, spec, *, catalog="iceberg") -> list[date]` (ascending);
  - `loader.load_pending(storage, executor, *, catalog="iceberg") -> PendingLoadSummary` with fields `loaded_dates: tuple[date, ...]`, `source_count: int`, `row_count: int`;
  - CLI: `python -m omni_retail.lakehouse.bronze run-new`.

- [ ] **Step 1: Write the failing unit tests**

Extend the existing fakes (FakeStorage/FakeExecutor pattern in `tests/unit/lakehouse/bronze/`) — fakes now also implement `fetch_all`. New tests (names indicative, keep file conventions):

```python
def test_pending_raw_dates_skips_already_loaded_partitions():
    # storage advertises raw days 2026-09-20/21/22; bronze already has 20/21
    # -> pending == [2026-09-22] (hole-free replay and holes both covered)


def test_pending_raw_dates_when_table_missing_loads_everything():
    # executor.fetch_all(table_exists query) -> []  => pending == all raw days


def test_pending_raw_dates_empty_archive_is_empty():
    # no raw objects -> []


def test_load_pending_loads_each_pending_day_and_reports_summary():
    # two sources x one pending day each; fake load returns LoadResult rows
    # -> summary.loaded_dates sorted, row_count summed; load() called once
    # per (spec, date)


def test_load_pending_replay_is_noop_when_nothing_pending():
    # everything loaded -> summary row_count 0, loaded_dates == ()


def test_run_new_cli_subcommand_returns_zero(capsys, monkeypatch):
    # argv ["run-new"] with fakes wired in -> exit code 0
```

Implementation must make the date-existence SQL explicit in tests: the table-existence check queries `{catalog}.information_schema.tables` for `table_schema='bronze' and table_name=<spec.name>`; the loaded-dates query is `select distinct "_batch_date" from {catalog}.bronze.<name>`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/lakehouse/bronze -v`
Expected: FAIL — `pending_raw_dates`/`load_pending`/`run-new` do not exist.

- [ ] **Step 3: Implement**

In `loader.py`:

```python
import re  # add to imports


def fetch_all(self: "DbapiTrinoExecutor", sql: str) -> list[tuple[object, ...]]:
    """Run a query and return all rows (SELECT boundary for watermarks)."""
    ...  # cursor.execute(sql); return cursor.fetchall()
```

(Add `fetch_all` to the `TrinoExecutor` Protocol with the same signature; update every fake executor in the unit tests.)

```python
@dataclass(frozen=True)
class PendingLoadSummary:
    """Outcome of one watermark-driven run-new pass over all sources."""

    loaded_dates: tuple[date, ...]
    source_count: int
    row_count: int


def bronze_table_exists(executor: TrinoExecutor, spec: BronzeTableSpec, catalog: str) -> bool:
    """True when the Bronze table for this spec already exists."""
    rows = executor.fetch_all(
        "select 1 from {}.information_schema.tables "
        "where table_schema = '{}' and table_name = '{}'".format(
            catalog, SCHEMA_BRONZE, spec.name
        )
    )
    return len(rows) > 0


def loaded_batch_dates(executor: TrinoExecutor, spec: BronzeTableSpec, catalog: str) -> set[date]:
    """Partition dates already present in one Bronze table (empty if none)."""
    rows = executor.fetch_all(
        'select distinct "{}" from {}.{}.{}'.format(
            BATCH_DATE, catalog, SCHEMA_BRONZE, spec.name
        )
    )
    return {cast(date, row[0]) for row in rows}


def raw_batch_dates(storage: ObjectStorage, spec: BronzeTableSpec) -> set[date]:
    """Logical dates present in the raw archive for one source."""
    root = (
        f"postgres/{spec.name}/" if spec.kind == "postgres" else f"api/{spec.source_name}/"
    )
    date_pattern = (
        re.compile(r"^(\d{4})/(\d{2})/(\d{2})/.+")
        if spec.kind == "postgres"
        else re.compile(r"^(\d{4})(\d{2})(\d{2})/.+")
    )
    found: set[date] = set()
    for key in storage.list_object_keys(BUCKET_ARCHIVE, root):
        match = date_pattern.match(key[len(root):])
        if match:
            year, month, day = (int(part) for part in match.groups())
            found.add(date(year, month, day))
    return found


def pending_raw_dates(
    storage: ObjectStorage,
    executor: TrinoExecutor,
    spec: BronzeTableSpec,
    *,
    catalog: str = "iceberg",
) -> list[date]:
    """Raw days of one source not yet loaded into Bronze, ascending.

    Distinct-partition comparison (not max(_batch_date)) so historical holes
    are backfilled too; already-loaded days are skipped, keeping replays
    idempotent (force a re-load with `run --date`, which replaces the day).
    """
    if not bronze_table_exists(executor, spec, catalog):
        return sorted(raw_batch_dates(storage, spec))
    return sorted(raw_batch_dates(storage, spec) - loaded_batch_dates(executor, spec, catalog))


def load_pending(
    storage: ObjectStorage,
    executor: TrinoExecutor,
    *,
    catalog: str = "iceberg",
) -> PendingLoadSummary:
    """Load every pending (source, raw day) into Bronze; watermark-driven."""
    log = context_logger(__name__)
    loaded_dates: set[date] = set()
    row_count = 0
    for spec in TABLES.values():
        for logical_date in pending_raw_dates(storage, executor, spec, catalog=catalog):
            result = load(
                storage, executor, spec, logical_date=logical_date, catalog=catalog
            )
            if result.status == "loaded":
                row_count += result.row_count
            loaded_dates.add(logical_date)
    log.info(
        "run-new completed: sources=%d loaded_dates=%s row_count=%d",
        len(TABLES),
        ",".join(day.isoformat() for day in sorted(loaded_dates)) or "-",
        row_count,
    )
    return PendingLoadSummary(tuple(sorted(loaded_dates)), len(TABLES), row_count)
```

In `cli.py`: add the subparser and dispatch (follow the existing `run-all` wiring):

```python
    run_new_parser = subparsers.add_parser(
        "run-new",
        help="load every raw day not yet in Bronze (watermark-driven; idempotent)",
    )
```

and in `main()`, after the executor is opened:

```python
            if args.command == "run-new":
                summary = load_pending(storage, executor)
                logger.info(
                    "run-new: loaded_dates=%s row_count=%d",
                    ",".join(day.isoformat() for day in summary.loaded_dates) or "-",
                    summary.row_count,
                )
                return 0
```

(Import `load_pending` alongside the existing loader imports; adjust `spec` resolution — `run-new` resolves no spec.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/lakehouse/bronze -v && make lint`
Expected: PASS (all bronze tests, ruff, mypy per repo policy).

- [ ] **Step 5: Commit**

```bash
git add src/omni_retail/lakehouse/bronze/ tests/unit/lakehouse/bronze/
git commit -m "feat(lakehouse): watermark-driven bronze run-new loader (Phase 5 slice 3)"
```

---

### Task 7: Airflow — `lakehouse://bronze` dataset + `transform_lakehouse` DAG

**Files:**
- Create: `airflow/include/datasets.py`
- Create: `airflow/dags/transform_lakehouse.py`
- Modify: `airflow/include/api_dag_factory.py` (outlets param)
- Modify: `airflow/dags/ingest_fx_api.py`, `ingest_marketing_api.py`, `ingest_delivery_api.py` (pass outlets)
- Modify: `airflow/dags/ingest_postgres_snapshot.py` (DAG-level outlets)
- Modify: `airflow/include/policy.py` (TRANSFORM_TASK_DEFAULT_ARGS)
- Modify: `airflow/include/runners.py` (`run_bronze_load_new`)
- Test: `airflow/tests/test_dags.py`

**Interfaces:**
- Consumes: `load_pending` (Task 6), existing factory/policy/runner patterns, `Dataset` from `airflow.datasets`.
- Produces: module constant `include.datasets.BRONZE_DATASET` (`uri="lakehouse://bronze"`); runner `run_bronze_load_new() -> dict[str, object]` (`{"loaded_dates": [iso...], "sources": int, "row_count": int}`); DAG `transform_lakehouse` with tasks `load_bronze >> dbt_build`.

- [ ] **Step 1: Write the failing DAG tests**

In `airflow/tests/test_dags.py` (runs inside the image; follow existing style):

```python
from airflow.datasets import Dataset  # add to imports

EXPECTED_DAG_IDS = {
    ...,
    "transform_lakehouse",
}

INGESTION_DAG_IDS = EXPECTED_DAG_IDS - {"transform_lakehouse"}
```

Re-point the `test_dag_schedule_policy` and `test_dag_task_retry_and_timeout_policy` parametrizations from `EXPECTED_DAG_IDS` to `INGESTION_DAG_IDS` (the transform DAG is dataset-scheduled and has its own task policy). New tests:

```python
BRONZE_DATASET_URI = "lakehouse://bronze"


def test_transform_dag_is_scheduled_on_bronze_dataset(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["transform_lakehouse"]
    schedule = dag.schedule
    assert isinstance(schedule, list) and len(schedule) == 1
    assert schedule[0].uri == BRONZE_DATASET_URI


def test_transform_dag_task_structure(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["transform_lakehouse"]
    assert [task.task_id for task in dag.tasks] == ["load_bronze", "dbt_build"]
    assert dag.get_task("dbt_build").upstream_task_ids == {"load_bronze"}


def test_transform_dag_task_policy(dag_bag: DagBag) -> None:
    dag = dag_bag.dags["transform_lakehouse"]
    for task in dag.tasks:
        assert task.retries == 2
        assert task.retry_exponential_backoff is True
    assert dag.get_task("load_bronze").execution_timeout == timedelta(minutes=20)
    assert dag.get_task("dbt_build").execution_timeout == timedelta(minutes=30)


def test_transform_dbt_command_writes_outside_readonly_mount(dag_bag: DagBag) -> None:
    """dbt project is mounted read-only; artifacts must go to /tmp."""
    command = dag_bag.dags["transform_lakehouse"].get_task("dbt_build").bash_command
    assert "--project-dir /opt/airflow/dbt" in command
    assert "--target-path /tmp/dbt-target" in command
    assert "--log-path /tmp/dbt-logs" in command


@pytest.mark.parametrize("dag_id", API_DAG_IDS)
def test_api_dag_publishes_bronze_dataset(dag_bag: DagBag, dag_id: str) -> None:
    task = dag_bag.dags[dag_id].get_task("ingest")
    assert [outlet.uri for outlet in task.outlets] == [BRONZE_DATASET_URI]


def test_postgres_snapshot_publishes_bronze_dataset(dag_bag: DagBag) -> None:
    """DAG-level outlets land on every leaf (all seven snapshot tasks)."""
    dag = dag_bag.dags["ingest_postgres_snapshot"]
    for task in dag.tasks:
        assert BRONZE_DATASET_URI in [outlet.uri for outlet in task.outlets]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make airflow-test` (builds the current image; Task 8 bumps the tag — run as-is for now)
Expected: FAIL — `transform_lakehouse` missing from DagBag; outlets empty.

- [ ] **Step 3: Implement the Airflow wiring**

`airflow/include/datasets.py`:

```python
"""Datasets published by ingestion DAGs (Phase 5 design spec §10 slice 3)."""

from airflow.datasets import Dataset

#: Published by every ingestion DAG that feeds the Bronze layer. The event
#: means "new raw data may be pending in the archive bucket"; consumers are
#: watermark-driven and treat extra events as cheap no-ops.
BRONZE_DATASET = Dataset(
    uri="lakehouse://bronze",
    extra={"description": "raw archive updated; Bronze refresh pending"},
)
```

`airflow/include/policy.py` (append):

```python
#: Transform tasks (Phase 5 slice 3): full-recompute workloads are
#: idempotent, so fewer retries with longer per-attempt timeouts.
TRANSFORM_TASK_DEFAULT_ARGS: dict[str, object] = {
    "retries": 2,
    "retry_delay": timedelta(seconds=60),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}
```

`airflow/include/runners.py` (append; import `load_pending` from the bronze loader):

```python
def run_bronze_load_new() -> dict[str, object]:
    """Load every pending raw day into Bronze (watermark-driven run-new)."""
    configure_logging()
    storage = BotoObjectStorage(StorageConfig.from_env())
    with contextlib.closing(DbapiTrinoExecutor(TrinoConfig.from_env())) as executor:
        summary = load_pending(storage, executor)
    return {
        "loaded_dates": [day.isoformat() for day in summary.loaded_dates],
        "sources": summary.source_count,
        "row_count": summary.row_count,
    }
```

`airflow/include/api_dag_factory.py` — add parameter and pass to the task decorator:

```python
def build_api_ingestion_dag(
    *,
    dag_id: str,
    source_name: str,
    start_date: datetime,
    schedule: str = "@daily",
    pool: str = "mock_api",
    outlets: list[Dataset] | None = None,
) -> DAG:
```

```python
        @task(pool=pool, outlets=outlets)
        def ingest(ds: str | None = None) -> dict[str, object]:
```

(import `Dataset` from `airflow.datasets` in the factory module).

Each API DAG file (fx / marketing / delivery) adds the outlet:

```python
from include.datasets import BRONZE_DATASET

dag = build_api_ingestion_dag(
    dag_id="ingest_fx_api",
    source_name="fx-rates",
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    outlets=[BRONZE_DATASET],
)
```

`ingest_postgres_snapshot.py` — add to the `@dag(...)` kwargs (DAG-level outlets attach to leaf tasks; all seven snapshot tasks are leaves):

```python
from include.datasets import BRONZE_DATASET
...
    outlets=[BRONZE_DATASET],
```

`airflow/dags/transform_lakehouse.py`:

```python
"""Airflow DAG: raw archive -> Iceberg Bronze -> dbt build (Silver/Gold/marts).

Triggered by the ``lakehouse://bronze`` dataset published by the ingestion
DAGs. The dataset event only means "new raw data may be pending" — tasks
are watermark-driven, not bound to the trigger's logical date (which is the
trigger timestamp and differs from the producers' logical dates).
Re-triggers without pending raw data are cheap no-ops: Bronze loads only
missing days; dbt rebuilds are deterministic full recomputes. The dbt
project is mounted read-only at /opt/airflow/dbt; artifacts go to /tmp.
"""

from datetime import UTC, datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from include.datasets import BRONZE_DATASET
from include.policy import TRANSFORM_TASK_DEFAULT_ARGS
from include.runners import run_bronze_load_new

DBT_PROJECT_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="transform_lakehouse",
    schedule=[BRONZE_DATASET],
    start_date=datetime(2026, 9, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=TRANSFORM_TASK_DEFAULT_ARGS,
    tags=["lakehouse", "dbt", "transform"],
    doc_md=(
        "Dataset-triggered lakehouse transform: `load_bronze` (watermark-driven "
        "run-new over all sources) then `dbt build` (models + tests) inside this "
        "image. Several ingestion DAGs may publish the dataset within one day; "
        "each event can queue a run, but extra runs without pending raw data "
        "are no-ops. `dbt build` failing tests fail the DAG (fail-loud policy, "
        "spec §6); data is deterministically recomputed on retry."
    ),
)
def build_transform_lakehouse_dag() -> None:
    @task(task_id="load_bronze", execution_timeout=timedelta(minutes=20))
    def load_bronze() -> dict[str, object]:
        return run_bronze_load_new()

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "dbt build"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROJECT_DIR}"
            " --target-path /tmp/dbt-target"
            " --log-path /tmp/dbt-logs"
            " --no-use-colors"
        ),
        execution_timeout=timedelta(minutes=30),
    )

    load_bronze() >> dbt_build


dag = build_transform_lakehouse_dag()
```

(Remove the unused `Dataset` import if ruff flags it.)

- [ ] **Step 4: Run DAG tests to verify they pass**

Run: `make airflow-test`
Expected: PASS — all DAG tests including the new ones. If `dag.schedule` is not a list on this Airflow version, adapt the assertion to `dag.timetable.summary == "lakehouse://bronze"` (verify inside the container; keep the test intent).

- [ ] **Step 5: Commit**

```bash
git add airflow/dags/ airflow/include/ airflow/tests/test_dags.py
git commit -m "feat(airflow): lakehouse://bronze dataset and transform_lakehouse DAG (Phase 5 slice 3)"
```

---

### Task 8: Infra wiring — dbt CLI on PATH, image 0.2.0, compose env/mount, guards, CI

**Files:**
- Modify: `infrastructure/airflow/Dockerfile`
- Modify: `docker-compose.yml` (image tag ×3, `x-airflow-env`, `x-airflow-volumes`)
- Modify: `infrastructure/scripts/airflow_tests.sh` (image tag)
- Modify: `Makefile` (airflow-build comment/help text tag)
- Modify: `.github/workflows/ci.yml` (tag + `dbt --version` step)
- Modify: `tests/test_compose_guards.py` (tag, marts guard, airflow env/mount guards)

**Interfaces:**
- Consumes: Task 7 DAG (needs `dbt` on PATH and the compose env `TRINO_HOST=trino` and the `./dbt` mount).
- Produces: image `omni-retail/airflow:0.2.0` with a working `dbt` CLI; compose orchestration services configured for the transform DAG.

- [ ] **Step 1: Write/extend the failing guard tests**

In `tests/test_compose_guards.py`:

```python
AIRFLOW_IMAGE = "omni-retail/airflow:0.2.0"  # bump from 0.1.0

DBT_MARTS_MODELS = {
    "mart_daily_sales.sql",
    "mart_customer_ltv.sql",
    "mart_marketing_roi.sql",
    "mart_delivery_performance.sql",
}
```

New tests (follow the file's existing yaml-parsing helpers):

```python
def test_airflow_env_points_trino_at_compose_network() -> None:
    """Transform DAG (slice 3) reaches Trino by service name, not 127.0.0.1."""
    compose = _load_compose()  # reuse the file's existing loader helper
    env = compose["x-airflow-env"]
    assert env["TRINO_HOST"] == "trino"


def test_airflow_mounts_dbt_project_readonly() -> None:
    volumes = compose["x-airflow-volumes"]
    assert "./dbt:/opt/airflow/dbt:ro" in volumes


def test_dbt_marts_models_committed() -> None:
    models = {path.name for path in (DBT_DIR / "models" / "marts").glob("*.sql")}
    assert models == DBT_MARTS_MODELS
```

Also extend the marts-dir assertion wherever `DBT_STAGING_MODELS` is asserted (line ~419 pattern) — mirror it for `models/marts`.

- [ ] **Step 2: Run guard tests to verify they fail**

Run: `uv run pytest tests/test_compose_guards.py -v`
Expected: FAIL — tag mismatch, TRINO_HOST/mount missing, marts guard passes already (Task 2–5 committed the models) but the env/mount tests fail.

- [ ] **Step 3: Apply the infra changes**

`infrastructure/airflow/Dockerfile` — after the `ARG AIRFLOW_SITE_PACKAGES=...` line:

```dockerfile
# Console scripts of the --target install (dbt, ...) land in
# <site-packages>/bin, which is NOT on the default PATH; expose it so DAG
# tasks can invoke the dbt CLI (Phase 5 slice 3).
ENV PATH="${AIRFLOW_SITE_PACKAGES}/bin:${PATH}"
```

`docker-compose.yml`:
- replace all three `image: omni-retail/airflow:0.1.0` with `omni-retail/airflow:0.2.0`;
- in `x-airflow-env`, append:

```yaml
  # Phase 5 slice 3: transform DAG (bronze loader + dbt) runs inside the
  # compose network; host-configured TRINO_HOST=127.0.0.1 does not apply.
  TRINO_HOST: trino
```

- in `x-airflow-volumes`, append:

```yaml
  # dbt project for the transform_lakehouse DAG; read-only — dbt artifacts
  # go to /tmp inside the container (--target-path/--log-path flags).
  - ./dbt:/opt/airflow/dbt:ro
```

`infrastructure/scripts/airflow_tests.sh`: `IMAGE="omni-retail/airflow:0.2.0"`.

`Makefile`: update the `airflow-build` comment/help to `0.2.0`.

`.github/workflows/ci.yml` — in the `airflow` job: update the build `tags:` to `omni-retail/airflow:0.2.0` and add after the build step:

```yaml
      - name: dbt CLI is available in the Airflow image
        run: docker run --rm omni-retail/airflow:0.2.0 dbt --version
```

- [ ] **Step 4: Verify everything passes**

Run:

```bash
grep -rn "omni-retail/airflow:0.1.0" . --exclude-dir=.git --exclude-dir=dbt  # expected: no hits
uv run pytest tests/test_compose_guards.py -v          # PASS
make airflow-build                                     # builds 0.2.0 (slow, once)
docker run --rm omni-retail/airflow:0.2.0 dbt --version  # PASS (PATH fix works)
make airflow-test                                      # PASS (uses new tag)
make lint && make test                                 # PASS
```

If the image build fails on an Airflow/dbt dependency conflict (spec §7 risk): STOP, report, and propose the fallback (separate dbt image) as an ADR discussion — do not improvise.

- [ ] **Step 5: Commit**

```bash
git add infrastructure/airflow/Dockerfile docker-compose.yml infrastructure/scripts/airflow_tests.sh Makefile .github/workflows/ci.yml tests/test_compose_guards.py
git commit -m "feat(infra): dbt CLI on PATH in airflow image 0.2.0, compose wiring for transform DAG (Phase 5 slice 3)"
```

---

### Task 9: Live integration test — `transform_lakehouse` end-to-end

**Files:**
- Create: `tests/integration/test_transform_dag.py`

**Interfaces:**
- Consumes: `put_parquet`, `put_api_pages`, `trino_scalar`, `bronze_table_exists` from sibling module `test_dbt_core_build` (same directory is on `sys.path` under pytest); `make up` + orchestration profile running with image 0.2.0 (`make airflow-build && make airflow-up` — manual preconditions, documented in the module docstring).
- Produces: integration proof that the dataset-triggered DAG loads pending raw days into Bronze and publishes all four marts with correct EUR math (ROADMAP Phase 5 slice-3 acceptance).

- [ ] **Step 1: Write the test**

`tests/integration/test_transform_dag.py` — deterministic one-day world (DAY_3 = 2026-09-22), reuse helpers by import:

```python
"""Live integration: the transform_lakehouse DAG end-to-end via dags test.

Requires the core AND orchestration profiles on the new image
(``make up && make airflow-build && make airflow-up``) plus
OMNI_INTEGRATION=1 (``make integration``). Seeds a deterministic one-day
raw world, executes the dataset-triggered DAG inside the scheduler
container (its own logical date is irrelevant — watermark-driven run-new
must find DAY_3), then asserts Bronze partitions and all four analytics
marts, including exact EUR normalization. Skips (not fails) when Airflow is
not reachable, so `make integration` still works with core-only stacks.
"""

import os
import subprocess
import urllib.error
import urllib.request
from datetime import UTC, date, datetime, time
from decimal import Decimal

import pytest

from omni_retail.ingestion.common.storage import BotoObjectStorage
from test_dbt_core_build import (
    bronze_table_exists,
    put_api_pages,
    put_parquet,
    reset_bronze,
    trino_scalar,
)

DAY_3 = date(2026, 9, 22)
REPO_ROOT = __file__.rsplit("/tests/", 1)[0]  # repo root for docker compose

FX_USD_PER_EUR = 1.1
FX_GBP_PER_EUR = 0.8
```

Skip gate + fixture:

```python
def airflow_reachable() -> bool:
    port = os.environ.get("AIRFLOW_WEBSERVER_PORT", "8081")
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/health", timeout=3
        ) as response:
            return response.status == 200
    except urllib.error.URLError:
        return False


pytestmark = pytest.mark.skipif(
    not airflow_reachable(),
    reason="orchestration profile (airflow) not running: make airflow-up",
)
```

Seeding — explicit dict-records converted through the snapshot specs (order-independent, mirrors `seed_day_1` payload shapes for the three API envelopes):

```python
def rows_from_dicts(table: str, records: list[dict[str, object]]) -> list[tuple[object, ...]]:
    spec = table_by_name(table)
    return [tuple(record[column.name] for column in spec.columns) for record in records]
```

World (2 categories, 2 products, 3 customers; 4 orders incl. cancelled + USD/EUR/GBP mix; payments reconcile 1:1 with consistent status pairs; one delivered shipment):

- `P1`: category A, price 10.00, cost 6.00; `P2`: category B, price 50.00, cost 30.00;
- `C1` region "EMEA" standard, `C2` "NA" premium, `C3` "EMEA" vip (single version each — SCD2 is covered by the slice-2 test);
- `O1` C1 delivered USD: P1×2 + P2×1, shipping 5.00, total 75.00, payment captured 75.00, shipment delivered (shipped 09:00 → delivered +24h);
- `O2` C2 cancelled EUR: P1×1, shipping 0, total 10.00, payment cancelled 10.00;
- `O3` C1 pending GBP: P2×1, shipping 5.00, total 55.00, payment pending 55.00;
- `O4` C3 paid USD: P1×3, shipping 0, total 30.00, payment captured 30.00;
- all `created_at`/`updated_at` within DAY_3 (e.g. 08:00–09:00 UTC);
- fx page for DAY_3: rates USD 1.1, GBP 0.8 (envelope `{"base": "EUR", "date": ..., "rates": [...]}`);
- campaigns page: 2 campaigns — active (impressions 1000, clicks 50, spend 100.00, budget 1000.00) and scheduled (all zeros);
- deliveries page: 4 — DHL delivered 24h, DHL in_transit, UPS delayed, GLS delivered 48h.

Fixture: `reset_bronze()`, purge DAY_3 raw namespaces (postgres `*/2026/09/22/`, api `*/20260922/`, manifests for both kinds), seed all ten sources, `yield`, then teardown (delete DAY_3 bronze partitions, purge DAY_3 raw) — follow the `seeded_world` pattern in `test_dbt_core_build.py`, DAY_3-only.

- [ ] **Step 2: Run the DAG and assert**

```python
def test_transform_lakehouse_dag_publishes_marts(seeded_day3: BotoObjectStorage) -> None:
    result = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "airflow-scheduler",
            "airflow",
            "dags",
            "test",
            "transform_lakehouse",
            "2026-09-22",
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=900,
    )
    assert result.returncode == 0, (
        f"airflow dags test failed:\nstdout:\n{result.stdout[-4000:]}\nstderr:\n{result.stderr[-2000:]}"
    )

    # Bronze picked up DAY_3 for every source (watermark-driven).
    assert trino_scalar(
        'select count(*) from iceberg.bronze.orders where "_batch_date" = '
        f"DATE '{DAY_3:%Y-%m-%d}'"
    ) == 4

    # mart_daily_sales: exact rows and EUR math (Review Focus 5 scenario).
    rows = trino_rows(  # small helper: trino.dbapi fetchall, mirroring trino_scalar
        "select category_name, region, orders_count, items_sold, "
        "round(revenue_eur, 4), round(margin_eur, 4) "
        f"from iceberg.analytics.mart_daily_sales where order_date = DATE '{DAY_3:%Y-%m-%d}' "
        "order by category_name, region"
    )
    expected = [
        ("A", "EMEA", 2, 5, 45.4545, 18.1818),   # (20+30)/1.1 ; margin (2+3)*4/1.1
        ("A", "NA", 1, 0, 0.0, 0.0),             # cancelled order: count only
        ("B", "EMEA", 2, 2, 107.9545, 43.1818),  # 50/1.1 + 50/0.8 ; 20/1.1 + 20/0.8
    ]
    assert [tuple(row) for row in rows] == expected

    # mart_customer_ltv: 3 customers, C1 gmv = 75/1.1 + 55/0.8.
    assert trino_scalar("select count(*) from iceberg.analytics.mart_customer_ltv") == 3
    c1 = trino_scalar(
        "select round(gmv_eur, 4) from iceberg.analytics.mart_customer_ltv "
        "where customer_id = 1"
    )
    assert c1 == 136.9318  # 68.1818 + 68.75

    # mart_marketing_roi: 2 campaigns; zero-impression campaign keeps NULL ctr.
    assert trino_scalar("select count(*) from iceberg.analytics.mart_marketing_roi") == 2
    assert trino_scalar(
        "select count(*) from iceberg.analytics.mart_marketing_roi where ctr is null"
    ) == 1

    # mart_delivery_performance: 3 carriers; DHL avg transit 24h.
    assert trino_scalar(
        "select count(*) from iceberg.analytics.mart_delivery_performance"
    ) == 3
    dhl = trino_scalar(
        "select round(avg_transit_hours, 4) from "
        "iceberg.analytics.mart_delivery_performance where carrier = 'DHL'"
    )
    assert dhl == 24.0
```

(Add the `trino_rows` helper next to `trino_scalar` import usage — either import-style or a 6-line local helper via `trino.dbapi`. Use the concrete customer/order ids actually seeded.)

- [ ] **Step 3: Run it live**

Preconditions (document in report): `make up`, rebuild + recreate orchestration on the new image (`make airflow-build && make airflow-up`), then:

Run: `make integration`
Expected: PASS — including the pre-existing integration tests. If `dags test` output shows dbt test failures, fix data or models per the fail-loud policy — never weaken assertions.

- [ ] **Step 4: Commit**

```bash
git add tests/integration/test_transform_dag.py
git commit -m "test(integration): transform_lakehouse DAG end-to-end with marts assertions (Phase 5 slice 3)"
```

---

### Task 10: dbt docs, documentation, final validation, PR

**Files:**
- Modify: `Makefile` (dbt-docs target)
- Modify: `README.md` (Phase 5 status + slice 3 section + dbt docs)
- Modify: `docs/data-model.md` (analytics layer section)

**Interfaces:**
- Consumes: everything above.
- Produces: `make dbt-docs` (generates `dbt/target/index.html` with lineage + catalog); documentation coverage for ROADMAP Phase 5 acceptance "lineage виден хотя бы в dbt docs".

- [ ] **Step 1: Add the Makefile target** (after `dbt-test`):

```makefile
dbt-docs: ## Generate dbt docs (lineage + catalog; live core stack required) then print serve hint
	@bash -c 'set -a; source .env; set +a; $(UV) run dbt docs generate --project-dir dbt --profiles-dir dbt' \
		&& echo "Open with: cd dbt && $$(basename $(UV)) run dbt docs serve --project-dir . --profiles-dir ."
```

- [ ] **Step 2: README**

- Phase checklist (line ~56): mark Phase 5 complete: `- [x] Phase 5 — dbt + Trino lakehouse (Bronze loader, Silver/Gold Kimball + SCD2, 4 marts, dataset-triggered transform DAG)`.
- New section `## Analytics marts and lakehouse orchestration (Phase 5 slice 3)` after the slice-2 section: the four marts (grain + policy in two sentences each), the `lakehouse://bronze` dataset flow (`ingest_*` → dataset → `transform_lakehouse`: `load_bronze` → `dbt build`), watermark semantics (`run-new`), and `make dbt-docs` usage. Keep portfolio tone; no internal dump.

- [ ] **Step 3: data-model.md**

Append after the Gold section:

```markdown
## Analytics layer (Phase 5 slice 3)

| Mart | Grain | PK | Key measures | Upstream |
|---|---|---|---|---|
| `mart_daily_sales` | (order_date, category, region) | combination | orders_count, items_sold, revenue_eur, margin_eur | int_orders_fx, fact_order_items, dim_product, dim_customer, dim_date |
| `mart_customer_ltv` | customer | customer_id | orders_count, gmv_eur, avg_order_value_eur | int_orders_fx, dim_customer (current) |
| `mart_marketing_roi` | campaign | campaign_id | spend, ctr, cpc_eur, cpm_eur, budget_utilization | dim_campaign |
| `mart_delivery_performance` | carrier | carrier | status counts, avg_transit_hours | int_deliveries |
```

Plus prose covering: `int_orders_fx` (rate semantics units-per-EUR, latest-on-or-before, EUR=1.0, NULL fail-loud); realized-revenue policy; `orders_count` additive caveat; LTV current-attributes choice; no-ROAS limitation; delivery-feed independence. Also update the slice-2 "Known limitations" bullet that deferred currency normalization to marts.

- [ ] **Step 4: Full local validation**

```bash
make lint && make test
make dbt-parse
make up && make dbt-build          # live: full build incl. marts + all tests
make integration                   # live: all integration tests incl. transform DAG
make airflow-test
```

Expected: all PASS. Then regenerate and inspect lineage once: `make dbt-docs` (manual: open `dbt/target/index.html`, confirm `mart_daily_sales` upstream chain reaches bronze sources).

- [ ] **Step 5: Push and open PR**

```bash
git checkout -b feature/phase5-slice3-marts-orchestration   # if not already
git push -u origin feature/phase5-slice3-marts-orchestration
gh pr create --title "feat(dbt): analytics marts + dataset-triggered transform_lakehouse DAG (Phase 5 slice 3)" \
  --body "Implements Phase 5 slice 3 per docs/superpowers/specs/2026-09-18-phase5-dbt-lakehouse-design.md §10. See docs/superpowers/plans/2026-09-19-phase5-slice3-marts-orchestration.md. Deviation from spec sketch: watermark-driven run-new instead of run-all --date ds (dataset-trigger logical date != producer logical date; see plan header)." || true
```

- [ ] **Step 6: Report**

Produce the AGENTS §49 completion report (implemented, changed files, validation with actual results, manual verification, known limitations, follow-up — expected follow-up: Phase 6 ClickHouse serving, plus the deferred bronze-for-file-sources issue from spec §11).

---

## Self-review notes

- Spec §10 slice 3 coverage: marts ×4 (Tasks 2–5) + Airflow dataset/DAG (Task 7) + dbt in Airflow image (Tasks 7–8, incl. the PATH fix the spec's §7 risk anticipated) + dbt docs (Task 10) + README (Task 10) + финальная integration-проверка через `airflow dags test` (Task 9). ✓
- Spec acceptance table: "marts имеют documentation" — schema.yml contracts (Tasks 2–5); "lineage в dbt docs" — Task 10.
- Deviations: exactly one (run-new vs run-all --date ds), flagged in the header with rationale; task granularity of the four marts follows slice-2 precedent.
- Type consistency: `rate_to_eur` naming used in Task 1 model, Task 2/3 models, both reconcile tests, Task 9 expectations; `PendingLoadSummary`/`load_pending`/`run-new` names consistent across Task 6/7/9; image tag `0.2.0` consistent across Task 8 files; DAY_3 = 2026-09-22 distinct from slice-2's DAY_1/DAY_2.
- Review Focus items each map to a concrete test: (1) Task 1 singular + Task 9 full-coverage seed; (2) Task 4 nullif + where-scoped not_null + validity test; (3) Task 8 ENV PATH + CI step; (4) Task 6 unit tests + Task 9 end-to-end; (5) Task 2 reconcile test + contract caveat + Task 9 exact values.
