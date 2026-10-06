# Superset BI assets (Phase 7, ADR 0005)

BI-as-code loop: datasets and dashboards are built in the UI (or via the
REST API), exported as import/export v1 ZIP bundles, sanitized, and
committed here; `superset-init` re-imports every bundle on each
`make bi-up`, so the repository stays the source of truth for BI assets and
a clean clone converges to the same BI state from this tree plus `.env`.

- `../superset_config.py` — Superset deployment config (mounted into the
  containers; secrets come from `.env`, never from this tree);
- `assets/datasets.zip` — the four mart datasets on the ClickHouse
  connection, with documented columns and saved metrics
  (`revenue`, `margin`, `orders`, `items_sold`, `aov` on
  `mart_daily_sales`, and the equivalents on the other marts);
- `assets/sales_dashboard.zip` — the **Sales** dashboard: revenue / margin /
  orders / AOV KPI tiles, revenue-and-margin and AOV daily trends, orders
  and items sold, revenue by category (bar) and region (pie), and a top
  categories × regions table, with a native order-date time filter;
- `assets/executive_dashboard.zip` — the **Executive** dashboard: GMV /
  revenue / margin / orders / AOV / items KPI tiles, monthly revenue-and-
  margin and orders trends, region and category mix, and delivered-vs-
  delayed by carrier (delivery ops). GMV is lifetime (customer grain) and
  is excluded from the order-date filter; conversion is not claimed because
  this repository has no clickstream/attribution source;
- `assets/customer_dashboard.zip` — the **Customer** dashboard: customers /
  GMV / orders / repeat-rate tiles, new-vs-repeat composition and repeat
  rate by first-order cohort month, GMV by segment and region, GMV per
  customer by segment, avg-order-value distribution (histogram), top
  customers by GMV, with a native first-order-date filter. Segment/region
  reflect the CURRENT customer version; retention curves/RFM need richer
  event history (documented limitation);
- `assets/marketing_dashboard.zip` — the **Marketing** dashboard:
  impressions / clicks / spend / CTR tiles, spend vs budget, CTR, CPC/CPM
  by channel, budget utilization by campaign, and a campaign scorecard
  table, with a native campaign-start-date filter. ROAS/CAC are not
  computable because the delivered sources contain no revenue attribution.

Bundles are sanitized exports (`infrastructure/scripts/superset_bundle_sanitize.py`):
no `databases/` entries (they would carry the ClickHouse URI **with the
password**), canonical `metadata.yaml` (`type: assets`), deterministic
archive bytes. `superset-init` injects the env-built database config at
import time, keyed by the fixed connection UUIDs — credentials never touch
this tree. Guarded by
`tests/unit/serving/superset/test_superset_assets.py`.

The exact round-trip commands (export → sanitize → commit → re-import) and
operational procedures live in `docs/runbooks/superset.md`.

Notes and known simplifications:

- `orders` (and therefore `aov`) sums the per-(date, category, region)
  distinct order counts, so totals across category/region overcount
  multi-category orders — additive across dates only, as documented in
  `dbt/models/marts/schema.yml` and in the metric descriptions;
- "top products" would need a product-grain mart; the Sales dashboard's
  top table works at category × region grain (the finest the published
  marts provide);
- ROAS/CAC and funnel/conversion metrics are outside this repository's final
  scope because no clickstream/attribution source is delivered.
