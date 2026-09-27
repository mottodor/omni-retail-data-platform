# Dashboard screenshots

ROADMAP Phase 7 acceptance: every delivered dashboard has a screenshot in
the main README. Screenshots are a one-time visual artifact — they are
captured manually from the web UI (no browser exists in the agent
environment) and committed here as PNG files.

## Capture procedure

With the BI stack up (`make up && make bi-up`), open each dashboard, let
all charts finish loading, and capture the full page (browser screenshot
tool of choice) at a typical desktop width (~1600px):

| Dashboard | URL | Save as |
| --- | --- | --- |
| Sales | `http://127.0.0.1:8088/superset/dashboard/sales/` | `sales-dashboard.png` |
| Executive | `http://127.0.0.1:8088/superset/dashboard/executive/` | `executive-dashboard.png` |
| Customer | `http://127.0.0.1:8088/superset/dashboard/customer/` | `customer-dashboard.png` |
| Marketing | `http://127.0.0.1:8088/superset/dashboard/marketing/` | `marketing-dashboard.png` |

Login: `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` from `.env`.

The README gallery table links these files by name — no README edit is
needed, just drop the PNGs into this directory and commit.

Notes:

- chart data reflects the last publication run; if the marts were rebuilt
  since, let the dashboards refresh before capturing;
- native filters default to "No filter" — capture the unfiltered view;
- no secrets appear in dashboard views: connections and credentials are
  admin-only metadata objects.
