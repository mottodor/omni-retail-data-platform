# Dashboard screenshots

ROADMAP Phase 7 acceptance: every delivered dashboard has a screenshot in
the main README. Screenshots are one-time visual artifacts captured manually
from the web UI and committed here as JPEG files.

## Capture procedure

With the BI stack up (`make up && make bi-up`), open each dashboard, let
all charts finish loading, and capture the full page (browser screenshot
tool of choice) at a typical desktop width (~1600px):

| Dashboard | URL | Save as |
| --- | --- | --- |
| Sales | `http://127.0.0.1:8088/superset/dashboard/sales/` | `sales-dashboard.jpg` |
| Executive | `http://127.0.0.1:8088/superset/dashboard/executive/` | `executive-dashboard.jpg` |
| Customer | `http://127.0.0.1:8088/superset/dashboard/customer/` | `customer-dashboard.jpg` |
| Marketing | `http://127.0.0.1:8088/superset/dashboard/marketing/` | `marketing-dashboard.jpg` |

Login: `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` from `.env`.

The main README embeds these files directly. Preserve the names when
recapturing a dashboard so its portfolio link remains stable.

Notes:

- chart data reflects the last publication run; if the marts were rebuilt
  since, let the dashboards refresh before capturing;
- native filters default to "No filter" — capture the unfiltered view;
- no secrets appear in dashboard views: connections and credentials are
  admin-only metadata objects.
