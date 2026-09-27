# Superset BI assets (Phase 7, ADR 0005)

BI-as-code loop: dashboards/datasets are built in the UI, exported as
import/export v1 ZIP bundles, and committed here; `superset-init` re-imports
every bundle on each `make bi-up`, so the repository stays the source of
truth for BI assets.

- `../superset_config.py` — Superset deployment config (mounted into the
  containers; secrets come from `.env`, never from this tree);
- `assets/*.zip` — exported dashboard/dataset bundles (empty in Phase 7
  slice 1; filled from slice 2).

Database connections are not assets: they carry credentials and are created
from `.env` by `infrastructure/scripts/superset_init.py`.
