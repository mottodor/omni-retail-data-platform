# Active task plan — Phase 5 slice 3, task 4

Single active-plan file (contract: `docs/agent/engineering-practices.md` §41.3;
status pointer: `PROGRESS.md` → Current focus). Replaced in the same commit as
the work it describes; history lives in git. Delete or overwrite this file
when starting the next task.

## Goal

Documentation closing slice 3: a README section for the dataset-triggered
lakehouse orchestration, and the analytics-marts tables in
`docs/data-model.md`.

## Context to read first

- `docs/agent/engineering-practices.md` §41.1 — README rules (structure,
  what belongs where);
- `README.md` — existing "Bronze layer (Phase 5 slice 1)" and "Silver and
  Gold layers (Phase 5 slice 2)" sections set the pattern;
- `docs/data-model.md` — Bronze/Silver/Gold sections; marts are missing;
- `dbt/models/marts/schema.yml` + the four mart SQL files — grain, measures,
  EUR normalization semantics (source of truth for the docs text);
- `airflow/dags/load_bronze.py`, `airflow/dags/transform_lakehouse.py`,
  `airflow/include/datasets.py` — what the README section describes;
- `tests/integration/test_lakehouse_orchestration.py` — verified behavior to
  reference (watermark sweep, idempotent re-trigger, full dbt build).

## Decisions (made, do not relitigate)

- Docs only — no code, DAG, or model changes in this task.
- README keeps the slice-section pattern (short prose + `make` commands);
  detailed model semantics go to `docs/data-model.md`, not the README.
- `docs/data-model.md` gets one new "Analytics marts (Phase 5 slice 3)"
  section covering all four marts (`mart_daily_sales`, `mart_customer_ltv`,
  `mart_marketing_roi`, `mart_delivery_performance`): grain, key columns,
  measures, normalization source (`int_orders_fx`), and the
  orders_count-additivity caveat where relevant.
- No screenshots; mention the Airflow Datasets view as manual verification.

## Steps

- [ ] README: add "Lakehouse orchestration (Phase 5 slice 3)" section after
      the Silver/Gold section — dataset chain `raw://` (four ingestion DAGs)
      -> `load_bronze` (watermark-driven `run-new`) -> `lakehouse://bronze`
      -> `transform_lakehouse` (full `dbt build`), plus the manual commands
      (`make airflow-up`, unpause, Datasets view; `make airflow-dag-test`).
- [ ] `docs/data-model.md`: add the marts section per the decisions above;
      keep table descriptions consistent with `dbt/models/marts/schema.yml`
      wording (grain first).
- [ ] Update `PROGRESS.md` (slice 3 -> done; current focus -> next task or
      slice 4/Phase 6 planning) and replace this plan file with the next
      task's plan (or delete it if the next task is not planned yet).

## Validation

```
make lint
make test
```

Docs-only change: no integration run required; manual review of rendered
markdown (table alignment, link targets) is part of the DoD.

## Out of scope

- ClickHouse publication / Phase 6 planning content;
- the deferred follow-ups listed in `PROGRESS.md` (branch reconciliation,
  MinIO image reproducibility, file-source Bronze ingestion);
- restructuring existing README/data-model sections.
