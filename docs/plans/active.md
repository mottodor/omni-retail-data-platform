# Active task plan — Phase 5 slice 3, task 4 (tail)

Single active-plan file (contract: `docs/agent/engineering-practices.md` §41.3;
status pointer: `PROGRESS.md` → Current focus). Replaced in the same commit as
the work it describes; history lives in git. Delete or overwrite this file
when starting the next task.

## Goal

Close the documentation tail of slice 3: the analytics-marts tables in
`docs/data-model.md`. The README half of the task is done — and was done as a
full restructure (see Decisions).

## Context to read first

- `docs/agent/engineering-practices.md` §41.1 — README rules;
- `docs/data-model.md` — Bronze/Silver/Gold sections; marts are missing;
- `dbt/models/marts/schema.yml` + the four mart SQL files — grain, measures,
  EUR normalization semantics (source of truth for the docs text);
- `README.md` — the new "Lakehouse" and "Orchestration" sections already
  summarize the marts in one line each; `docs/data-model.md` gets the detail.

## Decisions (made, do not relitigate)

- Superseded 2026-09-26 by explicit user request: the README was restructured
  to describe the platform by capability (no phase separation), with the
  target architecture diagram marked "target", grouped Commands, and a new
  Project layout section. The earlier "keep the slice-section pattern /
  restructuring out of scope" decision no longer applies.
- `docs/data-model.md` gets one new "Analytics marts" section covering all
  four marts (`mart_daily_sales`, `mart_customer_ltv`, `mart_marketing_roi`,
  `mart_delivery_performance`): grain, key columns, measures, normalization
  source (`int_orders_fx`), and the orders_count-additivity caveat where
  relevant. Keep wording consistent with `dbt/models/marts/schema.yml`
  (grain first).
- Docs only — no code, DAG, or model changes.
- No screenshots; the Airflow Datasets view stays the manual-verification
  pointer.

## Steps

- [x] README: restructured de-phased (state as of today), lakehouse
      orchestration + marts content included.
- [ ] `docs/data-model.md`: add the marts section per the decisions above.
- [ ] Update `PROGRESS.md` (slice 3 → done; current focus → next task or
      Phase 6 planning) and replace this plan file with the next task's plan
      (or delete it if the next task is not planned yet).

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
- restructuring `docs/data-model.md` beyond adding the marts section
  (its own phase-titled headings can be de-phased in a follow-up).
