# Agent guide — Serving and BI (ClickHouse, Superset, Spark)

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

**Phase note:** ClickHouse and Superset belong to the first vertical slice /
serving phases; Spark belongs to Phase F and is justified per workload only —
see the implementation order and phase gates in the `AGENTS.md` core.

Read this guide before working on: ClickHouse table design and publication,
Superset connections and dashboards, Spark jobs.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md sections 25–27. Section numbers are preserved so
existing references of the form "AGENTS §N" keep resolving.

---

## 25. Spark rules

Spark is not a default transformation engine for the project.

Use Spark only where it demonstrates a meaningful distributed-processing case, such as:

- clickstream sessionization;
- large backfills;
- expensive distributed joins;
- large file transformations.

Do not use Spark for a dataset that is trivially handled by Trino/dbt/Python unless the issue is specifically a Spark learning benchmark.

Spark jobs should consider:

- partition count;
- shuffle;
- broadcast join;
- skew;
- event time;
- late events;
- deterministic tests;
- memory limits appropriate for the laptop.

For benchmark tasks, capture `explain` output and before/after metrics.

---

## 26. ClickHouse rules

ClickHouse is a low-latency serving layer.

It is not the master analytical store.

### 26.1 Rebuildability

Every ClickHouse serving dataset must be reconstructable from Iceberg Gold.

### 26.2 Table design

Choose intentionally:

- engine;
- `ORDER BY`;
- partitioning;
- primary key semantics;
- TTL.

Do not add partitioning simply because the feature exists.

### 26.3 Publication

Gold -> ClickHouse publication must be:

- repeatable;
- idempotent;
- recoverable;
- measurable.

Document whether publication is:

- append;
- replace partition;
- upsert;
- full rebuild.

### 26.4 Permissions

Superset uses a read-only account.

Do not grant BI write permissions.

---

## 27. Superset rules

Superset is the main business BI tool.

Primary connection:

```text
Superset -> ClickHouse
```

Secondary exploratory connection:

```text
Superset -> Trino
```

Use Docker-network hostnames.

Do not require public exposure of ClickHouse for normal local use.

Store exportable Superset metadata in the repository where practical.

Expected dashboards eventually include:

- Executive;
- Sales;
- Customer;
- Marketing;
- Funnel.

Dashboard queries should primarily use prepared serving marts rather than expensive raw queries.
