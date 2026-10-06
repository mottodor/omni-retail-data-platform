# Agent guide — Serving and BI (ClickHouse, Superset)

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

ClickHouse and Superset are delivered components of the Phase 8 capstone.
Distributed processing and clickstream/sessionization are outside this
repository's maintenance scope under ADR 0009.

Read this guide before working on: ClickHouse table design and publication or
Superset connections and dashboards.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md sections 25–27. Section numbers are preserved so
existing references of the form "AGENTS §N" keep resolving.

---

## 25. Distributed-processing scope

Apache Spark is not implemented and is not part of this repository's final
scope. Use the delivered Trino/dbt/Python paths for maintenance work.

Do not add Spark, clickstream sessionization, or another distributed-processing
subsystem through an ordinary issue. Such an expansion requires a new ADR that
explicitly supersedes ADR 0009 and explains why an independent repository is
not the better boundary.

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

The delivered dashboards are:

- Executive;
- Sales;
- Customer;
- Marketing.

A Funnel dashboard is outside scope because the capstone has no clickstream or
attribution source. Dashboard queries should primarily use prepared serving
marts rather than expensive raw queries.
