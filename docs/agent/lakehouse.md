# Agent guide — Lakehouse (Iceberg, Polaris, Trino)

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

Read this guide before working on: Iceberg tables and maintenance, Polaris
catalog configuration, or Trino queries and federation.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md sections 22–24. Section numbers are preserved so
existing references of the form "AGENTS §N" keep resolving.

---

## 22. Iceberg rules

Iceberg is the persistent analytical source of truth.

Use Iceberg for Bronze, Silver, and Gold datasets where defined by architecture.

Important engineering topics include:

- snapshots;
- schema evolution;
- partition evolution;
- time travel;
- rollback;
- compaction;
- orphan files;
- snapshot expiration;
- small-files problem.

### 22.1 Bronze

Bronze should stay close to source representation.

Add ingestion metadata.

Avoid unnecessary business transformations.

### 22.2 Silver

Silver should contain:

- cleaned;
- typed;
- deduplicated;
- normalized;
- merge-ready/current-state or clearly historical datasets.

### 22.3 Gold

Gold contains business models and marts.

### 22.4 Maintenance

Do not run destructive maintenance with unsafe retention defaults.

Maintenance jobs must:

- be parameterized;
- have documented retention;
- be tested with non-production fixtures;
- log affected tables/snapshots/files.

---

## 23. Polaris rules

Apache Polaris is the Iceberg REST catalog.

Do not bypass Polaris with an unrelated catalog implementation without an ADR.

Configuration must be reproducible from Docker Compose and committed config.

Credentials remain externalized.

---

## 24. Trino rules

Trino is the primary SQL engine for Iceberg and federated analytical access.

Use Trino for:

- Iceberg SQL;
- dbt execution;
- ad-hoc federation;
- comparison against ClickHouse serving performance.

Do not rely on cross-system federation as a substitute for proper ingestion when persistent analytical data is required.

### 24.1 Query quality

For important or slow queries:

- inspect `EXPLAIN`;
- consider partition pruning;
- predicate pushdown;
- join strategy;
- scan volume;
- memory pressure.

Avoid unnecessary:

```sql
SELECT *
```

in stable analytical models.

Select required columns explicitly.
