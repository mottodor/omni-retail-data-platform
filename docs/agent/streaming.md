# Agent guide — Streaming and CDC (Kafka, Debezium)

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

Kafka and Debezium are delivered Phase 8 components. Maintenance must preserve
the CDC semantics and architecture gates in the `AGENTS.md` core.

Read this guide before working on: Kafka topics and consumers, Debezium
connectors, CDC-derived datasets.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md section 18. Section numbers are preserved so
existing references of the form "AGENTS §N" keep resolving.

---

## 18. Kafka and Debezium rules

Kafka is used for the delivered PostgreSQL CDC transport. Application-event
and clickstream domains are outside this repository's final scope.

Debezium is used for PostgreSQL CDC.

CDC implementation must explicitly handle:

- create;
- update;
- delete;
- duplicate delivery;
- restart;
- offset recovery;
- ordering limitations;
- schema changes;
- replay.

For CDC-derived state, define how event identity is determined.

Do not assume exactly-once semantics without proving the full end-to-end guarantee.

Prefer designing for at-least-once delivery plus idempotent consumers.

Document topic naming and retention strategy.
