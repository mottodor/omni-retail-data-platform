# ADR index

Architectural Decision Records for OmniRetail Data Platform.

Policy (when an ADR is required and what it must contain):
`docs/agent/engineering-practices.md` §42. Template: `template.md`.
When adding an ADR, append it to this index in the same change.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-project-architecture.md) | Project architecture and technology stack | Accepted |
| [0002](0002-mock-api-service.md) | Mock external API service for Phase 3 ingestion | Accepted |
| [0003](0003-airflow-deployment.md) | Airflow deployment model (Phase 4 orchestration) | Accepted |
| [0004](0004-clickhouse-serving-publication.md) | ClickHouse serving layer: deployment and Gold publication mechanism | Accepted |
| [0005](0005-superset-deployment.md) | Superset deployment and BI-as-code bootstrap | Accepted |
| [0006](0006-cdc-deployment-and-delivery.md) | CDC deployment and delivery semantics | Accepted |
| [0007](0007-cdc-gold-cutover.md) | CDC-backed Gold cutover and mixed child semantics | Accepted |
| [0008](0008-cdc-analytical-refresh-orchestration.md) | Stable-boundary CDC analytical refresh orchestration | Accepted |
