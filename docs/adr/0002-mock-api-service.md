# ADR 0002 — Mock external API service for Phase 3 ingestion

## Title

Introduce a self-hosted mock REST API service (FastAPI) as the deterministic external source for FX rates, marketing campaigns, and delivery status.

## Status

Accepted.

## Context

Phase 3 requires REST API ingestion (FX rates, marketing campaigns, delivery status — `ROADMAP.md` Phase 3). Real public APIs are unsuitable here because:

- unit/integration tests must be deterministic (AGENTS.md §38) and must not depend on external public APIs (AGENTS.md §48);
- failure engineering needs reproducible fault injection (HTTP 429, 5xx, timeouts) at controlled rates;
- backfills must return the same data for the same logical dates.

At the same time, the ingestion clients must exercise realistic HTTP behavior: pagination (offset and cursor), `Retry-After` headers, rate limiting, and malformed-response handling.

## Decision

Add a dedicated `mock-api` service — a small FastAPI application shipped as its own Docker image and run in the `core` Compose profile:

- **Service:** FastAPI + uvicorn, built from `infrastructure/mock_api/` (`python:3.12-slim` base, pinned dependency ranges in `requirements.txt`). FastAPI/uvicorn live *only* inside this image and are not dependencies of the main Python package.
- **Endpoints** (`/api/v1/...`): `fx-rates` (offset pagination, required `date`), `marketing/campaigns` (offset pagination, optional `date`, `status` filter), `deliveries` (cursor pagination, `updated_since`). Plus `/healthz` for the Compose healthcheck.
- **Determinism:** every response is derived from `(MOCK_API_SEED, date, request)` via `random.Random` seeded with a string — no database, no wall-clock dependence. The same seed and logical date always produce the same payload, which makes backfills reproducible.
- **Fault injection:** query parameters `fault=429|500|timeout` and `fault_rate=<0..1>`. The decision is a deterministic hash of `(seed, path, query-without-fault-params)`; a given fingerprint faults **at most once per server lifetime** so that client retry scenarios (429 → retry → success) are testable while a fresh server still fails the same request deterministically.
- **Exposure:** host port bound to `127.0.0.1` only (default `9002`, overridable via `MOCK_API_PORT`); image tagged `omni-retail/mock-api:0.1.0`.
- **Code layout:** `infrastructure/mock_api/src/mock_api/` with pure-stdlib `data.py` and `faults.py` (unit-testable without FastAPI installed) and a thin FastAPI `main.py`. The package is importable in tests via the pytest `pythonpath` setting; it is intentionally outside the mypy-strict scope of the main package (ruff still covers it).

## Alternatives considered

- **Real public APIs** (e.g. a live FX rates API): rejected — non-deterministic, rate-limited, and unsuitable for CI or fault injection.
- **Recording/replaying real responses (VCR-style):** rejected — fixtures rot quickly and cannot simulate cursor pagination edge cases or controlled 429/5xx storms.
- **Mocking at the client boundary only (httpx `MockTransport` everywhere):** kept for unit tests of the clients, but rejected as the sole mechanism — integration and failure scenarios need a real HTTP server with real sockets, headers, and timeouts.
- **Flask instead of FastAPI:** rejected — FastAPI gives declarative query validation (422 on bad input, which the client must classify as non-retryable) with minimal code.

## Consequences

- One more container in the `core` profile (small footprint: slim image, no database); `make up` starts it automatically alongside MinIO/Trino.
- The mock API is a *source simulator*, not platform business logic; nothing in the lakehouse depends on its internals, and replacing it with real vendor APIs later only changes `MOCK_API_BASE_URL`-style configuration.
- `httpx` is added to the main package dependencies for the ingestion clients; FastAPI/uvicorn are confined to the service image.
- Guard tests must cover the new service (pinned images, healthcheck, loopback-only port, `.env.example` coverage) — extended in `tests/test_compose_guards.py`.

## Rollback / migration considerations

- Removing the service is a Compose service removal plus deletion of `infrastructure/mock_api/`; no data or platform state depends on it.
- Swapping in real external APIs later: implement new source modules in `omni_retail.ingestion.api` pointing at real base URLs; the raw-page persistence and manifest contracts stay unchanged.
