# ADR 0003 — Airflow deployment model (Phase 4 orchestration)

## Title

Introduce Apache Airflow 2.11.2 in the `orchestration` Compose profile: LocalExecutor, custom image with the `omni_retail` package baked in, dedicated metadata PostgreSQL, and environment-only configuration.

## Status

Accepted.

## Context

Phase 4 (`ROADMAP.md`) moves orchestration into Airflow without mixing it with transformation logic (AGENTS.md §19): DAGs stay thin wrappers over the ingestion functions built in Phase 3. The deployment must fit a single WSL2 workstation (~24 GB RAM, ~6 CPUs), stay reproducible, and keep `make reset` from destroying orchestration metadata together with the OLTP source data.

Decisions requiring an ADR (AGENTS.md §42): a new platform service deployment model, a custom image build, and a second PostgreSQL instance.

## Decision

- **Executor: LocalExecutor.** One workstation node; CeleryExecutor + Redis + triggerer would be overengineering (AGENTS.md §53). No deferrable operators in this phase.
- **Metadata database: dedicated `airflow-postgres`** (`postgres:16.15-alpine`, volume `airflow-metadata-data`, no host port). Separate from the core OLTP PostgreSQL so `make reset` (which destroys `postgres-data`) cannot wipe Airflow metadata.
- **Custom image `omni-retail/airflow:0.1.0`** built from pinned `apache/airflow:2.11.2-python3.12` (`infrastructure/airflow/Dockerfile`, build context = repo root):
  - pinned `uv` binary copied from the tagged `ghcr.io/astral-sh/uv` image;
  - dependency versions are exported from the committed `uv.lock` (`uv export --frozen --no-dev`) and installed with `uv pip install --system` — no fresh resolution inside the build;
  - the `omni_retail` package itself is installed with `uv pip install --system --no-deps .`;
  - pinned `pytest` is included so DAG tests run inside the image;
  - `USER airflow` (non-root), matching the base image.
- **Ingestion code runs in the worker process**: TaskFlow tasks call `omni_retail` ingestion functions directly (env-configured), instead of shelling out or duplicating logic in DAGs.
- **Configuration: environment only.** Compose env is the single source (same variables as local CLI runs, with docker-network addresses: `http://minio:9000`, `http://mock-api:9002`, `postgres:5432`). Airflow Connections/Variables are not used; secrets stay in `.env` (`AIRFLOW_FERNET_KEY`, admin user, metadata DB password).
- **Services**: `airflow-init` (one-shot: `db migrate` + admin user + pool `mock_api` slots=2), `airflow-webserver` (loopback-only host port, default 8081), `airflow-scheduler`. DAG/include/tests directories are mounted read-only; `PYTHONPATH=/opt/airflow` makes the `include` helpers package importable.
- **New DAGs start paused** (`AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True`); the profile requires `core` to be up (documented, no hard cross-profile `depends_on`).

## Alternatives considered

- **CeleryExecutor + Redis:** rejected — multi-node semantics are meaningless on one workstation node and add three more services (broker, result backend, flower) to babysit.
- **SQLite metadata DB (default of the base image):** rejected — not production-like and awkward with LocalExecutor concurrency.
- **Reuse core `postgres` for Airflow metadata:** rejected — couples orchestration metadata to the OLTP source lifecycle; `make reset` would destroy both.
- **`pip install` inside the build (fresh resolution):** rejected — violates the reproducibility policy (AGENTS.md §11); versions must come from the committed lockfile.
- **Airflow `DockerOperator`/`KubernetesPodOperator` per task:** rejected — heavier runtime and image sprawl; the package is already baked into the workers, so in-process calls are simpler and faster.
- **CeleryExecutor later:** open — the switch is a deployment change (new services + executor flag), not a DAG rewrite.

## Consequences

- The `orchestration` profile adds four services (only webserver + scheduler are long-running) and one custom image build; `make airflow-up` starts them after `make up`.
- DAG tests (`DagBag`) run inside the image via `make airflow-test` / `infrastructure/scripts/airflow_tests.sh` — the dev venv intentionally does not install Airflow.
- Image rebuilds are needed when ingestion code or lockfile versions change; the tag `0.1.0` is explicit (no `latest`).
- Fernet key rotation invalidates encrypted connection secrets — acceptable because no connections are stored in the metadata DB in this phase.
- A second PostgreSQL container slightly increases RAM usage (~50–100 MB); accepted for isolation.

## Rollback / migration considerations

- Removing orchestration is a Compose profile removal plus deletion of `infrastructure/airflow/` and `airflow/`; no lakehouse data depends on it. Volume `airflow-metadata-data` holds only Airflow state.
- Upgrading Airflow means bumping the base image tag, re-checking the constraint overlap, and `airflow db migrate` on start (already part of `airflow-init`).
- Moving to CeleryExecutor later adds Redis/worker services and changes `AIRFLOW__CORE__EXECUTOR`; DAG code is unaffected.
