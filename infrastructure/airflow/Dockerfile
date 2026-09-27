# Custom Airflow image (Phase 4) — see docs/adr/0003-airflow-deployment.md.
#
# Built from the repo root (context: .) so the committed lockfile is the
# source of dependency versions; docker compose references it as
# omni-retail/airflow:0.1.0.

FROM apache/airflow:2.11.2-python3.12

USER root

# The base image routes the interpreter's site-packages to the airflow user's
# home (/usr/local/.../site-packages is NOT on sys.path), so every install in
# this image must explicitly target that directory.
ARG AIRFLOW_SITE_PACKAGES=/home/airflow/.local/lib/python3.12/site-packages

# Pinned uv binary: dependency versions are exported from the committed
# uv.lock (uv export --frozen) and installed without fresh resolution.
COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /usr/local/bin/

COPY pyproject.toml uv.lock README.md ./
RUN uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt \
    && uv pip install --target "${AIRFLOW_SITE_PACKAGES}" -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

# Pinned pytest for DAG tests executed inside this image (`make airflow-test`).
RUN uv pip install --target "${AIRFLOW_SITE_PACKAGES}" "pytest==8.4.2"

# dbt requires protobuf>=6, but the base image ships grpcio-status 1.71.2
# (protobuf<6). Overlay the newest grpcio-status compatible with both the
# image (grpcio 1.78.0) and our protobuf 6.x so `pip check` stays clean.
RUN uv pip install --target "${AIRFLOW_SITE_PACKAGES}" "grpcio-status==1.78.0"

# The platform package itself (pure Python): the wheel payload of a src-layout
# package is exactly its source tree, so it is copied straight into
# site-packages — no build backend, no resolution (AGENTS.md §11).
COPY --chown=airflow:0 src/omni_retail ${AIRFLOW_SITE_PACKAGES}/omni_retail

# Runtime imports (as the airflow user) can then write bytecode caches.
RUN chown -R airflow:0 "${AIRFLOW_SITE_PACKAGES}"

# Guard: the overlay must stay consistent with the base image's frozen set.
# Runs as the airflow user so pip also sees the overlay in its user site.
USER airflow
RUN python -m pip check

# DAG files import helpers as `include.*` (mounted at /opt/airflow/include).
ENV PYTHONPATH=/opt/airflow

USER airflow
