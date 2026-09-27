# Custom Superset image (Phase 7) — see docs/adr/0005-superset-deployment.md.
#
# Built from the repo root (context: .) so the committed lockfile is the
# source of dependency versions; docker compose references it as
# omni-retail/superset:0.1.0 (ADR 0003 custom-image conventions).
#
# The base image already carries Superset's full pinned dependency tree, so
# only what the BI layer needs and the base image does NOT already ship is
# installed, pinned from the frozen export of uv.lock with --no-deps: no
# fresh resolution, and Superset's own pins are never touched — the venv
# already provides e.g. zstandard==0.23.0, requests, urllib3, certifi,
# python-dateutil, pytz. The set: the two BI drivers (clickhouse-connect for
# ClickHouse, trino for the ad-hoc exploration path), their transitive
# closure, and psycopg2-binary for the PostgreSQL metadata DB (the base
# image defaults to sqlite and ships no postgres driver). The build ends
# with a smoke import (against the /app/.venv interpreter that actually
# runs Superset — `python` on PATH IS that venv, but the install targets it
# explicitly because `uv pip install --system` would resolve to the
# /usr/local interpreter instead) so a missing piece fails the build, not
# runtime. Guarded by tests/test_compose_guards.py.
FROM apache/superset:5.0.0

USER root

# Pinned uv binary (same tag as the Airflow image): extraction from the
# committed lockfile happens without any network resolution.
COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /usr/local/bin/

# BI driver closure exported from uv.lock (trailing line-continuations and
# environment markers are tolerated by pip; the marker on orjson is true on
# the image's CPython).
ARG BI_DRIVER_NAMES="^(clickhouse-connect|trino|backports-zstd|lz4|orjson|tzlocal|psycopg2-binary)=="
COPY pyproject.toml uv.lock README.md ./
RUN uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt \
    && grep -E "${BI_DRIVER_NAMES}" /tmp/requirements.txt \
       | sed 's/[[:space:]]*\\$//' > /tmp/bi-drivers.txt \
    && uv pip install --python /app/.venv/bin/python --no-deps -r /tmp/bi-drivers.txt \
    && /app/.venv/bin/python -c "import clickhouse_connect, trino" \
    && rm -f /tmp/requirements.txt /tmp/bi-drivers.txt

USER superset
