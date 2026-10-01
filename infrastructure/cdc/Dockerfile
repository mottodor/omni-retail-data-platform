# Restart-safe PostgreSQL CDC consumer (Phase 8, ADR 0006).
FROM python:3.12.12-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /usr/local/bin/

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --only-group cdc --no-install-project

COPY src ./src
RUN useradd --create-home --uid 10001 cdc \
    && chown -R cdc:cdc /app

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1

USER cdc

CMD ["python", "-m", "omni_retail.streaming.cdc", "run"]
