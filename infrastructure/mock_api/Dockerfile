# Mock external API service (Phase 3) — see docs/adr/0002-mock-api-service.md.
# Deterministic FX/marketing/delivery source with fault injection; HTTP shell
# is FastAPI, data and fault logic are pure stdlib and unit-tested from the
# main test suite.
FROM python:3.12-slim

RUN useradd --create-home --shell /usr/sbin/nologin appuser

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/mock_api ./mock_api

USER appuser

EXPOSE 9002

CMD ["uvicorn", "mock_api.main:app", "--host", "0.0.0.0", "--port", "9002"]
