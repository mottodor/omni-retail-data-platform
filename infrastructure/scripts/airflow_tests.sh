#!/usr/bin/env bash
# DAG test harness shared by `make airflow-test` and CI (Phase 4 design spec §12).
#
# Builds the custom Airflow image unless AIRFLOW_SKIP_BUILD=1, then runs the
# DAG tests plus `airflow dags list-import-errors` inside the image. No live
# Airflow services are required: the container uses a throwaway sqlite
# metadata database just for CLI startup.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="omni-retail/airflow:0.1.0"

if [[ "${AIRFLOW_SKIP_BUILD:-0}" != "1" ]]; then
  docker build -f "${REPO_ROOT}/infrastructure/airflow/Dockerfile" -t "${IMAGE}" "${REPO_ROOT}"
fi

docker run --rm \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:////tmp/airflow-dag-tests.db" \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=False \
  -e PYTHONPATH=/opt/airflow \
  -v "${REPO_ROOT}/airflow/dags:/opt/airflow/dags:ro" \
  -v "${REPO_ROOT}/airflow/include:/opt/airflow/include:ro" \
  -v "${REPO_ROOT}/airflow/tests:/opt/airflow/tests:ro" \
  --entrypoint /bin/bash \
  "${IMAGE}" -c '
    set -euo pipefail
    airflow db migrate >/dev/null
    python -m pytest /opt/airflow/tests -v
    import_errors="$(airflow dags list-import-errors -o json)"
    if [[ "${import_errors}" != "[]" ]]; then
      echo "DAG import errors:" >&2
      echo "${import_errors}" >&2
      exit 1
    fi
    echo "airflow dags list-import-errors: none"
  '
