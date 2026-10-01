#!/bin/bash
set -euo pipefail

project_name="${COMPOSE_PROJECT_NAME:-omni-retail}"
mapfile -t volume_ids < <(
  docker volume ls -q \
    --filter "label=com.docker.compose.project=${project_name}" \
    --filter "label=com.docker.compose.volume=kafka-data"
)
if ((${#volume_ids[@]} > 0)); then
  docker volume rm "${volume_ids[@]}"
fi
