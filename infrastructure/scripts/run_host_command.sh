#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
explicit_compose_project=${COMPOSE_PROJECT_NAME-}

set -a
# shellcheck source=/dev/null
source "${repo_root}/.env"
set +a

# Command-line/CI namespaces must win over the local .env default so live and
# destructive checks can run against an isolated Compose project.
if [[ -n "${explicit_compose_project}" ]]; then
  export COMPOSE_PROJECT_NAME="${explicit_compose_project}"
fi

# Local service probes must not inherit an ambient HTTP proxy. Preserve any
# additional exclusions supplied by the workstation or CI environment.
export no_proxy="127.0.0.1,localhost${no_proxy:+,${no_proxy}}"
export NO_PROXY="127.0.0.1,localhost${NO_PROXY:+,${NO_PROXY}}"

cd "${repo_root}"
exec "$@"
