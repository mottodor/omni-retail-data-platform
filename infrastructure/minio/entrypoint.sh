#!/bin/sh
set -eu

# Preserve the official image's CLI contract: Compose can pass `server ...`
# while direct callers can still invoke `minio ...` explicitly.
if [ "${1:-}" != "minio" ] && [ -n "${1:-}" ]; then
    set -- minio "$@"
fi

exec "$@"
