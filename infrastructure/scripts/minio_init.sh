#!/bin/sh
# Idempotent MinIO bucket initializer (minio-init one-shot container).
# Creates the platform buckets if they do not already exist.
set -eu

endpoint="${MINIO_ENDPOINT_URL:?MINIO_ENDPOINT_URL is required}"
user="${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
password="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"

mc alias set local "$endpoint" "$user" "$password" >/dev/null

for bucket in landing lakehouse archive rejected; do
    mc mb --ignore-existing "local/$bucket"
    echo "minio_init: bucket=$bucket ready"
done

mc ls local
