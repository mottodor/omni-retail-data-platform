#!/bin/sh
# Idempotent MinIO initializer (minio-init one-shot container).
# Creates the platform buckets and the least-privilege ingestion service
# account (rw on landing/archive/rejected, read-only on lakehouse).
set -eu

endpoint="${MINIO_ENDPOINT_URL:?MINIO_ENDPOINT_URL is required}"
user="${MINIO_ROOT_USER:?MINIO_ROOT_USER is required}"
password="${MINIO_ROOT_PASSWORD:?MINIO_ROOT_PASSWORD is required}"
ingestion_key="${S3_ACCESS_KEY_ID:?S3_ACCESS_KEY_ID is required}"
ingestion_secret="${S3_SECRET_ACCESS_KEY:?S3_SECRET_ACCESS_KEY is required}"

mc alias set local "$endpoint" "$user" "$password" >/dev/null

for bucket in landing lakehouse archive rejected; do
    mc mb --ignore-existing "local/$bucket"
    echo "minio_init: bucket=$bucket ready"
done

# Least-privilege policy for the ingestion pipeline (AGENTS.md §10, §40).
# Note: if this policy ever changes, delete it first with
#   mc admin policy delete local omni-ingestion-rw
# otherwise the existing definition is kept.
cat >/tmp/omni-ingestion-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::landing",
        "arn:aws:s3:::landing/*",
        "arn:aws:s3:::archive",
        "arn:aws:s3:::archive/*",
        "arn:aws:s3:::rejected",
        "arn:aws:s3:::rejected/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::lakehouse",
        "arn:aws:s3:::lakehouse/*"
      ]
    }
  ]
}
EOF

if mc admin policy create local omni-ingestion-rw /tmp/omni-ingestion-policy.json >/dev/null 2>&1; then
    echo "minio_init: policy=omni-ingestion-rw created"
else
    echo "minio_init: policy=omni-ingestion-rw already exists (kept as-is)"
fi

mc admin user add local "$ingestion_key" "$ingestion_secret"
mc admin policy attach local omni-ingestion-rw --user "$ingestion_key"
echo "minio_init: user=$ingestion_key attached to policy=omni-ingestion-rw"

mc ls local
