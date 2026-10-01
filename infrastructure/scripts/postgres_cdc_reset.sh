#!/bin/sh
set -eu

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"

psql --host=postgres --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 <<'SQL'
SELECT pg_drop_replication_slot(slot_name)
FROM pg_replication_slots
WHERE slot_name = 'omni_cdc_slot';
DROP PUBLICATION IF EXISTS omni_cdc_publication;
SQL

echo "PostgreSQL CDC slot and publication were removed"
