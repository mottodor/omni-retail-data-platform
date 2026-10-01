#!/bin/sh
set -eu

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${DEBEZIUM_POSTGRES_USER:?DEBEZIUM_POSTGRES_USER is required}"
: "${DEBEZIUM_POSTGRES_PASSWORD:?DEBEZIUM_POSTGRES_PASSWORD is required}"

case "$DEBEZIUM_POSTGRES_USER" in
  *[!a-z0-9_]*|'')
    echo "DEBEZIUM_POSTGRES_USER must be lower-case letters, digits, or underscores" >&2
    exit 2
    ;;
esac

# psql variables keep the password out of SQL interpolation and logs. No shell
# tracing is enabled; the complete command/config is never printed.
psql --host=postgres --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" \
  --set=ON_ERROR_STOP=1 \
  --set=cdc_user="$DEBEZIUM_POSTGRES_USER" \
  --set=cdc_password="$DEBEZIUM_POSTGRES_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE %I LOGIN REPLICATION PASSWORD %L', :'cdc_user', :'cdc_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'cdc_user') \gexec
SELECT format('ALTER ROLE %I WITH LOGIN REPLICATION PASSWORD %L', :'cdc_user', :'cdc_password') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), :'cdc_user') \gexec
SELECT format('GRANT USAGE ON SCHEMA public TO %I', :'cdc_user') \gexec
SELECT format(
  'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM %I',
  :'cdc_user'
) \gexec
SELECT format(
  'GRANT SELECT ON TABLE public.customers, public.orders, public.payments TO %I',
  :'cdc_user'
) \gexec

DO $bootstrap$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'omni_cdc_publication') THEN
    CREATE PUBLICATION omni_cdc_publication
      FOR TABLE public.customers, public.orders, public.payments;
  END IF;
END
$bootstrap$;
ALTER PUBLICATION omni_cdc_publication
  SET TABLE public.customers, public.orders, public.payments;
SQL

echo "PostgreSQL CDC role and three-table publication are reconciled"
