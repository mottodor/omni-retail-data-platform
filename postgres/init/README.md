# OLTP schema DDL lives here.

`01_oltp_schema.sql` is mounted into the postgres container as
`/docker-entrypoint-initdb.d/01_oltp_schema.sql` and runs automatically on a
fresh volume. The script is idempotent, so the generator
(`python -m omni_retail.generators.oltp schema|initial`) can safely re-apply
it against an existing database.
