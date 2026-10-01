"""Iceberg DDL and idempotent MERGE SQL for raw CDC events."""

# pyright: reportMissingImports=false

import re
from collections.abc import Sequence

from omni_retail.lakehouse.bronze.literals import sql_literal
from omni_retail.lakehouse.trino import TrinoExecutor, validate_schema_name

from .model import CdcEvent

CDC_TABLE = "postgres_cdc_events"
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,127}$")

CDC_COLUMNS: tuple[tuple[str, str], ...] = (
    ("event_id", "varchar"),
    ("kafka_topic", "varchar"),
    ("kafka_partition", "integer"),
    ("kafka_offset", "bigint"),
    ("kafka_timestamp", "timestamp(6) with time zone"),
    ("source_schema", "varchar"),
    ("source_table", "varchar"),
    ("operation", "varchar"),
    ("source_lsn", "bigint"),
    ("source_tx_id", "bigint"),
    ("source_timestamp", "timestamp(6) with time zone"),
    ("key_json", "varchar"),
    ("envelope_json", "varchar"),
    ("before_json", "varchar"),
    ("after_json", "varchar"),
    ("event_date", "date"),
    ("ingested_at", "timestamp(6) with time zone"),
)


def _safe_identifier(value: str, *, label: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"invalid {label} identifier {value!r}")
    return value


def create_cdc_table_sql(*, catalog: str = "iceberg", schema: str = "bronze") -> str:
    """Return idempotent DDL for the append-only raw CDC ledger."""
    catalog = _safe_identifier(catalog, label="catalog")
    schema = validate_schema_name(schema)
    columns = ", ".join(f'"{name}" {trino_type}' for name, trino_type in CDC_COLUMNS)
    return (
        f"create table if not exists {catalog}.{schema}.{CDC_TABLE} ({columns}) "
        "with (format = 'PARQUET', format_version = 2, "
        "partitioning = ARRAY['event_date'])"
    )


def _event_values(event: CdcEvent) -> tuple[object, ...]:
    return (
        event.event_id,
        event.kafka_topic,
        event.kafka_partition,
        event.kafka_offset,
        event.kafka_timestamp,
        event.source_schema,
        event.source_table,
        event.operation,
        event.source_lsn,
        event.source_tx_id,
        event.source_timestamp,
        event.key_json,
        event.envelope_json,
        event.before_json,
        event.after_json,
        event.event_date,
        event.ingested_at,
    )


def _typed_literal(value: object, trino_type: str) -> str:
    return f"CAST({sql_literal(value, trino_type)} AS {trino_type})"


def _merge_prefix(events: Sequence[CdcEvent], *, catalog: str, schema: str) -> tuple[str, str]:
    if not events:
        raise ValueError("cannot build a CDC MERGE for an empty event batch")
    catalog = _safe_identifier(catalog, label="catalog")
    schema = validate_schema_name(schema)
    if len({event.event_id for event in events}) != len(events):
        raise ValueError("CDC MERGE batch contains duplicate event IDs")
    names = ", ".join(f'"{name}"' for name, _ in CDC_COLUMNS)
    return f"merge into {catalog}.{schema}.{CDC_TABLE} AS t", names


def merge_cdc_events_sql(
    events: Sequence[CdcEvent], *, catalog: str = "iceberg", schema: str = "bronze"
) -> str:
    """Build a rendered MERGE for deterministic SQL contract tests only."""
    prefix, names = _merge_prefix(events, catalog=catalog, schema=schema)
    rows = [
        "("
        + ", ".join(
            _typed_literal(value, trino_type)
            for value, (_, trino_type) in zip(_event_values(event), CDC_COLUMNS, strict=True)
        )
        + ")"
        for event in events
    ]
    source_names = ", ".join(f's."{name}"' for name, _ in CDC_COLUMNS)
    return (
        f"{prefix} using (values {', '.join(rows)}) AS s ({names}) "
        'on t."event_id" = s."event_id" and t."event_date" = s."event_date" '
        f"when not matched then insert ({names}) values ({source_names})"
    )


def merge_cdc_events_statement(
    events: Sequence[CdcEvent], *, catalog: str = "iceberg", schema: str = "bronze"
) -> tuple[str, list[object]]:
    """Build a DB-API parameterized MERGE and its ordered payload values."""
    prefix, names = _merge_prefix(events, catalog=catalog, schema=schema)
    row_placeholder = (
        "(" + ", ".join(f"CAST(? AS {trino_type})" for _, trino_type in CDC_COLUMNS) + ")"
    )
    placeholders = ", ".join(row_placeholder for _ in events)
    params = [value for event in events for value in _event_values(event)]
    source_names = ", ".join(f's."{name}"' for name, _ in CDC_COLUMNS)
    statement = (
        f"{prefix} using (values {placeholders}) AS s ({names}) "
        'on t."event_id" = s."event_id" and t."event_date" = s."event_date" '
        f"when not matched then insert ({names}) values ({source_names})"
    )
    return statement, params


def ensure_cdc_table(
    executor: TrinoExecutor, *, catalog: str = "iceberg", schema: str = "bronze"
) -> None:
    """Create the configured Bronze schema and CDC event table if absent."""
    catalog = _safe_identifier(catalog, label="catalog")
    schema = validate_schema_name(schema)
    executor.execute(  # nosec B608 -- both identifiers are strict-regex allowlisted above
        f"create schema if not exists {catalog}.{schema}"
    )
    executor.execute(create_cdc_table_sql(catalog=catalog, schema=schema))
