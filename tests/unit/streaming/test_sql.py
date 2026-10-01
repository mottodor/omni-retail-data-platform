"""Iceberg CDC table and insert-only MERGE SQL tests."""

# pyright: reportMissingImports=false

from datetime import UTC, date, datetime

import pytest

from omni_retail.streaming.cdc.model import CdcEvent
from omni_retail.streaming.cdc.sql import (
    CDC_COLUMNS,
    create_cdc_table_sql,
    merge_cdc_events_sql,
    merge_cdc_events_statement,
)


def event(event_id: str = "event-1", offset: int = 3) -> CdcEvent:
    moment = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
    return CdcEvent(
        event_id=event_id,
        kafka_topic="omni.oltp.public.customers",
        kafka_partition=0,
        kafka_offset=offset,
        kafka_timestamp=moment,
        source_schema="public",
        source_table="customers",
        operation="c",
        source_lsn=123,
        source_tx_id=12,
        source_timestamp=moment,
        key_json='{"customer_id":1}',
        envelope_json='{"op":"c"}',
        before_json=None,
        after_json='{"customer_id":1}',
        event_date=date(2026, 9, 30),
        ingested_at=moment,
    )


def test_cdc_table_is_partitioned_by_event_date() -> None:
    ddl = create_cdc_table_sql(schema="bronze")
    assert "iceberg.bronze.postgres_cdc_events" in ddl
    assert '"event_id" varchar' in ddl
    assert "format_version = 2" in ddl
    assert "partitioning = ARRAY['event_date']" in ddl


def test_parameterized_merge_uses_event_id_and_all_values() -> None:
    item = event()
    statement, params = merge_cdc_events_statement([item])

    assert 'on t."event_id" = s."event_id"' in statement
    assert 't."event_date" = s."event_date"' in statement
    assert "when not matched then insert" in statement
    assert statement.count("?") == len(CDC_COLUMNS)
    assert len(params) == len(CDC_COLUMNS)
    assert params[0] == item.event_id
    assert item.envelope_json in params


def test_rendered_merge_escapes_raw_json_text() -> None:
    item = event()
    rendered = merge_cdc_events_sql([item])
    assert 'CAST(\'{"op":"c"}\' AS varchar)' in rendered
    assert "when matched" not in rendered.lower()


def test_merge_rejects_duplicate_event_ids() -> None:
    with pytest.raises(ValueError, match="duplicate event IDs"):
        merge_cdc_events_statement([event(), event(offset=4)])
