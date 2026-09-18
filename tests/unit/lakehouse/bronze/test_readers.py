"""Unit tests for the raw-object readers (fixtures only, no network)."""

import json
from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from fakes.storage import FakeStorage
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE
from omni_retail.ingestion.postgres_snapshot.tables import table_by_name
from omni_retail.lakehouse.bronze.readers import (
    BronzeReadError,
    list_data_objects,
    read_json_rows,
    read_parquet_rows,
    read_rows,
)
from omni_retail.lakehouse.bronze.specs import TABLES

LOGICAL_DATE = date(2026, 9, 18)
SNAPSHOT_ORDERS = table_by_name("orders")


def orders_parquet_body() -> bytes:
    rows = [
        (
            1,
            9001,
            "paid",
            "USD",
            Decimal("1.50"),
            Decimal("10.00"),
            datetime(2026, 9, 17, 10, 0, tzinfo=UTC),
            datetime(2026, 9, 17, 10, 5, tzinfo=UTC),
        ),
        (
            2,
            9002,
            "shipped",
            "EUR",
            Decimal("2.00"),
            Decimal("20.00"),
            datetime(2026, 9, 17, 11, 0, tzinfo=UTC),
            datetime(2026, 9, 17, 11, 5, tzinfo=UTC),
        ),
    ]
    arrays = [
        pa.array([row[index] for row in rows], type=column.type)
        for index, column in enumerate(SNAPSHOT_ORDERS.columns)
    ]
    table = pa.Table.from_arrays(arrays, schema=SNAPSHOT_ORDERS.arrow_schema())
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()  # type: ignore[no-any-return]


ORDERS_KEY = "postgres/orders/2026/09/18/data.parquet"


def test_parquet_reader_returns_source_rows() -> None:
    rows = read_parquet_rows(TABLES["orders"], ORDERS_KEY, orders_parquet_body())
    assert len(rows) == 2
    assert rows[0]["order_id"] == 1
    assert rows[0]["order_total"] == Decimal("10.00")
    assert rows[1]["currency"] == "EUR"


def test_parquet_reader_rejects_schema_drift() -> None:
    schema = pa.schema([pa.field("order_id", pa.int64()), pa.field("surprise", pa.string())])
    table = pa.Table.from_arrays(
        [pa.array([1], pa.int64()), pa.array(["x"], pa.string())], schema=schema
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    body = sink.getvalue().to_pybytes()
    with pytest.raises(BronzeReadError, match="schema drift"):
        read_parquet_rows(TABLES["orders"], ORDERS_KEY, body)


def test_read_rows_dispatches_by_spec_kind() -> None:
    rows = read_rows(TABLES["orders"], ORDERS_KEY, orders_parquet_body())
    assert len(rows) == 2


def test_json_reader_flattens_fx_page() -> None:
    page = {
        "base": "EUR",
        "date": "2026-09-18",
        "page": 1,
        "page_size": 100,
        "total_count": 1,
        "rates": [{"currency": "USD", "rate": 1.091234}],
    }
    rows = read_json_rows(
        TABLES["fx_rates"], "api/fx-rates/20260918/page_0001.json", json.dumps(page).encode()
    )
    assert rows == [{"currency": "USD", "rate": Decimal("1.091234")}]


def test_json_reader_parses_campaign_dates_and_decimals() -> None:
    page = {
        "campaigns": [
            {
                "campaign_id": "CMP-0001",
                "name": "Summer Sale",
                "channel": "search",
                "status": "active",
                "start_date": "2026-01-15",
                "end_date": "2026-03-15",
                "budget_eur": 25000.5,
                "spend_eur": 12000.25,
                "impressions": 150000,
                "clicks": 3000,
            }
        ]
    }
    rows = read_json_rows(
        TABLES["campaigns"],
        "api/marketing-campaigns/20260918/page_0001.json",
        json.dumps(page).encode(),
    )
    assert rows[0]["start_date"] == date(2026, 1, 15)
    assert rows[0]["end_date"] == date(2026, 3, 15)
    assert rows[0]["budget_eur"] == Decimal("25000.5")
    assert rows[0]["impressions"] == 150000


def test_json_reader_parses_delivery_datetimes_and_nulls() -> None:
    page = {
        "deliveries": [
            {
                "delivery_id": "DLV-000001",
                "order_id": "ORD-123456",
                "carrier": "DHL",
                "status": "in_transit",
                "shipped_at": "2026-07-01T05:30:00+00:00",
                "delivered_at": None,
                "updated_at": "2026-07-02T05:30:00+00:00",
            }
        ]
    }
    rows = read_json_rows(
        TABLES["deliveries"],
        "api/deliveries/20260918/page_0001.json",
        json.dumps(page).encode(),
    )
    assert rows[0]["shipped_at"] == datetime(2026, 7, 1, 5, 30, tzinfo=UTC)
    assert rows[0]["delivered_at"] is None


def test_json_reader_rejects_bad_envelope() -> None:
    with pytest.raises(BronzeReadError, match="envelope field"):
        read_json_rows(TABLES["fx_rates"], "key", b'{"nope": []}')


def test_json_reader_rejects_missing_row_field() -> None:
    page = {"rates": [{"currency": "USD"}]}
    with pytest.raises(BronzeReadError, match="missing field 'rate'"):
        read_json_rows(TABLES["fx_rates"], "key", json.dumps(page).encode())


def test_json_reader_rejects_non_nullable_null() -> None:
    page = {"rates": [{"currency": None, "rate": 1.0}]}
    with pytest.raises(BronzeReadError, match="not nullable"):
        read_json_rows(TABLES["fx_rates"], "key", json.dumps(page).encode())


def test_json_reader_rejects_invalid_json() -> None:
    with pytest.raises(BronzeReadError, match="invalid JSON"):
        read_json_rows(TABLES["fx_rates"], "key", b"{not json")


def test_json_reader_wraps_invalid_decimal_string() -> None:
    page = {"rates": [{"currency": "USD", "rate": "abc"}]}
    with pytest.raises(BronzeReadError, match="rate"):
        read_json_rows(TABLES["fx_rates"], "key", json.dumps(page).encode())


def test_list_data_objects_filters_by_suffix() -> None:
    storage = FakeStorage()
    storage.put_object(BUCKET_ARCHIVE, ORDERS_KEY, orders_parquet_body())
    storage.put_object(
        BUCKET_ARCHIVE,
        "_manifests/postgres-orders/postgres-orders-20260918.json",
        b"{}",
    )
    storage.put_object(BUCKET_ARCHIVE, "postgres/orders/2026/09/17/data.parquet", b"other-day")

    assert list_data_objects(storage, TABLES["orders"], LOGICAL_DATE) == (ORDERS_KEY,)
