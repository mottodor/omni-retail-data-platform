"""Unit tests for watermark persistence (fake storage)."""

from datetime import UTC, datetime

import pytest

from fakes.storage import FakeStorage
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE, postgres_watermark_key
from omni_retail.ingestion.postgres_snapshot.tables import TABLES
from omni_retail.ingestion.postgres_snapshot.watermark import (
    Watermark,
    load_watermark,
    purge_watermarks,
    save_watermark,
)

ORDERS = TABLES["orders"]


@pytest.fixture()
def purger_storage() -> FakeStorage:
    return FakeStorage()


def test_watermark_key_is_stable_and_namespaced() -> None:
    assert postgres_watermark_key("orders") == "_watermarks/postgres/orders.json"


def test_save_then_load_roundtrips() -> None:
    storage = FakeStorage()
    watermark = Watermark(
        table="orders",
        updated_at=datetime(2026, 9, 12, 10, 30, tzinfo=UTC),
        pk=42,
    )
    save_watermark(storage, watermark)

    loaded = load_watermark(storage, ORDERS)

    assert loaded == watermark


def test_load_returns_none_when_no_watermark_exists() -> None:
    assert load_watermark(FakeStorage(), ORDERS) is None


def test_save_is_idempotent_overwrite_of_same_object() -> None:
    storage = FakeStorage()
    first = Watermark("orders", datetime(2026, 9, 12, tzinfo=UTC), 1)
    second = Watermark("orders", datetime(2026, 9, 13, tzinfo=UTC), 2)

    save_watermark(storage, first)
    save_watermark(storage, second)

    assert load_watermark(storage, ORDERS) == second
    assert len(storage.stored_objects()) == 1


def test_watermark_rejects_naive_datetimes() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Watermark("orders", datetime(2026, 9, 12, 10, 30), 7)


def test_purge_watermarks_all_tables(purger_storage: FakeStorage) -> None:
    for name in ("orders", "customers"):
        purger_storage.put_object(BUCKET_ARCHIVE, postgres_watermark_key(name), b"{}")

    purged = purge_watermarks(purger_storage)

    assert sorted(purged) == [
        "_watermarks/postgres/customers.json",
        "_watermarks/postgres/orders.json",
    ]
    assert purger_storage.list_object_keys(BUCKET_ARCHIVE, "_watermarks/postgres/") == ()


def test_purge_watermarks_single_table(purger_storage: FakeStorage) -> None:
    for name in ("orders", "customers"):
        purger_storage.put_object(BUCKET_ARCHIVE, postgres_watermark_key(name), b"{}")

    purged = purge_watermarks(purger_storage, tables=["orders"])

    assert purged == ("_watermarks/postgres/orders.json",)
    assert purger_storage.object_exists(BUCKET_ARCHIVE, postgres_watermark_key("customers"))


def test_purge_watermarks_is_idempotent(purger_storage: FakeStorage) -> None:
    purger_storage.put_object(BUCKET_ARCHIVE, postgres_watermark_key("orders"), b"{}")

    assert purge_watermarks(purger_storage, tables=["orders"]) == (
        "_watermarks/postgres/orders.json",
    )
    assert purge_watermarks(purger_storage, tables=["orders"]) == ()


def test_purge_watermarks_rejects_unknown_table(purger_storage: FakeStorage) -> None:
    with pytest.raises(ValueError, match="unknown snapshot table"):
        purge_watermarks(purger_storage, tables=["nope"])
