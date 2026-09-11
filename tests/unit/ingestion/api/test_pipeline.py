"""Unit tests for API batch persistence (raw pages + manifest)."""

import hashlib
import json
from datetime import UTC, date, datetime

import httpx
import pytest

from fakes.storage import FakeStorage
from omni_retail.ingestion.api.client import ApiContractError
from omni_retail.ingestion.api.pipeline import (
    RawPage,
    api_batch_id,
    expect_int,
    expect_list,
    persist_api_batch,
)
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    api_page_key,
    manifest_key,
)

LOGICAL_DATE = date(2026, 9, 10)
FIXED_NOW = datetime(2026, 9, 11, 6, 0, tzinfo=UTC)


def fixed_clock() -> datetime:
    return FIXED_NOW


def make_pages() -> tuple[RawPage, ...]:
    return (
        RawPage(page_number=1, body=b'{"page": 1, "rates": []}', row_count=3),
        RawPage(page_number=2, body=b'{"page": 2, "rates": []}', row_count=2),
    )


def test_api_batch_id_is_date_addressed() -> None:
    assert api_batch_id("fx-rates", LOGICAL_DATE) == "fx-rates-20260910"


def test_persist_api_batch_stores_pages_and_manifest() -> None:
    storage = FakeStorage()
    pages = make_pages()

    manifest = persist_api_batch(
        storage,
        source="fx-rates",
        schema_version="1.0",
        logical_date=LOGICAL_DATE,
        pages=pages,
        clock=fixed_clock,
    )

    expected_checksum = hashlib.sha256(pages[0].body + pages[1].body).hexdigest()
    assert storage.get_object(BUCKET_ARCHIVE, api_page_key("fx-rates", LOGICAL_DATE, 1)) == (
        pages[0].body
    )
    assert storage.get_object(BUCKET_ARCHIVE, api_page_key("fx-rates", LOGICAL_DATE, 2)) == (
        pages[1].body
    )
    assert manifest.batch_id == "fx-rates-20260910"
    assert manifest.source_kind == "api"
    assert manifest.status == "completed"
    assert manifest.checksum == expected_checksum
    assert manifest.size_bytes == len(pages[0].body) + len(pages[1].body)
    assert manifest.row_count == 5
    assert manifest.logical_date == LOGICAL_DATE
    assert manifest.ingested_at == FIXED_NOW
    assert manifest.object_key == "api/fx-rates/20260910/"
    assert (
        json.loads(storage.get_object(BUCKET_ARCHIVE, manifest_key("fx-rates", manifest.batch_id)))[
            "batch_id"
        ]
        == "fx-rates-20260910"
    )


def test_persist_api_batch_is_idempotent_for_same_date() -> None:
    storage = FakeStorage()

    persist_api_batch(
        storage,
        source="marketing-campaigns",
        schema_version="1.0",
        logical_date=LOGICAL_DATE,
        pages=make_pages(),
        clock=fixed_clock,
    )
    second = persist_api_batch(
        storage,
        source="marketing-campaigns",
        schema_version="1.0",
        logical_date=LOGICAL_DATE,
        pages=make_pages(),
        clock=fixed_clock,
    )

    page_keys = storage.list_object_keys(BUCKET_ARCHIVE, "api/marketing-campaigns/20260910/")
    assert page_keys == (
        "api/marketing-campaigns/20260910/page_0001.json",
        "api/marketing-campaigns/20260910/page_0002.json",
    )
    assert second.status == "completed"
    # the re-run overwrote the same objects instead of duplicating them
    manifest_count = len(
        storage.list_object_keys(BUCKET_ARCHIVE, "_manifests/marketing-campaigns/")
    )
    assert manifest_count == 1


def test_persist_api_batch_rejects_empty_page_sets() -> None:
    with pytest.raises(ValueError, match="empty API batch"):
        persist_api_batch(
            FakeStorage(),
            source="fx-rates",
            schema_version="1.0",
            logical_date=LOGICAL_DATE,
            pages=(),
            clock=fixed_clock,
        )


def test_envelope_helpers_reject_malformed_payloads() -> None:
    request = httpx.Request("GET", "http://testserver/api/v1/fx-rates")
    response = httpx.Response(200, request=request)
    with pytest.raises(ApiContractError, match="rates"):
        expect_list({"total_count": 1}, "rates", response)
    with pytest.raises(ApiContractError, match="total_count"):
        expect_int({"total_count": "many"}, "total_count", response)
    assert expect_list({"rates": [1, 2]}, "rates", response) == [1, 2]
    assert expect_int({"total_count": 7}, "total_count", response) == 7
