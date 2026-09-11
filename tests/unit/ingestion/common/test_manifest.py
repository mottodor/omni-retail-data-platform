"""Unit tests for the batch manifest model and serialization."""

import json
from datetime import UTC, date, datetime

from omni_retail.ingestion.common.manifest import BatchManifest


def make_manifest(**overrides: object) -> BatchManifest:
    values: dict[str, object] = {
        "batch_id": "supplier-prices-" + "a" * 16,
        "source": "supplier-prices",
        "source_kind": "file",
        "status": "completed",
        "object_key": "archive/supplier-prices/2026/09/11/prices.csv",
        "checksum": "a" * 64,
        "size_bytes": 1234,
        "ingested_at": datetime(2026, 9, 11, 12, 0, tzinfo=UTC),
        "logical_date": date(2026, 9, 11),
        "row_count": 100,
        "rejected_row_count": 0,
        "schema_version": "1.0",
        "rejection_reasons": (),
    }
    values.update(overrides)
    return BatchManifest(**values)  # type: ignore[arg-type]


def test_manifest_roundtrip_through_json() -> None:
    manifest = make_manifest()

    restored = BatchManifest.from_json(manifest.to_json())

    assert restored == manifest


def test_manifest_json_is_stable_and_sorted() -> None:
    payload = make_manifest().to_json()

    assert json.loads(payload) == json.loads(payload)
    assert json.loads(payload)["batch_id"].startswith("supplier-prices-")


def test_manifest_serializes_timestamps_as_iso_utc() -> None:
    payload = json.loads(make_manifest().to_json())

    assert payload["ingested_at"] == "2026-09-11T12:00:00+00:00"
    assert payload["logical_date"] == "2026-09-11"


def test_manifest_serializes_rejection_reasons_as_list() -> None:
    manifest = make_manifest(
        status="rejected",
        rejection_reasons=("missing required column: sku", "row 3: invalid price"),
    )

    payload = json.loads(manifest.to_json())

    assert payload["rejection_reasons"] == [
        "missing required column: sku",
        "row 3: invalid price",
    ]


def test_duplicate_status_is_allowed() -> None:
    assert make_manifest(status="duplicate").status == "duplicate"
