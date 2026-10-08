"""Unit tests for the file ingestion flow (incoming -> processing -> archive|rejected).

Uses an in-memory fake storage; every scenario maps to a Phase 3
acceptance criterion: idempotent re-runs, quarantine of broken files and
rows, interrupted-run recovery and duplicate detection.
"""

import io
import json
from datetime import UTC, date, datetime

import pytest
from openpyxl import Workbook

from fakes.storage import FakeStorage
from omni_retail.generators.vendor_files.generator import generate_payload
from omni_retail.ingestion.common.checksum import compute_checksum
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    BUCKET_LANDING,
    BUCKET_REJECTED,
    dedup_key,
    manifest_key,
)
from omni_retail.ingestion.common.storage import ObjectStorage
from omni_retail.ingestion.files.flow import (
    BatchOutcome,
    FileFlowError,
    RejectedRowsError,
    process_incoming,
)
from omni_retail.ingestion.files.schemas import (
    HISTORICAL_ORDERS,
    PARTNER_PRODUCTS,
    SUPPLIER_PRICES,
    SUPPLIER_STOCK,
)

RUN_DATE = date(2026, 9, 11)
FIXED_NOW = datetime(2026, 9, 11, 12, 30, tzinfo=UTC)

GOOD_CSV = (
    b"supplier_id,sku,price,currency,valid_from\n"
    b"acme,SKU-1,9.99,EUR,2026-09-01\n"
    b"acme,SKU-2,15.50,EUR,2026-09-01\n"
)
BROKEN_CSV = b"supplier_id,price\nacme,9.99\n"  # header misses sku/currency/valid_from
PARTIAL_CSV = (
    b"supplier_id,sku,price,currency,valid_from\n"
    b"acme,SKU-1,9.99,EUR,2026-09-01\n"
    b"acme,SKU-2,broken,EUR,2026-09-01\n"
)
GOOD_JSON = json.dumps(
    [
        {
            "partner_id": "p1",
            "sku": "SKU-1",
            "title": "Widget",
            "brand": "Acme",
            "category": "tools",
        },
    ]
).encode()


def fixed_clock() -> datetime:
    return FIXED_NOW


def xlsx_payload(rows: list[list[object]]) -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    assert sheet is not None
    for row in rows:
        sheet.append(row)
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def seed_incoming(storage: ObjectStorage, filename: str, payload: bytes) -> None:
    storage.put_object(BUCKET_LANDING, f"supplier-prices/incoming/{filename}", payload)


def outcome_for(outcomes: tuple[BatchOutcome, ...], filename: str) -> BatchManifest:
    matches = [o.manifest for o in outcomes if o.filename == filename]
    assert len(matches) == 1, f"expected exactly one outcome for {filename}"
    return matches[0]


def test_valid_file_is_archived_with_manifest_and_dedup_marker() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "prices.csv", GOOD_CSV)

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    manifest = outcome_for(outcomes, "prices.csv")
    assert manifest.status == "completed"
    assert manifest.row_count == 2
    assert manifest.rejected_row_count == 0
    assert manifest.logical_date == RUN_DATE
    assert manifest.source_kind == "file"
    assert manifest.object_key == "supplier-prices/2026/09/11/prices.csv"
    assert manifest.ingested_at == FIXED_NOW

    archived = storage.stored_objects()
    assert archived[(BUCKET_ARCHIVE, "supplier-prices/2026/09/11/prices.csv")] == GOOD_CSV
    assert (BUCKET_ARCHIVE, manifest_key("supplier-prices", manifest.batch_id)) in archived
    assert (BUCKET_ARCHIVE, dedup_key("supplier-prices", manifest.checksum)) in archived
    # transit and drop zone are cleaned up
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()


def test_batch_id_is_content_addressed() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "prices.csv", GOOD_CSV)

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    manifest = outcome_for(outcomes, "prices.csv")
    assert manifest.batch_id == f"supplier-prices-{manifest.checksum[:16]}"


def test_rerun_of_same_batch_creates_no_duplicates() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "prices.csv", GOOD_CSV)
    first = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    seed_incoming(storage, "prices.csv", GOOD_CSV)
    second = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    first_manifest = outcome_for(first, "prices.csv")
    assert first_manifest.status == "completed"
    assert outcome_for(second, "prices.csv").status == "duplicate"
    canonical = BatchManifest.from_json(
        storage.get_object(
            BUCKET_ARCHIVE,
            manifest_key("supplier-prices", first_manifest.batch_id),
        ).decode()
    )
    assert canonical.status == "completed"
    assert canonical.row_count == 2
    archived_files = [
        key
        for bucket, key in storage.stored_objects()
        if bucket == BUCKET_ARCHIVE and key.startswith("supplier-prices/2026")
    ]
    assert archived_files == ["supplier-prices/2026/09/11/prices.csv"]
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()


def test_duplicate_marker_without_completed_manifest_fails_loudly() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "prices.csv", GOOD_CSV)
    first = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)
    manifest = outcome_for(first, "prices.csv")
    storage.delete_object(
        BUCKET_ARCHIVE,
        manifest_key("supplier-prices", manifest.batch_id),
    )
    seed_incoming(storage, "prices.csv", GOOD_CSV)

    with pytest.raises(FileFlowError, match="duplicate evidence"):
        process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)


def test_same_archive_path_with_different_content_is_rejected_without_overwrite() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "prices.csv", GOOD_CSV)
    first = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)
    original = outcome_for(first, "prices.csv")
    replacement = GOOD_CSV + b"acme,SKU-3,1.00,EUR,2026-09-02\n"
    seed_incoming(storage, "prices.csv", replacement)

    second = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    assert outcome_for(second, "prices.csv").status == "rejected"
    assert any(
        "archive path collision" in reason
        for reason in outcome_for(second, "prices.csv").rejection_reasons
    )
    assert storage.get_object(BUCKET_ARCHIVE, original.object_key) == GOOD_CSV


def test_same_content_under_new_filename_is_duplicate() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "prices.csv", GOOD_CSV)
    process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    seed_incoming(storage, "prices_reuploaded.csv", GOOD_CSV)
    second = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    assert outcome_for(second, "prices_reuploaded.csv").status == "duplicate"
    keys = [
        key
        for bucket, key in storage.stored_objects()
        if bucket == BUCKET_ARCHIVE and key.startswith("supplier-prices/2026")
    ]
    assert keys == ["supplier-prices/2026/09/11/prices.csv"]


def test_broken_file_is_quarantined_with_reason() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "broken.csv", BROKEN_CSV)

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    manifest = outcome_for(outcomes, "broken.csv")
    assert manifest.status == "rejected"
    assert any("sku" in reason for reason in manifest.rejection_reasons)

    stored = storage.stored_objects()
    assert (BUCKET_REJECTED, "supplier-prices/2026/09/11/broken.csv") in stored
    reasons = json.loads(
        stored[(BUCKET_REJECTED, "supplier-prices/2026/09/11/broken.csv.rejection.json")]
    )
    assert any("sku" in reason for reason in reasons)
    # rejected files do not create dedup markers or archived copies
    assert storage.list_object_keys(BUCKET_ARCHIVE, "_dedup/") == ()
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()


def test_bad_rows_are_quarantined_and_file_archived() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "partial.csv", PARTIAL_CSV)

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    manifest = outcome_for(outcomes, "partial.csv")
    assert manifest.status == "completed"
    assert manifest.row_count == 2
    assert manifest.rejected_row_count == 1

    stored = storage.stored_objects()
    assert (BUCKET_ARCHIVE, "supplier-prices/2026/09/11/partial.csv") in stored
    badrows = stored[(BUCKET_REJECTED, "supplier-prices/2026/09/11/partial.csv.badrows.csv")]
    assert b"SKU-2" in badrows
    assert b"_rejection_reason" in badrows


def test_fail_on_rejected_raises_for_bad_rows() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "partial.csv", PARTIAL_CSV)

    with pytest.raises(RejectedRowsError):
        process_incoming(
            storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock, fail_on_rejected=True
        )


class MarkerFailOnceStorage(FakeStorage):
    def __init__(self) -> None:
        super().__init__()
        self.fail_marker = True

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        if self.fail_marker and bucket == BUCKET_ARCHIVE and key.startswith("_dedup/"):
            self.fail_marker = False
            raise RuntimeError("injected marker write failure")
        super().put_object(bucket, key, body)


def test_completed_manifest_precedes_marker_and_interrupted_run_recovers() -> None:
    storage = MarkerFailOnceStorage()
    seed_incoming(storage, "prices.csv", GOOD_CSV)
    checksum = compute_checksum(GOOD_CSV)
    batch_id = f"supplier-prices-{checksum[:16]}"

    with pytest.raises(RuntimeError, match="marker write failure"):
        process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    canonical = BatchManifest.from_json(
        storage.get_object(
            BUCKET_ARCHIVE,
            manifest_key("supplier-prices", batch_id),
        ).decode()
    )
    assert canonical.status == "completed"
    assert not storage.object_exists(BUCKET_ARCHIVE, dedup_key("supplier-prices", checksum))

    recovered = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    assert outcome_for(recovered, "prices.csv").status == "completed"
    assert storage.object_exists(BUCKET_ARCHIVE, dedup_key("supplier-prices", checksum))
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()


def test_stranded_processing_object_is_reprocessed() -> None:
    storage = FakeStorage()
    storage.put_object(BUCKET_LANDING, "supplier-prices/processing/prices.csv", GOOD_CSV)

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    manifest = outcome_for(outcomes, "prices.csv")
    assert manifest.status == "completed"
    assert (BUCKET_ARCHIVE, "supplier-prices/2026/09/11/prices.csv") in storage.stored_objects()
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()


def test_stranded_processing_with_incoming_copy_cleans_both() -> None:
    storage = FakeStorage()
    storage.put_object(BUCKET_LANDING, "supplier-prices/processing/prices.csv", GOOD_CSV)
    seed_incoming(storage, "prices.csv", GOOD_CSV)

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    assert len(outcomes) == 1
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()


def test_completed_batch_left_in_processing_is_treated_as_duplicate() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "prices.csv", GOOD_CSV)
    process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)
    # simulate a stale leftover of a completed batch
    storage.put_object(BUCKET_LANDING, "supplier-prices/processing/prices.csv", GOOD_CSV)

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    manifest = outcome_for(outcomes, "prices.csv")
    assert manifest.status == "duplicate"
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()


def test_extension_mismatch_is_quarantined() -> None:
    storage = FakeStorage()
    storage.put_object(BUCKET_LANDING, "supplier-prices/incoming/prices.json", GOOD_JSON)

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    manifest = outcome_for(outcomes, "prices.json")
    assert manifest.status == "rejected"
    assert any("extension" in reason for reason in manifest.rejection_reasons)


def test_json_source_flow_archives_payload() -> None:
    storage = FakeStorage()
    storage.put_object(BUCKET_LANDING, "partner-products/incoming/products.json", GOOD_JSON)

    outcomes = process_incoming(storage, PARTNER_PRODUCTS, RUN_DATE, clock=fixed_clock)

    manifest = outcome_for(outcomes, "products.json")
    assert manifest.status == "completed"
    assert manifest.row_count == 1
    assert manifest.object_key == "partner-products/2026/09/11/products.json"


def test_no_incoming_objects_returns_empty_result() -> None:
    storage = FakeStorage()

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock)

    assert outcomes == ()


def test_limit_restricts_number_of_processed_objects() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "a.csv", GOOD_CSV)
    seed_incoming(storage, "b.csv", GOOD_CSV + b"acme,SKU-3,1.00,EUR,2026-09-02\n")

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE, clock=fixed_clock, limit=1)

    assert len(outcomes) == 1
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-prices/incoming/") != ()


def test_parquet_source_flow_archives_payload() -> None:
    storage = FakeStorage()
    payload = generate_payload(HISTORICAL_ORDERS, seed=7, rows=10, run_date=RUN_DATE)
    storage.put_object(BUCKET_LANDING, "historical-orders/incoming/orders.parquet", payload)

    outcomes = process_incoming(storage, HISTORICAL_ORDERS, RUN_DATE, clock=fixed_clock)

    manifest = outcome_for(outcomes, "orders.parquet")
    assert manifest.status == "completed"
    assert manifest.row_count == 10
    assert manifest.object_key == "historical-orders/2026/09/11/orders.parquet"
    assert storage.stored_objects()[(BUCKET_ARCHIVE, manifest.object_key)] == payload
    assert storage.list_object_keys(BUCKET_LANDING, "historical-orders/") == ()


def test_xlsx_source_with_bad_rows_quarantines_rows_and_archives_file() -> None:
    storage = FakeStorage()
    payload = xlsx_payload(
        [
            ["supplier_id", "sku", "quantity", "updated_at"],
            ["acme", "AC-S0001", 42, date(2026, 3, 5)],
            ["acme", "AC-S0002", "many", date(2026, 3, 5)],
        ]
    )
    storage.put_object(BUCKET_LANDING, "supplier-stock/incoming/stock.xlsx", payload)

    outcomes = process_incoming(storage, SUPPLIER_STOCK, RUN_DATE, clock=fixed_clock)

    manifest = outcome_for(outcomes, "stock.xlsx")
    assert manifest.status == "completed"
    assert manifest.row_count == 2
    assert manifest.rejected_row_count == 1
    badrows = storage.stored_objects()[
        (BUCKET_REJECTED, "supplier-stock/2026/09/11/stock.xlsx.badrows.xlsx")
    ]
    assert b"AC-S0002" in badrows
    assert b"_rejection_reason" in badrows
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-stock/") == ()
