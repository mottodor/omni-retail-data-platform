"""Integration tests: file ingestion flow against live MinIO.

Phase 3 design spec §12 scenarios: idempotent re-runs, quarantine of
broken files, and the Parquet/XLSX sources added in slice 3.
"""

import json
from collections.abc import Callable
from datetime import date

from omni_retail.generators.vendor_files.generator import (
    generate_payload,
    upload_payload,
)
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    BUCKET_LANDING,
    BUCKET_REJECTED,
    incoming_key,
    rejection_key,
)
from omni_retail.ingestion.common.storage import BotoObjectStorage
from omni_retail.ingestion.files.flow import process_incoming
from omni_retail.ingestion.files.schemas import (
    HISTORICAL_ORDERS,
    SUPPLIER_PRICES,
    SUPPLIER_STOCK,
)

RUN_DATE = date(2026, 9, 11)
SEED = 4242
ROWS = 30


def test_rerun_of_same_file_batch_creates_no_duplicates(
    live_storage: BotoObjectStorage, purged_sources: Callable[..., None]
) -> None:
    purged_sources("supplier-prices")
    payload = generate_payload(SUPPLIER_PRICES, seed=SEED, rows=ROWS, run_date=RUN_DATE)
    upload_payload(live_storage, SUPPLIER_PRICES, payload, seed=SEED, rows=ROWS, run_date=RUN_DATE)

    first = process_incoming(live_storage, SUPPLIER_PRICES, RUN_DATE)

    assert len(first) == 1
    manifest = first[0].manifest
    assert manifest.status == "completed"
    assert manifest.row_count == ROWS
    assert live_storage.get_object(BUCKET_ARCHIVE, manifest.object_key) == payload
    archived_keys = live_storage.list_object_keys(BUCKET_ARCHIVE, "supplier-prices/")
    assert len(archived_keys) == 1

    upload_payload(live_storage, SUPPLIER_PRICES, payload, seed=SEED, rows=ROWS, run_date=RUN_DATE)
    second = process_incoming(live_storage, SUPPLIER_PRICES, RUN_DATE)

    assert second[0].manifest.status == "duplicate"
    assert live_storage.list_object_keys(BUCKET_ARCHIVE, "supplier-prices/") == archived_keys
    assert live_storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()


def test_broken_file_is_quarantined_with_reason(
    live_storage: BotoObjectStorage, purged_sources: Callable[..., None]
) -> None:
    purged_sources("supplier-prices")
    garbage = b"\xff\xfe\x00 not a valid csv payload"
    live_storage.put_object(BUCKET_LANDING, incoming_key("supplier-prices", "broken.csv"), garbage)

    outcomes = process_incoming(live_storage, SUPPLIER_PRICES, RUN_DATE)

    assert len(outcomes) == 1
    manifest = outcomes[0].manifest
    assert manifest.status == "rejected"
    assert manifest.rejection_reasons
    quarantine_key = f"supplier-prices/{RUN_DATE:%Y/%m/%d}/broken.csv"
    assert live_storage.get_object(BUCKET_REJECTED, quarantine_key) == garbage
    reasons = json.loads(
        live_storage.get_object(
            BUCKET_REJECTED, rejection_key("supplier-prices", "broken.csv", RUN_DATE)
        )
    )
    assert isinstance(reasons, list) and reasons
    assert live_storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()
    assert live_storage.list_object_keys(BUCKET_ARCHIVE, "_dedup/supplier-prices/") == ()


def test_parquet_and_xlsx_sources_flow_through_live_storage(
    live_storage: BotoObjectStorage, purged_sources: Callable[..., None]
) -> None:
    purged_sources("historical-orders", "supplier-stock")
    parquet_payload = generate_payload(HISTORICAL_ORDERS, seed=SEED, rows=ROWS, run_date=RUN_DATE)
    upload_payload(
        live_storage, HISTORICAL_ORDERS, parquet_payload, seed=SEED, rows=ROWS, run_date=RUN_DATE
    )
    xlsx_payload = generate_payload(SUPPLIER_STOCK, seed=SEED, rows=ROWS, run_date=RUN_DATE)
    upload_payload(
        live_storage, SUPPLIER_STOCK, xlsx_payload, seed=SEED, rows=ROWS, run_date=RUN_DATE
    )

    parquet_outcomes = process_incoming(live_storage, HISTORICAL_ORDERS, RUN_DATE)
    xlsx_outcomes = process_incoming(live_storage, SUPPLIER_STOCK, RUN_DATE)

    assert parquet_outcomes[0].manifest.status == "completed"
    assert parquet_outcomes[0].manifest.row_count == ROWS
    assert (
        live_storage.get_object(BUCKET_ARCHIVE, parquet_outcomes[0].manifest.object_key)
        == parquet_payload
    )
    assert xlsx_outcomes[0].manifest.status == "completed"
    assert xlsx_outcomes[0].manifest.row_count == ROWS
    xlsx_archived = live_storage.get_object(BUCKET_ARCHIVE, xlsx_outcomes[0].manifest.object_key)
    assert xlsx_archived == xlsx_payload
    assert live_storage.list_object_keys(BUCKET_LANDING, "historical-orders/") == ()
    assert live_storage.list_object_keys(BUCKET_LANDING, "supplier-stock/") == ()
