"""Live file-ingestion tests with exact object ownership and rollback."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

import json
from datetime import date

from integration.namespace_ownership import (
    ObjectMutationJournal,
    ObjectRef,
    ScopedObjectStorage,
)
from omni_retail.generators.vendor_files.generator import (
    generate_payload,
    make_filename,
    upload_payload,
)
from omni_retail.ingestion.common.checksum import compute_checksum
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    BUCKET_LANDING,
    BUCKET_REJECTED,
    archive_key,
    badrows_key,
    dedup_key,
    incoming_key,
    manifest_key,
    processing_key,
    rejected_key,
    rejection_key,
)
from omni_retail.ingestion.files.flow import process_incoming
from omni_retail.ingestion.files.schemas import (
    HISTORICAL_ORDERS,
    SUPPLIER_PRICES,
    SUPPLIER_STOCK,
    FileSourceSchema,
)

RUN_DATE = date(2026, 9, 11)
SEED = 4242
ROWS = 30


def isolated_file_storage(
    journal: ObjectMutationJournal,
    schema: FileSourceSchema,
    filename: str,
    payload: bytes,
) -> ScopedObjectStorage:
    """Lease and clear only keys one deterministic file batch may mutate."""
    checksum = compute_checksum(payload)
    batch_id = f"{schema.name}-{checksum[:16]}"
    refs: set[ObjectRef] = {
        (BUCKET_LANDING, incoming_key(schema.name, filename)),
        (BUCKET_LANDING, processing_key(schema.name, filename)),
        (BUCKET_ARCHIVE, archive_key(schema.name, filename, RUN_DATE)),
        (BUCKET_ARCHIVE, manifest_key(schema.name, batch_id)),
        (BUCKET_ARCHIVE, dedup_key(schema.name, checksum)),
        (BUCKET_REJECTED, rejected_key(schema.name, filename, RUN_DATE)),
        (BUCKET_REJECTED, rejection_key(schema.name, filename, RUN_DATE)),
        (BUCKET_REJECTED, badrows_key(schema.name, filename, RUN_DATE)),
    }
    journal.lease_keys(refs)
    view = ScopedObjectStorage(journal, keys=refs)
    for bucket, key in refs:
        view.delete_object(bucket, key)
    return view


def generated_batch(
    journal: ObjectMutationJournal, schema: FileSourceSchema
) -> tuple[ScopedObjectStorage, bytes]:
    payload = generate_payload(schema, seed=SEED, rows=ROWS, run_date=RUN_DATE)
    filename = make_filename(schema, seed=SEED, rows=ROWS, run_date=RUN_DATE)
    return isolated_file_storage(journal, schema, filename, payload), payload


def test_rerun_of_same_file_batch_creates_no_duplicates(
    object_journal: ObjectMutationJournal,
) -> None:
    storage, payload = generated_batch(object_journal, SUPPLIER_PRICES)
    upload_payload(storage, SUPPLIER_PRICES, payload, seed=SEED, rows=ROWS, run_date=RUN_DATE)

    first = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE)

    assert len(first) == 1
    manifest = first[0].manifest
    assert manifest.status == "completed"
    assert manifest.row_count == ROWS
    assert storage.get_object(BUCKET_ARCHIVE, manifest.object_key) == payload
    archived_keys = storage.list_object_keys(BUCKET_ARCHIVE, "supplier-prices/")
    assert archived_keys == (manifest.object_key,)

    upload_payload(storage, SUPPLIER_PRICES, payload, seed=SEED, rows=ROWS, run_date=RUN_DATE)
    second = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE)

    assert second[0].manifest.status == "duplicate"
    assert storage.list_object_keys(BUCKET_ARCHIVE, "supplier-prices/") == archived_keys
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()


def test_broken_file_is_quarantined_with_reason(
    object_journal: ObjectMutationJournal,
) -> None:
    garbage = b"\xff\xfe\x00 not a valid csv payload"
    filename = "broken.csv"
    storage = isolated_file_storage(object_journal, SUPPLIER_PRICES, filename, garbage)
    storage.put_object(BUCKET_LANDING, incoming_key("supplier-prices", filename), garbage)

    outcomes = process_incoming(storage, SUPPLIER_PRICES, RUN_DATE)

    assert len(outcomes) == 1
    manifest = outcomes[0].manifest
    assert manifest.status == "rejected"
    assert manifest.rejection_reasons
    quarantine_key = rejected_key("supplier-prices", filename, RUN_DATE)
    assert storage.get_object(BUCKET_REJECTED, quarantine_key) == garbage
    reasons = json.loads(
        storage.get_object(BUCKET_REJECTED, rejection_key("supplier-prices", filename, RUN_DATE))
    )
    assert isinstance(reasons, list) and reasons
    assert storage.list_object_keys(BUCKET_LANDING, "supplier-prices/") == ()
    assert storage.list_object_keys(BUCKET_ARCHIVE, "_dedup/supplier-prices/") == ()


def test_parquet_and_xlsx_sources_flow_through_live_storage(
    object_journal: ObjectMutationJournal,
) -> None:
    parquet_storage, parquet_payload = generated_batch(object_journal, HISTORICAL_ORDERS)
    xlsx_storage, xlsx_payload = generated_batch(object_journal, SUPPLIER_STOCK)
    upload_payload(
        parquet_storage,
        HISTORICAL_ORDERS,
        parquet_payload,
        seed=SEED,
        rows=ROWS,
        run_date=RUN_DATE,
    )
    upload_payload(
        xlsx_storage,
        SUPPLIER_STOCK,
        xlsx_payload,
        seed=SEED,
        rows=ROWS,
        run_date=RUN_DATE,
    )

    parquet_outcomes = process_incoming(parquet_storage, HISTORICAL_ORDERS, RUN_DATE)
    xlsx_outcomes = process_incoming(xlsx_storage, SUPPLIER_STOCK, RUN_DATE)

    assert parquet_outcomes[0].manifest.status == "completed"
    assert parquet_outcomes[0].manifest.row_count == ROWS
    assert (
        parquet_storage.get_object(BUCKET_ARCHIVE, parquet_outcomes[0].manifest.object_key)
        == parquet_payload
    )
    assert xlsx_outcomes[0].manifest.status == "completed"
    assert xlsx_outcomes[0].manifest.row_count == ROWS
    assert (
        xlsx_storage.get_object(BUCKET_ARCHIVE, xlsx_outcomes[0].manifest.object_key)
        == xlsx_payload
    )
    assert parquet_storage.list_object_keys(BUCKET_LANDING, "historical-orders/") == ()
    assert xlsx_storage.list_object_keys(BUCKET_LANDING, "supplier-stock/") == ()
