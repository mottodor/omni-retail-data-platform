"""Unit tests for exact-key integration ownership helpers."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

from datetime import UTC, date, datetime

import pytest

from fakes.storage import FakeStorage
from integration.namespace_ownership import (
    ObjectMutationJournal,
    OwnershipError,
    ScopedObjectStorage,
    manifest_object_refs,
    manifest_scoped_storage,
)
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    manifest_key,
)


def manifest(object_key: str = "postgres/orders/2026/09/20/data.parquet") -> BatchManifest:
    return BatchManifest(
        batch_id="postgres-orders-20260920",
        source="postgres-orders",
        source_kind="postgres",
        status="completed",
        object_key=object_key,
        checksum="0" * 64,
        size_bytes=1,
        ingested_at=datetime(2026, 9, 20, 8, 0, tzinfo=UTC),
        logical_date=date(2026, 9, 20),
        row_count=1,
        rejected_row_count=0,
        schema_version="1.0",
    )


def test_journal_restores_created_overwritten_deleted_and_copied_keys() -> None:
    storage = FakeStorage()
    storage.put_object("archive", "existing", b"before")
    storage.put_object("archive", "deleted", b"restore-me")
    storage.put_object("landing", "source", b"copied")

    journal = ObjectMutationJournal(storage)
    journal.put_object("archive", "existing", b"after")
    journal.put_object("archive", "created", b"new")
    journal.delete_object("archive", "deleted")
    journal.copy_object("landing", "source", "archive", "copy")
    journal.rollback()

    assert storage.get_object("archive", "existing") == b"before"
    assert storage.get_object("archive", "deleted") == b"restore-me"
    assert not storage.object_exists("archive", "created")
    assert not storage.object_exists("archive", "copy")
    assert storage.get_object("landing", "source") == b"copied"


def test_context_manager_rolls_back_when_setup_or_test_body_raises() -> None:
    storage = FakeStorage()
    storage.put_object("archive", "owned", b"original")

    with (
        pytest.raises(RuntimeError, match="setup failed"),
        ObjectMutationJournal(storage) as journal,
    ):
        journal.put_object("archive", "owned", b"temporary")
        journal.put_object("archive", "residue", b"temporary")
        raise RuntimeError("setup failed")

    assert storage.get_object("archive", "owned") == b"original"
    assert not storage.object_exists("archive", "residue")


def test_early_key_lease_preserves_original_before_partial_setup_failure() -> None:
    storage = FakeStorage()
    storage.put_object("archive", "first", b"one")
    storage.put_object("archive", "second", b"two")
    journal = ObjectMutationJournal(storage)
    journal.lease_keys((("archive", "first"), ("archive", "second")))

    journal.delete_object("archive", "first")
    journal.put_object("archive", "second", b"changed")
    journal.rollback()

    assert storage.get_object("archive", "first") == b"one"
    assert storage.get_object("archive", "second") == b"two"


def test_prefix_lease_rejects_duplicate_and_overlapping_registration() -> None:
    journal = ObjectMutationJournal(FakeStorage())
    journal.lease_prefix("archive", "api/fx-rates/20260920/")

    with pytest.raises(OwnershipError, match="duplicate prefix"):
        journal.lease_prefix("archive", "api/fx-rates/20260920/")
    with pytest.raises(OwnershipError, match="overlapping prefix"):
        journal.lease_prefix("archive", "api/fx-rates/")
    with pytest.raises(OwnershipError, match="overlaps an existing prefix"):
        journal.lease_key("archive", "api/fx-rates/20260920/page_0001.json")


def test_scoped_view_hides_and_rejects_unowned_objects() -> None:
    storage = FakeStorage()
    storage.put_object("archive", "owned/key.json", b"owned")
    storage.put_object("archive", "foreign/key.json", b"foreign")
    view = ScopedObjectStorage(
        storage,
        keys=(("archive", "owned/key.json"),),
        prefixes=(("archive", "batch/"),),
    )

    assert view.list_object_keys("archive", "") == ("owned/key.json",)
    assert not view.object_exists("archive", "foreign/key.json")
    with pytest.raises(OwnershipError, match="outside"):
        view.get_object("archive", "foreign/key.json")
    view.put_object("archive", "batch/page.json", b"page")
    assert view.list_object_keys("archive", "batch/") == ("batch/page.json",)


def test_manifest_expansion_includes_registry_direct_and_prefixed_data() -> None:
    storage = FakeStorage()
    direct = manifest()
    api = BatchManifest(
        **{
            **direct.__dict__,
            "batch_id": "fx-rates-20260920",
            "source": "fx-rates",
            "source_kind": "api",
            "object_key": "api/fx-rates/20260920/",
        }
    )
    direct_manifest_key = manifest_key(direct.source, direct.batch_id)
    api_manifest_key = manifest_key(api.source, api.batch_id)
    storage.put_object(BUCKET_ARCHIVE, direct.object_key, b"parquet")
    storage.put_object(BUCKET_ARCHIVE, direct_manifest_key, b"manifest")
    storage.put_object(BUCKET_ARCHIVE, "api/fx-rates/20260920/page_0001.json", b"page")
    storage.put_object(BUCKET_ARCHIVE, api_manifest_key, b"manifest")
    storage.put_object(BUCKET_ARCHIVE, "api/fx-rates/20260919/page_0001.json", b"foreign")

    refs = manifest_object_refs(storage, (direct, api))
    view = manifest_scoped_storage(storage, (direct, api))

    assert refs == {
        (BUCKET_ARCHIVE, direct.object_key),
        (BUCKET_ARCHIVE, direct_manifest_key),
        (BUCKET_ARCHIVE, "api/fx-rates/20260920/page_0001.json"),
        (BUCKET_ARCHIVE, api_manifest_key),
    }
    assert view.list_object_keys(BUCKET_ARCHIVE, "api/fx-rates/") == (
        "api/fx-rates/20260920/page_0001.json",
    )
    with pytest.raises(OwnershipError, match="outside"):
        view.get_object(BUCKET_ARCHIVE, "missing")
