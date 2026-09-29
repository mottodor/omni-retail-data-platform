"""Live API-ingestion tests with deterministic batch-prefix leases."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

import contextlib
import os
from datetime import date

from integration.namespace_ownership import ObjectMutationJournal, ObjectRef, ScopedObjectStorage
from omni_retail.ingestion.api.cli import run_batch, spec_by_name
from omni_retail.ingestion.api.client import (
    DEFAULT_BASE_URL,
    ApiClient,
    ApiClientConfig,
)
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    api_batch_prefix,
    manifest_key,
)

BACKFILL_DATES = (date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10))
SMALL_PAGE_SIZE = 7  # forces several pages per day


def live_client_config() -> ApiClientConfig:
    """Fast-but-realistic client config pointing at the live mock service."""
    return ApiClientConfig(
        base_url=os.environ.get("MOCK_API_BASE_URL", DEFAULT_BASE_URL),
        backoff_base_seconds=0.1,
        min_request_interval_seconds=0.05,
    )


def isolated_api_storage(
    journal: ObjectMutationJournal, source: str, logical_dates: tuple[date, ...]
) -> ScopedObjectStorage:
    """Lease and clear only the selected source/day page prefixes and manifests."""
    prefixes: set[ObjectRef] = set()
    keys: set[ObjectRef] = set()
    for logical_date in logical_dates:
        prefix = api_batch_prefix(source, logical_date)
        batch_id = f"{source}-{logical_date:%Y%m%d}"
        journal.lease_prefix(BUCKET_ARCHIVE, prefix)
        prefixes.add((BUCKET_ARCHIVE, prefix))
        manifest_ref = (BUCKET_ARCHIVE, manifest_key(source, batch_id))
        journal.lease_key(*manifest_ref)
        keys.add(manifest_ref)
    view = ScopedObjectStorage(journal, keys=keys, prefixes=prefixes)
    for bucket, prefix in prefixes:
        for key in view.list_object_keys(bucket, prefix):
            view.delete_object(bucket, key)
    for bucket, key in keys:
        view.delete_object(bucket, key)
    return view


def test_api_429_fault_is_retried_and_succeeds() -> None:
    with contextlib.closing(ApiClient(live_client_config())) as client:
        response = client.get(
            "/api/v1/fx-rates",
            {
                "base": "EUR",
                "date": "2026-09-11",
                "page": 1,
                "page_size": SMALL_PAGE_SIZE,
                "fault": "429",
                "fault_rate": 1.0,
            },
        )

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload.get("rates"), list)


def test_api_backfill_range_is_idempotent(
    object_journal: ObjectMutationJournal,
) -> None:
    storage = isolated_api_storage(object_journal, "fx-rates", BACKFILL_DATES)
    spec = spec_by_name("fx-rates")

    with contextlib.closing(ApiClient(live_client_config())) as client:
        manifests = [
            run_batch(spec, client, storage, logical_date, SMALL_PAGE_SIZE)
            for logical_date in BACKFILL_DATES
        ]

    assert [manifest.status for manifest in manifests] == ["completed"] * len(BACKFILL_DATES)
    assert all(manifest.row_count > 0 for manifest in manifests)
    page_keys = tuple(
        key
        for logical_date in BACKFILL_DATES
        for key in storage.list_object_keys(
            BUCKET_ARCHIVE, api_batch_prefix("fx-rates", logical_date)
        )
    )
    manifest_keys = storage.list_object_keys(BUCKET_ARCHIVE, "_manifests/fx-rates/")
    assert len(manifest_keys) == len(BACKFILL_DATES)
    assert len(page_keys) > len(BACKFILL_DATES)  # multiple pages per day

    with contextlib.closing(ApiClient(live_client_config())) as client:
        for logical_date in BACKFILL_DATES:
            run_batch(spec, client, storage, logical_date, SMALL_PAGE_SIZE)

    assert (
        tuple(
            key
            for logical_date in BACKFILL_DATES
            for key in storage.list_object_keys(
                BUCKET_ARCHIVE, api_batch_prefix("fx-rates", logical_date)
            )
        )
        == page_keys
    )
    assert storage.list_object_keys(BUCKET_ARCHIVE, "_manifests/fx-rates/") == manifest_keys
