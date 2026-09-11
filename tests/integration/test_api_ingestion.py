"""Integration tests: API ingestion against the live mock external API.

Phase 3 design spec §12 scenarios: a 429 fault is retried to success, and
a date-range backfill is idempotent (re-running overwrites the same
deterministic page keys instead of duplicating objects).
"""

import contextlib
import os
from collections.abc import Callable
from datetime import date

from omni_retail.ingestion.api.cli import run_batch, spec_by_name
from omni_retail.ingestion.api.client import DEFAULT_BASE_URL, ApiClient, ApiClientConfig
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE
from omni_retail.ingestion.common.storage import BotoObjectStorage

BACKFILL_DATES = (date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10))
SMALL_PAGE_SIZE = 7  # forces several pages per day


def live_client_config() -> ApiClientConfig:
    """Fast-but-realistic client config pointing at the live mock service."""
    return ApiClientConfig(
        base_url=os.environ.get("MOCK_API_BASE_URL", DEFAULT_BASE_URL),
        backoff_base_seconds=0.1,
        min_request_interval_seconds=0.05,
    )


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
    live_storage: BotoObjectStorage, purged_sources: Callable[..., None]
) -> None:
    purged_sources("fx-rates")
    spec = spec_by_name("fx-rates")

    with contextlib.closing(ApiClient(live_client_config())) as client:
        manifests = [
            run_batch(spec, client, live_storage, logical_date, SMALL_PAGE_SIZE)
            for logical_date in BACKFILL_DATES
        ]

    assert [manifest.status for manifest in manifests] == ["completed"] * len(BACKFILL_DATES)
    assert all(manifest.row_count > 0 for manifest in manifests)
    page_keys = live_storage.list_object_keys(BUCKET_ARCHIVE, "api/fx-rates/")
    manifest_keys = live_storage.list_object_keys(BUCKET_ARCHIVE, "_manifests/fx-rates/")
    assert len(manifest_keys) == len(BACKFILL_DATES)
    assert len(page_keys) > len(BACKFILL_DATES)  # multiple pages per day

    with contextlib.closing(ApiClient(live_client_config())) as client:
        for logical_date in BACKFILL_DATES:
            run_batch(spec, client, live_storage, logical_date, SMALL_PAGE_SIZE)

    assert live_storage.list_object_keys(BUCKET_ARCHIVE, "api/fx-rates/") == page_keys
    assert live_storage.list_object_keys(BUCKET_ARCHIVE, "_manifests/fx-rates/") == manifest_keys
