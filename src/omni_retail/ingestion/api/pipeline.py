"""Durable raw-page persistence for API ingestion batches.

API batches differ from file batches (Phase 3 design spec §3, §5): pages are
already final payloads, so there is no transit ``processing`` stage and no
dedup marker — the page keys themselves are deterministic for a logical date
and a re-run simply overwrites the same objects.
"""

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import httpx

from omni_retail.ingestion.api.client import ApiContractError
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    api_batch_prefix,
    api_page_key,
    manifest_key,
)
from omni_retail.ingestion.common.storage import ObjectStorage

Clock = Callable[[], datetime]


@dataclass(frozen=True)
class RawPage:
    """One raw API response page as received on the wire."""

    page_number: int
    body: bytes
    row_count: int


def api_batch_id(source: str, logical_date: date) -> str:
    """Logical batch identity: ``<source>-<yyyymmdd>`` (date-addressed, not wall-clock)."""
    return f"{source}-{logical_date:%Y%m%d}"


def expect_list(payload: Any, key: str, response: httpx.Response) -> list[Any]:
    """Extract a required list field from a response envelope."""
    value = payload.get(key) if isinstance(payload, dict) else None
    if not isinstance(value, list):
        raise ApiContractError(
            f"GET {response.request.url}: envelope field {key!r} must be a list, "
            f"got {type(value).__name__}"
        )
    return value


def expect_int(payload: Any, key: str, response: httpx.Response) -> int:
    """Extract a required integer field from a response envelope."""
    value = payload.get(key) if isinstance(payload, dict) else None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ApiContractError(
            f"GET {response.request.url}: envelope field {key!r} must be an integer, "
            f"got {type(value).__name__}"
        )
    return value


def persist_api_batch(
    storage: ObjectStorage,
    *,
    source: str,
    schema_version: str,
    logical_date: date,
    pages: tuple[RawPage, ...],
    clock: Clock | None = None,
) -> BatchManifest:
    """Store raw pages and the batch manifest; safe to re-run for the same date.

    Idempotency: page keys and ``batch_id`` depend only on (source, logical
    date, page number), so a retry or backfill of the same date overwrites the
    same objects — duplicate business state cannot appear.
    """
    if not pages:
        raise ValueError(
            f"refusing to persist an empty API batch: source={source} date={logical_date}"
        )
    effective_clock: Clock = clock or (lambda: datetime.now(UTC))

    digest = hashlib.sha256()
    for page in sorted(pages, key=lambda item: item.page_number):
        storage.put_object(
            BUCKET_ARCHIVE,
            api_page_key(source, logical_date, page.page_number),
            page.body,
        )
        digest.update(page.body)

    checksum = digest.hexdigest()
    manifest = BatchManifest(
        batch_id=api_batch_id(source, logical_date),
        source=source,
        source_kind="api",
        status="completed",
        object_key=api_batch_prefix(source, logical_date),
        checksum=checksum,
        size_bytes=sum(len(page.body) for page in pages),
        ingested_at=effective_clock(),
        logical_date=logical_date,
        row_count=sum(page.row_count for page in pages),
        rejected_row_count=0,
        schema_version=schema_version,
    )
    storage.put_object(
        BUCKET_ARCHIVE, manifest_key(source, manifest.batch_id), manifest.to_json().encode()
    )
    return manifest
