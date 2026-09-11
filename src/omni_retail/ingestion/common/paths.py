"""Deterministic S3 object key constructors for the ingestion flow.

Buckets (``landing``, ``archive``, ``rejected``) are separate MinIO buckets;
the functions below build keys *within* those buckets. All functions are
pure: the same inputs always produce the same key, so re-running a logical
batch addresses exactly the same objects.
"""

from datetime import date
from pathlib import PurePosixPath

BUCKET_LANDING = "landing"
BUCKET_ARCHIVE = "archive"
BUCKET_REJECTED = "rejected"


def incoming_key(source: str, filename: str) -> str:
    """Drop-zone key inside the landing bucket: ``<source>/incoming/<filename>``."""
    return f"{source}/incoming/{filename}"


def processing_key(source: str, filename: str) -> str:
    """Transit key inside the landing bucket marking an in-flight run."""
    return f"{source}/processing/{filename}"


def archive_key(source: str, filename: str, run_date: date) -> str:
    """Durable raw key inside the archive bucket: ``<source>/<yyyy>/<mm>/<dd>/<filename>``."""
    return f"{source}/{run_date:%Y/%m/%d}/{filename}"


def rejected_key(source: str, filename: str, run_date: date) -> str:
    """Quarantine key for structurally broken files inside the rejected bucket."""
    return f"{source}/{run_date:%Y/%m/%d}/{filename}"


def badrows_key(source: str, filename: str, run_date: date) -> str:
    """Quarantine key for the malformed rows of an otherwise valid file."""
    key = rejected_key(source, filename, run_date)
    extension = PurePosixPath(filename).suffix.lstrip(".")
    if extension:
        return f"{key}.badrows.{extension}"
    return f"{key}.badrows"


def rejection_key(source: str, filename: str, run_date: date) -> str:
    """Sidecar key holding the rejection reason for a broken file."""
    return f"{rejected_key(source, filename, run_date)}.rejection.json"


def manifest_key(source: str, batch_id: str) -> str:
    """Batch manifest key: ``_manifests/<source>/<batch_id>.json`` (archive bucket)."""
    return f"_manifests/{source}/{batch_id}.json"


def dedup_key(source: str, checksum: str) -> str:
    """Duplicate-detection marker key: ``_dedup/<source>/<sha256>.json`` (archive bucket)."""
    return f"_dedup/{source}/{checksum}.json"
