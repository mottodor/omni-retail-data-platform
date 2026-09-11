"""Unit tests for deterministic S3 object key constructors."""

from datetime import date

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


def test_bucket_constants() -> None:
    assert (BUCKET_LANDING, BUCKET_ARCHIVE, BUCKET_REJECTED) == ("landing", "archive", "rejected")


def test_incoming_key_layout() -> None:
    assert incoming_key("supplier-prices", "prices.csv") == ("supplier-prices/incoming/prices.csv")


def test_processing_key_layout() -> None:
    assert processing_key("supplier-prices", "prices.csv") == (
        "supplier-prices/processing/prices.csv"
    )


def test_archive_key_is_date_partitioned() -> None:
    assert archive_key("supplier-prices", "prices.csv", date(2026, 9, 11)) == (
        "supplier-prices/2026/09/11/prices.csv"
    )


def test_rejected_key_is_date_partitioned() -> None:
    assert rejected_key("supplier-prices", "prices.csv", date(2026, 9, 11)) == (
        "supplier-prices/2026/09/11/prices.csv"
    )


def test_badrows_key_appends_suffix_before_extension() -> None:
    assert badrows_key("supplier-prices", "prices.csv", date(2026, 9, 11)) == (
        "supplier-prices/2026/09/11/prices.csv.badrows.csv"
    )


def test_badrows_key_without_extension() -> None:
    assert badrows_key("supplier-prices", "prices", date(2026, 9, 11)) == (
        "supplier-prices/2026/09/11/prices.badrows"
    )


def test_rejection_key_uses_json_sidecar() -> None:
    assert rejection_key("supplier-prices", "prices.csv", date(2026, 9, 11)) == (
        "supplier-prices/2026/09/11/prices.csv.rejection.json"
    )


def test_manifest_key_layout() -> None:
    assert manifest_key("supplier-prices", "supplier-prices-abc123") == (
        "_manifests/supplier-prices/supplier-prices-abc123.json"
    )


def test_dedup_key_layout() -> None:
    checksum = "a" * 64
    assert dedup_key("supplier-prices", checksum) == (
        "_dedup/supplier-prices/" + checksum + ".json"
    )


def test_keys_are_deterministic_for_same_inputs() -> None:
    run_date = date(2026, 9, 11)
    assert archive_key("x", "f.csv", run_date) == archive_key("x", "f.csv", run_date)
