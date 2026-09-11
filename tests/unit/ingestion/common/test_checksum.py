"""Unit tests for chunked sha256 checksums."""

import hashlib

from omni_retail.ingestion.common.checksum import compute_checksum


def test_checksum_matches_hashlib_sha256() -> None:
    payload = b"supplier,sku,price\nacme,SKU-1,9.99\n"

    assert compute_checksum(payload) == hashlib.sha256(payload).hexdigest()


def test_empty_payload_checksum() -> None:
    assert compute_checksum(b"") == hashlib.sha256(b"").hexdigest()


def test_checksum_is_deterministic() -> None:
    assert compute_checksum(b"abc") == compute_checksum(b"abc")


def test_different_payloads_produce_different_checksums() -> None:
    assert compute_checksum(b"abc") != compute_checksum(b"abd")


def test_large_payload_is_processed_in_chunks() -> None:
    payload = bytes(range(256)) * 4096

    assert compute_checksum(payload, chunk_size=1024) == hashlib.sha256(payload).hexdigest()
