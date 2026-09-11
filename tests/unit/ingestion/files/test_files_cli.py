"""Unit tests for the file ingestion CLI (offline, fake storage)."""

import argparse

import pytest

from fakes.storage import FakeStorage
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE, BUCKET_LANDING
from omni_retail.ingestion.files.cli import build_parser, main, run_process

GOOD_CSV = b"supplier_id,sku,price,currency,valid_from\nacme,SKU-1,9.99,EUR,2026-09-01\n"
BROKEN_CSV = b"supplier_id,price\nacme,9.99\n"


def seed_incoming(storage: FakeStorage, filename: str, payload: bytes) -> None:
    storage.put_object(BUCKET_LANDING, f"supplier-prices/incoming/{filename}", payload)


def parse_process_args(*argv: str) -> argparse.Namespace:
    return build_parser().parse_args(["process", *argv])


def test_run_process_archives_file_with_explicit_date() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "prices.csv", GOOD_CSV)
    args = parse_process_args("--source", "supplier-prices", "--date", "2026-09-11")

    exit_code = run_process(args, storage=storage)

    assert exit_code == 0
    assert (BUCKET_ARCHIVE, "supplier-prices/2026/09/11/prices.csv") in storage.stored_objects()


def test_run_process_quarantines_broken_file_without_failing() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "broken.csv", BROKEN_CSV)
    args = parse_process_args("--source", "supplier-prices", "--date", "2026-09-11")

    exit_code = run_process(args, storage=storage)

    assert exit_code == 0


def test_run_process_strict_mode_raises_on_bad_rows() -> None:
    storage = FakeStorage()
    payload = b"supplier_id,sku,price,currency,valid_from\nacme,SKU-1,broken,EUR,2026-09-01\n"
    seed_incoming(storage, "partial.csv", payload)
    args = parse_process_args(
        "--source", "supplier-prices", "--date", "2026-09-11", "--fail-on-rejected"
    )

    with pytest.raises(Exception, match="strict mode"):
        run_process(args, storage=storage)


def test_run_process_limit_is_forwarded() -> None:
    storage = FakeStorage()
    seed_incoming(storage, "a.csv", GOOD_CSV)
    seed_incoming(storage, "b.csv", GOOD_CSV + b"acme,SKU-2,1.00,EUR,2026-09-02\n")
    args = parse_process_args("--source", "supplier-prices", "--date", "2026-09-11", "--limit", "1")

    assert run_process(args, storage=storage) == 0
    assert len(storage.list_object_keys(BUCKET_LANDING, "supplier-prices/incoming/")) == 1


def test_main_returns_error_for_unknown_source() -> None:
    assert main(["process", "--source", "does-not-exist"]) == 1


def test_main_returns_error_for_bad_date() -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(["process", "--source", "supplier-prices", "--date", "not-a-date"])
    assert excinfo.value.code == 2
