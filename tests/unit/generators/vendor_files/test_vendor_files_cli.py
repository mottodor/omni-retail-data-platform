"""Unit tests for the vendor file generator CLI (offline)."""

import json
from datetime import date
from pathlib import Path

import pytest

from fakes.storage import FakeStorage
from omni_retail.generators.vendor_files.cli import build_parser, main, run_generate
from omni_retail.ingestion.common.paths import BUCKET_LANDING


def parse_generate_args(*argv: str) -> object:
    return build_parser().parse_args(["generate", *argv])


def test_generate_writes_file_to_output_dir(tmp_path: Path) -> None:
    args = build_parser().parse_args(
        [
            "generate",
            "--source",
            "partner-products",
            "--rows",
            "5",
            "--seed",
            "3",
            "--date",
            "2026-09-11",
            "--output-dir",
            str(tmp_path / "nested"),
        ]
    )

    assert run_generate(args) == 0

    written = tmp_path / "nested" / "partner-products_20260911_s3_n5.json"
    assert written.is_file()
    document = json.loads(written.read_text(encoding="utf-8"))
    assert len(document) == 5


def test_generate_upload_uses_injected_storage() -> None:
    storage = FakeStorage()
    args = build_parser().parse_args(
        [
            "generate",
            "--source",
            "supplier-prices",
            "--rows",
            "5",
            "--seed",
            "7",
            "--date",
            "2026-09-11",
            "--upload",
        ]
    )

    assert run_generate(args, storage=storage) == 0

    key = "supplier-prices/incoming/supplier-prices_20260911_s7_n5.csv"
    assert (BUCKET_LANDING, key) in storage.stored_objects()


def test_generate_requires_destination() -> None:
    args = build_parser().parse_args(
        ["generate", "--source", "supplier-prices", "--date", "2026-09-11"]
    )

    with pytest.raises(ValueError, match="output-dir"):
        run_generate(args)


def test_main_returns_error_for_unknown_source() -> None:
    assert main(["generate", "--source", "does-not-exist", "--output-dir", "/tmp/opencode"]) == 1


def test_parser_defaults_are_deterministic() -> None:
    args = build_parser().parse_args(["generate", "--source", "supplier-prices"])

    assert args.rows == 200
    assert args.seed == 7
    assert args.date is None or isinstance(args.date, date)
