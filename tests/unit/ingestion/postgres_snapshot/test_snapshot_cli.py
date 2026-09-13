"""Unit tests for the snapshot CLI argument parsing and validation."""

import pytest

from omni_retail.ingestion.postgres_snapshot.cli import build_parser


def test_run_requires_table_and_date() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["run"])


def test_run_parses_date_and_flags() -> None:
    args = build_parser().parse_args(
        ["run", "--table", "orders", "--date", "2026-09-13", "--full-refresh"]
    )

    assert args.command == "run"
    assert args.table == "orders"
    assert args.date.isoformat() == "2026-09-13"
    assert args.full_refresh is True


def test_run_full_refresh_defaults_to_false() -> None:
    args = build_parser().parse_args(["run", "--table", "orders", "--date", "2026-09-13"])

    assert args.full_refresh is False


def test_run_rejects_malformed_dates() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--table", "orders", "--date", "13.09.2026"])
