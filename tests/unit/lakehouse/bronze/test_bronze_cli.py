"""Unit tests for the Bronze CLI with fakes and no network."""

from datetime import date
from typing import Literal

import pytest

from fakes.storage import FakeStorage
from fakes.trino import FakeTrinoExecutor
from omni_retail.lakehouse.bronze import cli
from omni_retail.lakehouse.bronze.cli import build_parser, main, run_all, run_new, run_one
from omni_retail.lakehouse.bronze.loader import LoadError, LoadResult
from omni_retail.lakehouse.bronze.specs import TABLES, BronzeTableSpec

LOGICAL_DATE = date(2026, 9, 18)


def test_parser_run_parses_source_and_date() -> None:
    args = build_parser().parse_args(["run", "--source", "orders", "--date", "2026-09-18"])
    assert args.command == "run"
    assert args.source == "orders"
    assert args.date == LOGICAL_DATE


def test_parser_run_all_parses_date() -> None:
    args = build_parser().parse_args(["run-all", "--date", "2026-09-18"])
    assert args.command == "run-all"
    assert args.date == LOGICAL_DATE


def test_parser_run_new_takes_no_arguments() -> None:
    args = build_parser().parse_args(["run-new"])
    assert args.command == "run-new"


def test_run_one_returns_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[object] = []

    def fake_load(*args: object, **kwargs: object) -> LoadResult:
        calls.append(args)
        return _result("loaded")

    monkeypatch.setattr(cli, "load_with_retry", fake_load)
    assert run_one(TABLES["orders"], FakeStorage(), FakeTrinoExecutor(), LOGICAL_DATE) == 0
    assert len(calls) == 1


def test_run_all_loads_every_source_and_tolerates_empty_days(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loaded: list[str] = []

    def fake_load(
        storage: object,
        executor: object,
        spec: BronzeTableSpec,
        *,
        logical_date: date,
        clock: object | None = None,
        catalog: str = "iceberg",
        schema: str = "bronze",
    ) -> LoadResult:
        loaded.append(spec.source_key)
        return _result("empty")

    monkeypatch.setattr(cli, "load_with_retry", fake_load)
    assert run_all(FakeStorage(), FakeTrinoExecutor(), LOGICAL_DATE) == 0
    assert len(loaded) == 10


def test_run_all_fails_fast_on_load_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_load(
        storage: object,
        executor: object,
        spec: BronzeTableSpec,
        *,
        logical_date: date,
        clock: object | None = None,
        catalog: str = "iceberg",
        schema: str = "bronze",
    ) -> LoadResult:
        raise LoadError("boom")

    monkeypatch.setattr(cli, "load_with_retry", fake_load)
    with pytest.raises(LoadError):
        run_all(FakeStorage(), FakeTrinoExecutor(), LOGICAL_DATE)


def test_run_new_loads_every_source_and_tolerates_uptodate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    called: list[str] = []

    def fake_load_new(
        storage: object,
        executor: object,
        spec: BronzeTableSpec,
        **kwargs: object,
    ) -> list[LoadResult]:
        called.append(spec.source_key)
        return []

    monkeypatch.setattr(cli, "load_new", fake_load_new)
    assert run_new(FakeStorage(), FakeTrinoExecutor()) == 0
    assert len(called) == 10


def test_run_new_fails_fast_on_load_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_load_new(*args: object, **kwargs: object) -> list[LoadResult]:
        raise LoadError("boom")

    monkeypatch.setattr(cli, "load_new", fake_load_new)
    with pytest.raises(LoadError):
        run_new(FakeStorage(), FakeTrinoExecutor())


def test_main_unknown_source_returns_one() -> None:
    assert main(["run", "--source", "nope", "--date", "2026-09-18"]) == 1


def _result(status: Literal["loaded", "empty"]) -> LoadResult:
    return LoadResult("orders", "postgres-orders-20260918", LOGICAL_DATE, 0, status)
