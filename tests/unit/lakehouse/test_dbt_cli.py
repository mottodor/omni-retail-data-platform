"""Checks for the clock-stable dbt CLI boundary."""

# pyright: reportMissingImports=false

from __future__ import annotations

import time
from collections.abc import Iterator

import pytest

import omni_retail.lakehouse.dbt_cli as dbt_cli


def values(items: list[float]) -> Iterator[float]:
    yield from items


def test_monotonic_wall_time_ignores_backward_realtime_changes() -> None:
    monotonic_values = values([10.0, 10.25, 11.5])
    stable_time = dbt_cli._monotonic_wall_time(
        wall_time=lambda: 100.0,
        monotonic_time=lambda: next(monotonic_values),
    )

    assert stable_time() == 100.25
    assert stable_time() == 101.5


def test_main_scopes_stable_clock_to_dbt_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[tuple[str, float]] = []

    def fake_dbt_cli(*, prog_name: str) -> None:
        observed.append((prog_name, time.time()))

    monkeypatch.setattr("dbt.cli.main.cli", fake_dbt_cli)
    monkeypatch.setattr("omni_retail.lakehouse.dbt_cli._monotonic_wall_time", lambda: lambda: 123.5)
    original_time = time.time

    dbt_cli.main()

    assert observed == [("dbt", 123.5)]
    assert time.time is original_time
