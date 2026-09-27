"""Unit tests for the serving publisher CLI (fakes; no network)."""

from pathlib import Path

import pytest

from fakes.clickhouse import FakeClickHouseExecutor
from fakes.trino import FakeTrinoExecutor
from omni_retail.serving.clickhouse import cli
from omni_retail.serving.clickhouse.cli import build_parser, main, run_command
from omni_retail.serving.clickhouse.publisher import PublishError
from omni_retail.serving.clickhouse.specs import MART_DAILY_SALES, MARTS


def trino_seeded_for(*first_columns: str) -> FakeTrinoExecutor:
    """Fake Trino answering one snapshot per mart, keyed by first column."""
    executor = FakeTrinoExecutor()
    executor.fetch_by_prefix = {f'select "{column}"': [(1,)] for column in first_columns}
    return executor


def test_parser_publish_parses_mart() -> None:
    args = build_parser().parse_args(["publish", "--mart", "mart_daily_sales"])
    assert args.command == "publish"
    assert args.mart == "mart_daily_sales"


def test_parser_rebuild_parses_mart() -> None:
    args = build_parser().parse_args(["rebuild", "--mart", "mart_daily_sales"])
    assert args.command == "rebuild"
    assert args.mart == "mart_daily_sales"
    assert args.all is False


def test_parser_rebuild_all() -> None:
    args = build_parser().parse_args(["rebuild", "--all"])
    assert args.mart is None
    assert args.all is True


def test_parser_rebuild_requires_a_target() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["rebuild"])


def test_parser_rebuild_rejects_mart_and_all_together() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["rebuild", "--mart", "mart_daily_sales", "--all"])


def test_parser_publish_still_requires_mart() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["publish"])


def test_run_command_publish_returns_zero() -> None:
    trino = trino_seeded_for("date_key")
    assert run_command("publish", [MART_DAILY_SALES], trino, FakeClickHouseExecutor()) == 0


def test_run_command_rebuild_returns_zero() -> None:
    trino = trino_seeded_for("date_key")
    assert run_command("rebuild", [MART_DAILY_SALES], trino, FakeClickHouseExecutor()) == 0


def test_run_command_rebuild_all_processes_every_mart_in_registry_order() -> None:
    trino = trino_seeded_for(*(spec.column_names[0] for spec in MARTS.values()))
    ch = FakeClickHouseExecutor()
    assert run_command("rebuild", list(MARTS.values()), trino, ch) == 0

    # Every mart gets the full rebuild sequence: serving DDL, staging DDL,
    # truncate staging, swap — in registry order, nothing skipped. DDL
    # statements are compared by their first line (the full statement is
    # multiline); single-line statements must match exactly.
    expected: list[str] = []
    for spec in MARTS.values():
        expected += [
            f"create table if not exists {spec.serving_table}",
            f"create table if not exists {spec.staging_table}",
            f"truncate table {spec.staging_table}",
            f"exchange tables {spec.serving_table} and {spec.staging_table}",
        ]
    assert len(ch.commands) == len(expected)
    for actual, prefix in zip(ch.commands, expected, strict=True):
        if prefix.startswith("create table"):
            assert actual.startswith(prefix + "\n"), actual[:80]
        else:
            assert actual == prefix
    assert [table for table, _, _ in ch.inserts] == [spec.staging_table for spec in MARTS.values()]


def test_main_unknown_mart_returns_one_without_building_clients(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Spec resolution must fail before any client is constructed."""

    def forbidden(*args: object, **kwargs: object) -> None:
        raise AssertionError("clients must not be built for an unknown mart")

    monkeypatch.setattr(cli, "DbapiTrinoExecutor", forbidden)
    monkeypatch.setattr(cli, "ClickHouseConnectClient", forbidden)
    assert main(["publish", "--mart", "nope"]) == 1
    assert main(["rebuild", "--mart", "nope"]) == 1


def test_parser_benchmark_defaults() -> None:
    args = build_parser().parse_args(["benchmark"])
    assert args.command == "benchmark"
    assert args.repetitions == 5
    assert args.output == Path("docs/benchmarks/phase6-trino-vs-clickhouse.md")


def test_main_publish_error_returns_one(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_publish(*args: object, **kwargs: object) -> None:
        raise PublishError("boom")

    monkeypatch.setattr(cli, "publish", failing_publish)
    assert main(["publish", "--mart", "mart_daily_sales"]) == 1
