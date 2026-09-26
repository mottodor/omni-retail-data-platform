"""Unit tests for the serving publisher CLI (fakes; no network)."""

import pytest

from fakes.clickhouse import FakeClickHouseExecutor
from fakes.trino import FakeTrinoExecutor
from omni_retail.serving.clickhouse import cli
from omni_retail.serving.clickhouse.cli import build_parser, main, run_command
from omni_retail.serving.clickhouse.publisher import PublishError
from omni_retail.serving.clickhouse.specs import MART_DAILY_SALES


def test_parser_publish_parses_mart() -> None:
    args = build_parser().parse_args(["publish", "--mart", "mart_daily_sales"])
    assert args.command == "publish"
    assert args.mart == "mart_daily_sales"


def test_parser_rebuild_parses_mart() -> None:
    args = build_parser().parse_args(["rebuild", "--mart", "mart_daily_sales"])
    assert args.command == "rebuild"
    assert args.mart == "mart_daily_sales"


def test_parser_requires_mart() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["publish"])


def test_run_command_publish_returns_zero() -> None:
    trino = FakeTrinoExecutor()
    trino.fetch_by_prefix = {'select "date_key"': [(1,)]}
    assert run_command("publish", MART_DAILY_SALES, trino, FakeClickHouseExecutor()) == 0


def test_run_command_rebuild_returns_zero() -> None:
    trino = FakeTrinoExecutor()
    trino.fetch_by_prefix = {'select "date_key"': [(1,)]}
    assert run_command("rebuild", MART_DAILY_SALES, trino, FakeClickHouseExecutor()) == 0


def test_main_unknown_mart_returns_one() -> None:
    assert main(["publish", "--mart", "nope"]) == 1


def test_main_publish_error_returns_one(monkeypatch: pytest.MonkeyPatch) -> None:
    def failing_publish(*args: object, **kwargs: object) -> None:
        raise PublishError("boom")

    monkeypatch.setattr(cli, "publish", failing_publish)
    assert main(["publish", "--mart", "mart_daily_sales"]) == 1
