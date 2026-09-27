"""Unit tests for the reproducible serving benchmark."""

import pytest

from fakes.clickhouse import FakeClickHouseExecutor
from fakes.trino import FakeTrinoExecutor
from omni_retail.serving.clickhouse.benchmark import render_report, run_benchmark


def test_benchmark_uses_equivalent_queries_and_reports_plans() -> None:
    trino = FakeTrinoExecutor()
    trino.fetch_by_prefix = {
        "EXPLAIN ANALYZE": [("trino analyze",)],
        "EXPLAIN": [("trino plan",)],
        "SELECT": [("2026-09-18", "emea", 10)],
    }
    clickhouse = FakeClickHouseExecutor()
    clickhouse.query_rows = [("2026-09-18", "emea", 10)]

    results = run_benchmark(trino, clickhouse, repetitions=3)
    report = render_report(results, repetitions=3)

    assert [result.row_count for result in results] == [1, 1]
    assert len(results[0].timings_ms) == 3
    assert len(results[1].timings_ms) == 3
    assert "trino plan" in report
    assert "Trino -> Iceberg" in report
    assert "ClickHouse" in report
    assert any("iceberg.analytics.mart_daily_sales" in statement for statement in trino.statements)
    assert any("analytics.mart_daily_sales" in statement for statement in clickhouse.commands)


def test_benchmark_requires_cold_and_warm_samples() -> None:
    with pytest.raises(ValueError, match="at least 2"):
        run_benchmark(FakeTrinoExecutor(), FakeClickHouseExecutor(), repetitions=1)
