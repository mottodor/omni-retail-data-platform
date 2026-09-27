"""Small reproducible Trino-versus-ClickHouse serving benchmark."""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path

from omni_retail.lakehouse.bronze.loader import TrinoExecutor
from omni_retail.serving.clickhouse.client import ClickHouseExecutor

BENCHMARK_QUERY = """
SELECT order_date, region,
       sum(revenue_eur) AS revenue_eur,
       sum(margin_eur) AS margin_eur,
       sum(orders_count) AS orders_count
FROM {table}
GROUP BY order_date, region
ORDER BY order_date, region
""".strip()


@dataclass(frozen=True)
class BenchmarkResult:
    """Measured query timings and plan output for one engine."""

    engine: str
    table: str
    row_count: int
    timings_ms: tuple[float, ...]
    explain: tuple[str, ...]
    explain_analyze: tuple[str, ...]

    @property
    def first_ms(self) -> float:
        return self.timings_ms[0]

    @property
    def median_ms(self) -> float:
        return statistics.median(self.timings_ms)

    @property
    def p95_ms(self) -> float:
        ordered = sorted(self.timings_ms)
        index = min(len(ordered) - 1, max(0, int(len(ordered) * 0.95) - 1))
        return ordered[index]

    @property
    def min_ms(self) -> float:
        return min(self.timings_ms)

    @property
    def max_ms(self) -> float:
        return max(self.timings_ms)


def _measure(
    engine: str,
    table: str,
    query: str,
    explain: list[tuple[object, ...]],
    explain_analyze: list[tuple[object, ...]],
    fetch: Callable[[str], list[tuple[object, ...]]],
    repetitions: int,
) -> BenchmarkResult:
    timings: list[float] = []
    rows: list[tuple[object, ...]] = []
    for _ in range(repetitions):
        started = time.perf_counter()
        rows = fetch(query)
        timings.append((time.perf_counter() - started) * 1000)
    return BenchmarkResult(
        engine=engine,
        table=table,
        row_count=len(rows),
        timings_ms=tuple(timings),
        explain=tuple(str(value) for row in explain for value in row),
        explain_analyze=tuple(str(value) for row in explain_analyze for value in row),
    )


def run_benchmark(
    trino: TrinoExecutor,
    clickhouse: ClickHouseExecutor,
    *,
    repetitions: int = 5,
) -> tuple[BenchmarkResult, BenchmarkResult]:
    """Run the same aggregate query against Gold and its serving copy.

    The first sample is retained as the cold-start proxy and the remaining
    samples represent the warm series. The benchmark deliberately reports
    observed timings rather than asserting that one engine is universally
    faster.
    """
    if repetitions < 2:
        raise ValueError("repetitions must be at least 2 to compare cold and warm runs")

    trino_table = "iceberg.analytics.mart_daily_sales"
    clickhouse_table = "analytics.mart_daily_sales"
    trino_query = BENCHMARK_QUERY.format(table=trino_table)
    clickhouse_query = BENCHMARK_QUERY.format(table=clickhouse_table)

    # Measure before EXPLAIN ANALYZE so the first sample includes connection
    # and initial-read cost instead of work performed by plan capture.
    trino_result = _measure(
        "Trino -> Iceberg",
        trino_table,
        trino_query,
        [],
        [],
        trino.fetch,
        repetitions,
    )
    clickhouse_result = _measure(
        "ClickHouse",
        clickhouse_table,
        clickhouse_query,
        [],
        [],
        clickhouse.query,
        repetitions,
    )

    trino_explain = trino.fetch(f"EXPLAIN {trino_query}")
    trino_explain_analyze = trino.fetch(f"EXPLAIN ANALYZE {trino_query}")
    clickhouse_explain = clickhouse.query(f"EXPLAIN {clickhouse_query}")
    clickhouse_explain_analyze = clickhouse.query(f"EXPLAIN PIPELINE {clickhouse_query}")
    return (
        replace(
            trino_result,
            explain=tuple(str(value) for row in trino_explain for value in row),
            explain_analyze=tuple(str(value) for row in trino_explain_analyze for value in row),
        ),
        replace(
            clickhouse_result,
            explain=tuple(str(value) for row in clickhouse_explain for value in row),
            explain_analyze=tuple(
                str(value) for row in clickhouse_explain_analyze for value in row
            ),
        ),
    )


def render_report(
    results: tuple[BenchmarkResult, BenchmarkResult],
    *,
    repetitions: int,
    generated_at: datetime | None = None,
) -> str:
    """Render a committed-artifact-friendly Markdown benchmark report."""
    timestamp = (generated_at or datetime.now(UTC)).isoformat()
    trino, clickhouse = results
    timing_rows = "\n".join(
        f"| {result.engine} | {result.row_count} | {result.first_ms:.2f} | "
        f"{result.min_ms:.2f} | {result.median_ms:.2f} | {result.p95_ms:.2f} | "
        f"{result.max_ms:.2f} |"
        for result in results
    )
    plan_sections: list[str] = []
    for result in results:
        explain = "\n".join(result.explain)
        explain_analyze = "\n".join(result.explain_analyze)
        plan_sections.append(
            f"### {result.engine}\n\n#### EXPLAIN\n\n```text\n{explain}\n```\n\n"
            f"#### EXPLAIN ANALYZE / PIPELINE\n\n```text\n{explain_analyze}\n```"
        )
    plans = "\n\n".join(plan_sections)
    return f"""# Phase 6 benchmark: Trino vs ClickHouse

Generated at: `{timestamp}`  
Query: aggregate `mart_daily_sales` by `order_date` and `region`  
Repetitions: `{repetitions}`; first sample is the cold-start proxy, remaining samples are warm runs.

## Query

```sql
Trino:
{BENCHMARK_QUERY.format(table=trino.table)}

ClickHouse:
{BENCHMARK_QUERY.format(table=clickhouse.table)}
```

The query is semantically identical; only the fully qualified table name differs:
`iceberg.analytics.mart_daily_sales` for Trino and `analytics.mart_daily_sales` for
ClickHouse.

## Results (milliseconds)

| Engine | Result rows | First/cold | Min | Median | P95 | Max |
|---|---:|---:|---:|---:|---:|---:|
{timing_rows}

## Plans

{plans}

## Interpretation

These are local-workstation observations for the recorded dataset and stack
configuration, not a general performance claim. Repeat after changing data
volume or physical design; retain the query equivalence and plan output when
comparing runs.
"""


def write_report(
    output: Path,
    results: tuple[BenchmarkResult, BenchmarkResult],
    *,
    repetitions: int,
) -> None:
    """Write a benchmark report, creating its parent directory if needed."""
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_report(results, repetitions=repetitions), encoding="utf-8")
