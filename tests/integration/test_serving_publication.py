"""Live integration: Gold -> ClickHouse serving publication (Phase 6).

Requires the core AND ``bi`` Compose profiles plus OMNI_INTEGRATION=1::

    make up && make bi-up && make integration

Asserts the Phase 6 acceptance criteria at the publication level
(ROADMAP Phase 6, ADR 0004):

- every registered mart publishes, with Trino-side Gold row count ==
  ClickHouse rows written (row parity);
- per-mart reconciliation: full ordered row-by-row equality between the
  Gold snapshot and the serving copy (types normalized — exact mirror);
- publishing twice is idempotent — identical row count and checksum;
- the serving layer is rebuildable from Iceberg Gold with one command:
  drop every serving table -> ``rebuild --all`` -> identical copies;
- ``superset_reader`` can SELECT but cannot INSERT.

The Gold world is whatever the earlier integration files leave behind
(alphabetical order runs ``test_dbt_core_build`` and
``test_lakehouse_orchestration`` first; a standalone run needs one prior
``make dbt-build``). The module skips — with an actionable reason — when
ClickHouse or the Gold marts are not available.
"""

from collections.abc import Generator
from contextlib import closing
from datetime import UTC, datetime

import pytest
from clickhouse_connect.driver.exceptions import Error as ClickHouseError
from trino.exceptions import Error as TrinoError

from integration.lakehouse_seed import trino_scalar
from omni_retail.lakehouse.bronze.loader import DbapiTrinoExecutor, TrinoConfig
from omni_retail.serving.clickhouse import cli
from omni_retail.serving.clickhouse.client import ClickHouseConnectClient
from omni_retail.serving.clickhouse.config import ClickHouseConfig
from omni_retail.serving.clickhouse.publisher import publish, snapshot_sql
from omni_retail.serving.clickhouse.specs import (
    MART_CUSTOMER_LTV,
    MART_DAILY_SALES,
    MARTS,
    MartSpec,
)


def checksum_sql(spec: MartSpec) -> str:
    """Row count + deterministic content hash over every mart column."""
    hashed = ", ".join(f"toString({name})" for name in spec.column_names)
    return f"select count(), sum(cityHash64({hashed})) from {spec.serving_table}"


def gold_row_count(spec: MartSpec) -> int:
    """Guard: the Gold mart must exist and be non-empty (module docstring)."""
    try:
        count = trino_scalar(f"select count(*) from iceberg.analytics.{spec.name}")
    except TrinoError:
        pytest.skip(
            "Gold marts missing — run `make dbt-build` (or the full make integration) first"
        )
    assert isinstance(count, int)
    assert count > 0, f"Gold {spec.name} is empty; seed Gold before serving tests"
    return count


def _normalize_value(value: object) -> object:
    """Make Trino and ClickHouse representations directly comparable.

    The only wrinkle is timestamptz: Trino returns tz-aware datetimes while
    clickhouse-connect returns the DateTime64(.., 'UTC') column as naive UTC
    — both normalize to naive UTC. Everything else round-trips exactly
    (mirrored types, IEEE-754 floats, exact decimals).
    """
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(UTC).replace(tzinfo=None)
        return value
    return value


def _normalize_row(row: tuple[object, ...]) -> tuple[object, ...]:
    return tuple(_normalize_value(value) for value in row)


def _sort_key(row: tuple[object, ...]) -> tuple[tuple[bool, object], ...]:
    """Total order over rows with possible NULLs (NULL sorts independently)."""
    return tuple((value is None, 0 if value is None else value) for value in row)


def assert_reconciled(
    spec: MartSpec,
    trino_executor: DbapiTrinoExecutor,
    ch_client: ClickHouseConnectClient,
) -> int:
    """Gold snapshot == serving copy: same rows, values, and count.

    Rows are compared as order-independent multisets sorted in Python —
    cross-engine ``ORDER BY`` semantics (NULL placement, collation) are
    exactly the kind of subtlety reconciliation should not depend on.
    """
    gold_rows = [_normalize_row(row) for row in trino_executor.fetch(snapshot_sql(spec, "iceberg"))]
    columns = ", ".join(spec.column_names)
    serving_sql = f"select {columns} from {spec.serving_table}"
    serving_rows = [_normalize_row(row) for row in ch_client.query(serving_sql)]
    assert len(gold_rows) > 0, f"{spec.name}: Gold snapshot unexpectedly empty"
    assert len(serving_rows) == len(gold_rows), (
        f"{spec.name}: row parity failed: gold={len(gold_rows)} serving={len(serving_rows)}"
    )
    assert sorted(gold_rows, key=_sort_key) == sorted(serving_rows, key=_sort_key), (
        f"{spec.name}: serving copy diverged from the Gold snapshot"
    )
    return len(gold_rows)


@pytest.fixture(scope="module")
def publisher_client() -> Generator[ClickHouseConnectClient, None, None]:
    try:
        client = ClickHouseConnectClient(ClickHouseConfig.from_env())
        client.query("select 1")
    except ClickHouseError as error:
        pytest.skip(f"ClickHouse not reachable ({error}); start the bi profile: make bi-up")
    yield client
    client.close()


@pytest.fixture(scope="module")
def trino_executor() -> Generator[DbapiTrinoExecutor, None, None]:
    with closing(DbapiTrinoExecutor(TrinoConfig.from_env())) as executor:
        yield executor


@pytest.mark.parametrize("spec", list(MARTS.values()), ids=lambda spec: spec.name)
def test_publish_every_mart_and_reconcile_against_gold(
    spec: MartSpec,
    publisher_client: ClickHouseConnectClient,
    trino_executor: DbapiTrinoExecutor,
) -> None:
    gold_count = gold_row_count(spec)

    result = publish(trino_executor, publisher_client, spec)

    assert result.row_count == gold_count, "(d) row parity: Trino vs publish summary"
    assert assert_reconciled(spec, trino_executor, publisher_client) == gold_count


def test_republish_is_idempotent(
    publisher_client: ClickHouseConnectClient,
    trino_executor: DbapiTrinoExecutor,
) -> None:
    # One of the slice-2 marts (nullable avg_order_value_eur included):
    # two consecutive publishes over the same Gold state produce an
    # identical serving table — no duplicates, no drift.
    spec = MART_CUSTOMER_LTV
    gold_row_count(spec)

    first = publish(trino_executor, publisher_client, spec)
    first_checksum = publisher_client.query(checksum_sql(spec))[0]
    second = publish(trino_executor, publisher_client, spec)

    assert second.row_count == first.row_count
    assert publisher_client.query(checksum_sql(spec))[0] == first_checksum


def test_full_rebuild_from_iceberg_gold(
    publisher_client: ClickHouseConnectClient,
    trino_executor: DbapiTrinoExecutor,
) -> None:
    # The "rebuildable from Gold" acceptance criterion, end to end: drop
    # EVERY serving table (serving + staging pairs), then one command —
    # the real CLI entrypoint — rebuilds and republishes all of them.
    for spec in MARTS.values():
        gold_row_count(spec)
        publisher_client.command(f"drop table if exists {spec.serving_table}")
        publisher_client.command(f"drop table if exists {spec.staging_table}")

    assert cli.main(["rebuild", "--all"]) == 0

    for spec in MARTS.values():
        assert assert_reconciled(spec, trino_executor, publisher_client) > 0


def test_superset_reader_is_read_only(
    publisher_client: ClickHouseConnectClient,
) -> None:
    reader = ClickHouseConnectClient(ClickHouseConfig.reader_from_env())
    try:
        # Positive control: the BI account reads the serving mart...
        count_row = reader.query(f"select count() from {MART_DAILY_SALES.serving_table}")[0]
        assert isinstance(count_row[0], int)

        # ...and is denied any write (guide §26.4 — no BI write permissions).
        values = "(20260101, '2026-01-01', 'reader-probe', 'emea', 0, 0, 0, 0)"
        with pytest.raises(ClickHouseError):
            reader.command(f"insert into {MART_DAILY_SALES.serving_table} values {values}")
        assert reader.query(f"select count() from {MART_DAILY_SALES.serving_table}")[0] == count_row
    finally:
        reader.close()


def test_serving_tables_survived_reader_probe(
    publisher_client: ClickHouseConnectClient,
    trino_executor: DbapiTrinoExecutor,
) -> None:
    # Post-probe sanity: the reader's denied INSERT left the mart intact.
    assert assert_reconciled(MART_DAILY_SALES, trino_executor, publisher_client) > 0
