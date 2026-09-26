"""Live integration: Gold -> ClickHouse serving publication (Phase 6 slice 1).

Requires the core AND ``bi`` Compose profiles plus OMNI_INTEGRATION=1::

    make up && make bi-up && make integration

Asserts the Phase 6 acceptance criteria at the publication level
(ROADMAP Phase 6, ADR 0004):

- (a) publishing twice is idempotent — identical row count and checksum;
- (b) the mart is rebuildable: DROP -> rebuild -> identical snapshot;
- (d) Trino-side Gold row count == ClickHouse serving row count;
- (c) ``superset_reader`` can SELECT but cannot INSERT.

The Gold world is whatever the earlier integration files leave behind
(alphabetical order runs ``test_dbt_core_build`` and
``test_lakehouse_orchestration`` first; a standalone run needs one prior
``make dbt-build``). The module skips — with an actionable reason — when
ClickHouse or the Gold mart is not available.
"""

from collections.abc import Generator
from contextlib import closing

import pytest
from clickhouse_connect.driver.exceptions import Error as ClickHouseError
from trino.exceptions import Error as TrinoError

from integration.lakehouse_seed import trino_scalar
from omni_retail.lakehouse.bronze.loader import DbapiTrinoExecutor, TrinoConfig
from omni_retail.serving.clickhouse.client import ClickHouseConnectClient
from omni_retail.serving.clickhouse.config import ClickHouseConfig
from omni_retail.serving.clickhouse.publisher import publish, rebuild
from omni_retail.serving.clickhouse.specs import MART_DAILY_SALES, MartSpec

MART = MART_DAILY_SALES


def checksum_sql(spec: MartSpec) -> str:
    """Row count + deterministic content hash over every mart column."""
    hashed = ", ".join(f"toString({name})" for name in spec.column_names)
    return f"select count(), sum(cityHash64({hashed})) from {spec.serving_table}"


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
def gold_mart_row_count() -> int:
    """Guard: Gold mart must exist and be non-empty (see module docstring)."""
    try:
        count = trino_scalar(f"select count(*) from iceberg.analytics.{MART.name}")
    except TrinoError:
        pytest.skip(
            "Gold marts missing — run `make dbt-build` (or the full make integration) first"
        )
    assert isinstance(count, int)
    assert count > 0, "Gold mart_daily_sales is empty; seed Gold before serving tests"
    return count


def test_publication_cycle_idempotent_and_rebuildable(
    publisher_client: ClickHouseConnectClient, gold_mart_row_count: int
) -> None:
    with closing(DbapiTrinoExecutor(TrinoConfig.from_env())) as trino_executor:
        # (a) Two consecutive publishes over the same Gold state produce an
        # identical serving table: no duplicates, no drift.
        first = publish(trino_executor, publisher_client, MART)
        assert first.mode == "publish"
        assert first.row_count == gold_mart_row_count, "(d) row parity: Trino vs first publish"
        first_checksum = publisher_client.query(checksum_sql(MART))[0]
        second = publish(trino_executor, publisher_client, MART)
        assert second.row_count == first.row_count
        assert publisher_client.query(checksum_sql(MART))[0] == first_checksum

        # (b) Rebuild after DROP: recreating the DDL and republishing the
        # same Gold snapshot restores the identical serving table.
        publisher_client.command(f"drop table {MART.serving_table}")
        rebuilt = rebuild(trino_executor, publisher_client, MART)
        assert rebuilt.mode == "rebuild"
        assert rebuilt.row_count == first.row_count
        assert publisher_client.query(checksum_sql(MART))[0] == first_checksum


def test_superset_reader_is_read_only(
    publisher_client: ClickHouseConnectClient,
) -> None:
    reader = ClickHouseConnectClient(ClickHouseConfig.reader_from_env())
    try:
        # Positive control: the BI account reads the serving mart...
        count_row = reader.query(f"select count() from {MART.serving_table}")[0]
        assert isinstance(count_row[0], int)

        # ...and is denied any write (guide §26.4 — no BI write permissions).
        values = "(20260101, '2026-01-01', 'reader-probe', 'emea', 0, 0, 0, 0)"
        with pytest.raises(ClickHouseError):
            reader.command(f"insert into {MART.serving_table} values {values}")
        assert reader.query(f"select count() from {MART.serving_table}")[0] == count_row
    finally:
        reader.close()
