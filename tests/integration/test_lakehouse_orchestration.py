"""Live integration: the dataset-triggered lakehouse pair at the runner level.

Covers the exact code paths of the ``load_bronze`` -> ``transform_lakehouse``
DAG tasks (Phase 5 slice 3): ``run_bronze_load()`` watermark-sweeps every
registered source into Iceberg Bronze and ``run_dbt_build()`` runs the full
dbt build (staging -> intermediate -> core -> marts plus tests) over the
resulting world. Dataset wiring itself is asserted by the DAG structure
tests (``airflow/tests/test_dags.py``); scheduler-level dataset triggering
is manual verification (``make airflow-up``, Datasets view).

While this test runs it owns Bronze (setup wipes every partition; teardown
removes exactly what the sweep loaded). Re-run ``make bronze-load`` to
restore local Bronze data afterwards.
"""

from collections.abc import Generator
from pathlib import Path
from typing import cast

import pytest
from include.runners import run_bronze_load, run_dbt_build

from integration.lakehouse_seed import (
    DAY_1,
    DAY_2,
    purge_test_raw,
    reset_bronze,
    seed_day_1,
    seed_day_2,
    trino_scalar,
)
from omni_retail.ingestion.common.storage import BotoObjectStorage

REPO_ROOT = Path(__file__).resolve().parents[2]

#: Registered Bronze sources: 7 PostgreSQL snapshots + 3 API feeds.
REGISTERED_SOURCES = 10

#: Allowed dbt build statuses; everything else (error/fail/skip) fails the test.
HEALTHY_DBT_STATUSES = {"success", "pass"}

#: Seeded (source, day) partitions: DAY_1 is the full snapshot day, DAY_2 the
#: changed day (one mutated customer, one re-snapshotted order).
EXPECTED_SEEDED_PARTITIONS = {
    "orders": {DAY_1: 2, DAY_2: 1},
    "fx_rates": {DAY_1: 2, DAY_2: 1},
}


@pytest.fixture()
def orchestrated_world(live_storage: BotoObjectStorage) -> Generator[None, None, None]:
    """Seed the two-day raw world over an emptied Bronze; clean up after."""
    reset_bronze()
    purge_test_raw(live_storage)
    seed_day_1(live_storage)
    seed_day_2(live_storage)
    yield
    # Teardown follows the clean_bronze pattern: Polaris denies DROP to the
    # bootstrap principal, so partitions are DELETEd instead. Setup wiped
    # every partition, so reset_bronze() removes exactly what the sweep
    # loaded — the seeded days plus any other archive dates it picked up.
    reset_bronze()
    purge_test_raw(live_storage)


def test_dataset_triggered_lakehouse_pipeline(
    orchestrated_world: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    load_summary = run_bronze_load()

    # Watermark sweep: every registered source loaded; the seeded world is
    # DAY_1 for all 10 sources plus DAY_2 for the 5 changed ones (customers,
    # orders, and the three API feeds) — 15 dates (>= because a dirty local
    # archive may hold more).
    assert load_summary["sources"] == REGISTERED_SOURCES
    assert load_summary["loaded_sources"] == REGISTERED_SOURCES
    assert cast(int, load_summary["dates"]) >= 15
    assert cast(int, load_summary["rows"]) > 0
    by_source = cast(dict[str, dict[str, object]], load_summary["by_source"])
    for source_key in ("orders", "fx-rates"):
        loaded_dates = cast(list[str], by_source[source_key]["dates"])
        assert {DAY_1.isoformat(), DAY_2.isoformat()} <= set(loaded_dates), source_key

    # Bronze partitions for both seeded days are exactly the seeded batches
    # (per-day loads are partition-scoped, so other archive dates cannot
    # leak into these counts).
    for table, per_day in EXPECTED_SEEDED_PARTITIONS.items():
        for day, expected_rows in per_day.items():
            where = f"where \"_batch_date\" = DATE '{day:%Y-%m-%d}'"
            assert trino_scalar(f"select count(*) from iceberg.bronze.{table} {where}") == (
                expected_rows
            ), (table, day)

    # Idempotency leg: a dataset re-trigger between ingestion batches is a
    # no-op — the Bronze watermark already covers every archived date.
    second_load = run_bronze_load()
    assert second_load["loaded_sources"] == 0
    assert second_load["dates"] == 0

    # Transform leg: the dbt_build task code path against the live stack.
    # dbt artifacts land in a per-run temp dir, so assertions use the
    # XCom-shaped summary plus Trino reads, never target/ files.
    monkeypatch.setenv("DBT_PROJECT_DIR", str(REPO_ROOT / "dbt"))
    build_summary = run_dbt_build()
    statuses = cast(dict[str, int], build_summary["status_counts"])
    assert cast(int, build_summary["result_count"]) > 0
    assert set(statuses) <= HEALTHY_DBT_STATUSES, statuses
    assert "success" in statuses, "models must have run, not only tests"

    # The Gold->mart chain produced business output for the seeded days:
    # DAY_1 has two realized groups (order 990001 across two categories,
    # region it-region) plus one refunded group (order 990002).
    day1_rows = trino_scalar(
        "select count(*) from iceberg.analytics.mart_daily_sales "
        f"where order_date = DATE '{DAY_1:%Y-%m-%d}'"
    )
    assert day1_rows == 3
