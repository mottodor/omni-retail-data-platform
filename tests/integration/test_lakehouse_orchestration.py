"""Live dataset-triggered runner paths with scoped raw data and disposable schemas."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

import pytest
from include import runners
from include.runners import DbtBuildError, run_bronze_load, run_dbt_build

from integration.lakehouse_seed import (
    DAY_1,
    DAY_2,
    isolate_seed_coordinates,
    seed_day_1,
    seed_day_2,
    trino_scalar,
)
from integration.namespace_ownership import (
    ObjectMutationJournal,
    ScopedObjectStorage,
    manifest_scoped_storage,
)
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    postgres_snapshot_key,
    postgres_watermark_key,
)
from omni_retail.ingestion.common.storage import BotoObjectStorage

REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTERED_SOURCES = 10
HEALTHY_DBT_STATUSES = {"success", "pass"}
EXPECTED_SEEDED_PARTITIONS = {
    "orders": {DAY_1: 2, DAY_2: 1},
    "fx_rates": {DAY_1: 2, DAY_2: 1},
}


class Namespace(Protocol):
    bronze: str
    analytics: str

    def dbt_env(self) -> dict[str, str]: ...


@dataclass(frozen=True)
class OrchestratedWorld:
    storage: ScopedObjectStorage
    namespace: Namespace


@pytest.fixture()
def orchestrated_world(
    object_journal: ObjectMutationJournal,
    lakehouse_namespace: Namespace,
) -> OrchestratedWorld:
    isolate_seed_coordinates(object_journal)
    manifests = (*seed_day_1(object_journal), *seed_day_2(object_journal))
    return OrchestratedWorld(
        storage=manifest_scoped_storage(object_journal, manifests),
        namespace=lakehouse_namespace,
    )


def test_dataset_triggered_lakehouse_pipeline(
    orchestrated_world: OrchestratedWorld,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    namespace = orchestrated_world.namespace
    load_summary = run_bronze_load(
        storage=orchestrated_world.storage,
        schema=namespace.bronze,
    )

    assert load_summary["sources"] == REGISTERED_SOURCES
    assert load_summary["loaded_sources"] == REGISTERED_SOURCES
    assert load_summary["dates"] == 15
    assert cast(int, load_summary["rows"]) > 0
    by_source = cast(dict[str, dict[str, object]], load_summary["by_source"])
    for source_key in ("orders", "fx-rates"):
        loaded_dates = cast(list[str], by_source[source_key]["dates"])
        assert loaded_dates == [DAY_1.isoformat(), DAY_2.isoformat()]

    for table, per_day in EXPECTED_SEEDED_PARTITIONS.items():
        for day, expected_rows in per_day.items():
            where = f"where \"_batch_date\" = DATE '{day:%Y-%m-%d}'"
            assert (
                trino_scalar(f"select count(*) from iceberg.{namespace.bronze}.{table} {where}")
                == expected_rows
            )

    second_load = run_bronze_load(
        storage=orchestrated_world.storage,
        schema=namespace.bronze,
    )
    assert second_load["loaded_sources"] == 0
    assert second_load["dates"] == 0

    monkeypatch.setenv("DBT_PROJECT_DIR", str(REPO_ROOT / "dbt"))
    for name, value in namespace.dbt_env().items():
        monkeypatch.setenv(name, value)
    build_summary = run_dbt_build()
    statuses = cast(dict[str, int], build_summary["status_counts"])
    assert cast(int, build_summary["result_count"]) > 0
    assert set(statuses) <= HEALTHY_DBT_STATUSES, statuses
    assert "success" in statuses, "models must have run, not only tests"

    day1_rows = trino_scalar(
        f"select count(*) from iceberg.{namespace.analytics}.mart_daily_sales "
        f"where order_date = DATE '{DAY_1:%Y-%m-%d}'"
    )
    assert day1_rows == 3
    assert namespace.bronze != "bronze"
    assert namespace.analytics != "analytics"


def test_transform_failure_restores_archive_and_watermark(
    live_storage: BotoObjectStorage,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failing transform cannot strand borrowed raw or watermark bytes."""
    refs = (
        (BUCKET_ARCHIVE, postgres_snapshot_key("orders", DAY_1)),
        (BUCKET_ARCHIVE, postgres_watermark_key("customers")),
    )
    before = {
        ref: live_storage.get_object(*ref) if live_storage.object_exists(*ref) else None
        for ref in refs
    }

    def failed_dbt(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(args=[], returncode=1, stdout="failed", stderr="")

    monkeypatch.setattr("include.runners.subprocess.run", failed_dbt)
    with pytest.raises(DbtBuildError), ObjectMutationJournal(live_storage) as journal:
        journal.lease_keys(refs)
        for bucket, key in refs:
            journal.put_object(bucket, key, b"temporary-test-bytes")
        runners.run_dbt_build()

    after = {
        ref: live_storage.get_object(*ref) if live_storage.object_exists(*ref) else None
        for ref in refs
    }
    assert after == before
