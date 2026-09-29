"""Live dbt build over a manifest-scoped raw world and disposable schemas."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

import os
import subprocess
from collections.abc import Generator
from pathlib import Path
from typing import Protocol

import pytest

from integration.lakehouse_seed import (
    DAY_1,
    DAY_2,
    isolate_seed_coordinates,
    seed_day_1,
    seed_day_2,
    trino_scalar,
)
from integration.namespace_ownership import ObjectMutationJournal, manifest_scoped_storage
from omni_retail.lakehouse.bronze.loader import (
    DbapiTrinoExecutor,
    TrinoConfig,
    load,
)
from omni_retail.lakehouse.bronze.specs import (
    all_sources,
    spec_by_source,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


class Namespace(Protocol):
    bronze: str
    silver: str
    gold: str
    analytics: str

    def dbt_env(self) -> dict[str, str]: ...


@pytest.fixture()
def seeded_world(
    object_journal: ObjectMutationJournal,
    lakehouse_namespace: Namespace,
) -> Generator[Namespace, None, None]:
    isolate_seed_coordinates(object_journal)
    manifests = (*seed_day_1(object_journal), *seed_day_2(object_journal))
    storage = manifest_scoped_storage(object_journal, manifests)
    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        for logical_date in (DAY_1, DAY_2):
            for source in all_sources():
                load(
                    storage,
                    executor,
                    spec_by_source(source),
                    logical_date=logical_date,
                    schema=lakehouse_namespace.bronze,
                )
    yield lakehouse_namespace


def test_full_dbt_build_with_kimball_semantics(seeded_world: Namespace) -> None:
    env = {
        **os.environ,
        "TRINO_HOST": os.environ.get("TRINO_HOST", "127.0.0.1"),
        **seeded_world.dbt_env(),
    }
    completed = subprocess.run(
        [
            "uv",
            "run",
            "dbt",
            "build",
            "--project-dir",
            "dbt",
            "--profiles-dir",
            "dbt",
        ],
        cwd=REPO_ROOT,
        check=False,
        timeout=900,
        env=env,
    )
    assert completed.returncode == 0, "dbt build (models + tests) must succeed"

    gold = seeded_world.gold
    silver = seeded_world.silver
    assert (
        trino_scalar(f"select count(*) from iceberg.{gold}.dim_customer where customer_id = 990001")
        == 2
    )
    assert (
        trino_scalar(
            f"select region from iceberg.{gold}.dim_customer "
            "where customer_id = 990001 and is_current"
        )
        == "it-region-2"
    )
    assert (
        trino_scalar(
            f"select valid_to from iceberg.{gold}.dim_customer "
            "where customer_id = 990001 and not is_current"
        )
        == DAY_1
    )
    assert (
        trino_scalar(f"select count(*) from iceberg.{gold}.dim_customer where customer_id = 990002")
        == 1
    )
    assert (
        trino_scalar(f"select customer_key from iceberg.{gold}.fact_orders where order_id = 990001")
        == "990001_2026-09-20"
    )
    assert (
        trino_scalar(
            f"select count(*) from iceberg.{gold}.fact_order_items "
            "where order_id in (990001, 990002)"
        )
        == 3
    )
    assert trino_scalar(f"select count(*) from iceberg.{gold}.fact_payments") == 2
    assert trino_scalar(f"select count(*) from iceberg.{gold}.fact_shipments") == 1
    assert trino_scalar(f"select count(*) from iceberg.{gold}.dim_campaign") == 1
    assert (
        trino_scalar(
            f"select category_name from iceberg.{gold}.dim_product where product_id = 910002"
        )
        == "it-root-cat"
    )
    assert (
        trino_scalar(
            f"select count(*) from iceberg.{silver}.int_fx_rates_daily where currency = 'USD'"
        )
        == 2
    )
    assert (
        trino_scalar(
            f"select status from iceberg.{silver}.int_deliveries "
            "where delivery_id = 'DLV-IT-000001'"
        )
        == "delivered"
    )
    assert trino_scalar(f"select min(full_date) from iceberg.{gold}.dim_date") == DAY_1
    assert trino_scalar(f"select max(full_date) from iceberg.{gold}.dim_date") == DAY_2
