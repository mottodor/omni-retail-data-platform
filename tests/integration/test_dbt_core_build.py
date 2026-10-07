"""Live dbt build over a manifest-scoped raw world and disposable schemas."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

from collections.abc import Generator
from pathlib import Path
from typing import Protocol

import pytest

from integration.dbt_diagnostics import run_dbt_build
from integration.lakehouse_seed import (
    DAY_1,
    DAY_2,
    isolate_seed_coordinates,
    seed_cdc_events,
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
from omni_retail.streaming.cdc.model import event_identity

CUSTOMER_TOPIC = "omni.oltp.public.customers"


class Namespace(Protocol):
    bronze: str
    silver: str
    gold: str
    analytics: str

    def dbt_env(self) -> dict[str, str]: ...

    def all_schemas(self) -> tuple[str, ...]: ...


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
        seed_cdc_events(executor, schema=lakehouse_namespace.bronze)
    yield lakehouse_namespace


def query_rows(sql: str) -> list[tuple[object, ...]]:
    with DbapiTrinoExecutor(TrinoConfig.from_env()) as executor:
        return executor.fetch(sql)


def cdc_output_snapshot(namespace: Namespace) -> dict[str, list[tuple[object, ...]]]:
    """Capture every CDC-cutover Gold/mart row for repeat-build comparison."""
    gold = namespace.gold
    analytics = namespace.analytics
    queries = {
        "dim_customer": (
            "select customer_key, customer_id, email, first_name, last_name, region, city, "
            "customer_status, segment, registered_at, created_at, updated_at, "
            "valid_from_lsn, valid_to_lsn, is_snapshot_baseline, is_current, "
            "opened_at_source_timestamp, closed_at_source_timestamp, event_id, "
            "source_tx_id, kafka_offset "
            f"from iceberg.{gold}.dim_customer order by customer_id, "
            "is_snapshot_baseline desc, valid_from_lsn, customer_key"
        ),
        "fact_orders": f"select * from iceberg.{gold}.fact_orders order by order_id",
        "fact_order_items": (
            f"select * from iceberg.{gold}.fact_order_items order by order_item_id"
        ),
        "fact_payments": f"select * from iceberg.{gold}.fact_payments order by payment_id",
        "fact_shipments": f"select * from iceberg.{gold}.fact_shipments order by shipment_id",
        "mart_daily_sales": (
            f"select * from iceberg.{analytics}.mart_daily_sales "
            "order by order_date, category_name, region"
        ),
        "mart_customer_ltv": (
            f"select * from iceberg.{analytics}.mart_customer_ltv order by customer_id"
        ),
    }
    return {name: query_rows(sql) for name, sql in queries.items()}


def test_full_dbt_build_with_cdc_backed_gold_semantics(
    seeded_world: Namespace, tmp_path: Path
) -> None:
    run_dbt_build(
        schema_env=seeded_world.dbt_env(),
        schema_names=seeded_world.all_schemas(),
        run_dir=tmp_path / "dbt-first",
        label="first",
    )

    gold = seeded_world.gold
    silver = seeded_world.silver
    analytics = seeded_world.analytics

    # Snapshot baseline + stream update use half-open LSN validity. A populated
    # LSN on the r event remains a baseline and cannot outrank the update.
    assert (
        trino_scalar(f"select count(*) from iceberg.{gold}.dim_customer where customer_id = 990001")
        == 2
    )
    assert (
        trino_scalar(
            f"select valid_to_lsn from iceberg.{gold}.dim_customer "
            "where customer_id = 990001 and is_snapshot_baseline"
        )
        == 205
    )
    assert (
        trino_scalar(
            f"select region from iceberg.{gold}.dim_customer "
            "where customer_id = 990001 and is_current"
        )
        == "it-region-2"
    )

    # Same-LSN customer events collapse to the final topic offset. A later
    # customer change becomes current, but the already-created order keeps the
    # version selected at its lifecycle start.
    assert (
        trino_scalar(
            f"select count(*) from iceberg.{gold}.dim_customer where customer_id = 9910001"
        )
        == 3
    )
    assert (
        trino_scalar(
            f"select segment from iceberg.{gold}.dim_customer "
            "where customer_id = 9910001 and valid_from_lsn = 200"
        )
        == "vip"
    )
    assert (
        trino_scalar(
            f"select region from iceberg.{gold}.dim_customer "
            "where customer_id = 9910001 and is_current"
        )
        == "cdc-later-version"
    )

    # Delete closes history without an attribute-less dimension row; recreate
    # starts a new lifecycle and restores exactly one current version.
    assert (
        trino_scalar(
            f"select count(*) from iceberg.{gold}.dim_customer "
            "where customer_id = 9910002 and is_current"
        )
        == 0
    )
    assert (
        trino_scalar(
            f"select valid_to_lsn from iceberg.{gold}.dim_customer where customer_id = 9910002"
        )
        == 201
    )
    assert (
        trino_scalar(
            f"select count(*) from iceberg.{gold}.dim_customer where customer_id = 9910003"
        )
        == 2
    )
    assert (
        trino_scalar(
            f"select region from iceberg.{gold}.dim_customer "
            "where customer_id = 9910003 and is_current"
        )
        == "cdc-recreated"
    )

    baseline_key = f"990001_{event_identity(CUSTOMER_TOPIC, 0, 8)}"
    lifecycle_key = f"9910001_{event_identity(CUSTOMER_TOPIC, 0, 3)}"
    assert (
        trino_scalar(f"select customer_key from iceberg.{gold}.fact_orders where order_id = 990001")
        == baseline_key
    )
    assert (
        trino_scalar(
            f"select customer_key from iceberg.{gold}.fact_orders where order_id = 9920001"
        )
        == lifecycle_key
    )
    assert (
        trino_scalar(
            f"select order_status from iceberg.{gold}.fact_orders where order_id = 9920001"
        )
        == "paid"
    )

    # The deleted snapshot order is gone. Its still-present snapshot line and
    # shipment are parent-gated, while the live CDC-only order is allowed to
    # exist before its child snapshot arrives.
    assert (
        trino_scalar(f"select count(*) from iceberg.{gold}.fact_orders where order_id = 990002")
        == 0
    )
    assert (
        trino_scalar(
            f"select count(*) from iceberg.{gold}.fact_order_items where order_id = 990002"
        )
        == 0
    )
    assert (
        trino_scalar(f"select count(*) from iceberg.{gold}.fact_shipments where order_id = 990002")
        == 0
    )
    assert (
        trino_scalar(
            f"select count(*) from iceberg.{gold}.fact_order_items where order_id = 9920001"
        )
        == 0
    )
    assert trino_scalar(f"select count(*) from iceberg.{gold}.fact_order_items") == 2
    assert trino_scalar(f"select count(*) from iceberg.{gold}.fact_shipments") == 1
    assert trino_scalar(f"select count(*) from iceberg.{gold}.fact_payments") == 2

    # Marts follow the live order set: stale lines/customer activity disappear,
    # while the CDC-only order contributes to LTV despite child snapshot lag.
    assert trino_scalar(f"select count(*) from iceberg.{analytics}.mart_daily_sales") == 2
    assert (
        trino_scalar(
            f"select count(*) from iceberg.{analytics}.mart_customer_ltv where customer_id = 990002"
        )
        == 0
    )
    assert (
        trino_scalar(
            f"select count(*) from iceberg.{analytics}.mart_customer_ltv "
            "where customer_id = 9910001"
        )
        == 1
    )

    assert trino_scalar(f"select min(full_date) from iceberg.{gold}.dim_date") == DAY_1
    assert trino_scalar(f"select max(full_date) from iceberg.{gold}.dim_date") == DAY_1
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
    assert (
        trino_scalar(f"select count(*) from iceberg.{seeded_world.bronze}.postgres_cdc_events")
        == 26
    )

    first_snapshot = cdc_output_snapshot(seeded_world)
    run_dbt_build(
        schema_env=seeded_world.dbt_env(),
        schema_names=seeded_world.all_schemas(),
        run_dir=tmp_path / "dbt-second",
        label="second",
    )
    assert cdc_output_snapshot(seeded_world) == first_snapshot
