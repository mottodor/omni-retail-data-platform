"""Unit tests for the declarative Bronze table specs."""

from datetime import date

import pytest

from omni_retail.ingestion.postgres_snapshot.tables import TABLES as SNAPSHOT_TABLES
from omni_retail.lakehouse.bronze.specs import (
    SERVICE_COLUMNS,
    TABLES,
    all_sources,
    spec_by_source,
)

LOGICAL_DATE = date(2026, 9, 18)


def test_all_ten_tables_registered() -> None:
    assert set(TABLES) == {
        "orders",
        "order_items",
        "customers",
        "products",
        "categories",
        "payments",
        "shipments",
        "fx_rates",
        "campaigns",
        "deliveries",
    }


def test_oltp_specs_mirror_snapshot_column_names() -> None:
    for name, snapshot in SNAPSHOT_TABLES.items():
        bronze = TABLES[name]
        assert [column.name for column in bronze.columns] == [
            column.name for column in snapshot.columns
        ], f"bronze spec for {name} drifted from the snapshot spec"


def test_oltp_source_names_match_snapshot_registry() -> None:
    for name, snapshot in SNAPSHOT_TABLES.items():
        assert TABLES[name].source_name == snapshot.source_name


def test_batch_ids_match_raw_producers() -> None:
    assert TABLES["orders"].batch_id(LOGICAL_DATE) == "postgres-orders-20260918"
    assert TABLES["fx_rates"].batch_id(LOGICAL_DATE) == "fx-rates-20260918"
    assert TABLES["campaigns"].batch_id(LOGICAL_DATE) == "marketing-campaigns-20260918"


def test_object_prefixes_match_raw_layout() -> None:
    assert TABLES["orders"].object_prefix(LOGICAL_DATE) == "postgres/orders/2026/09/18/"
    assert TABLES["fx_rates"].object_prefix(LOGICAL_DATE) == "api/fx-rates/20260918/"


def test_root_prefixes_address_every_date_of_a_source() -> None:
    assert TABLES["orders"].root_prefix == "postgres/orders/"
    assert TABLES["order_items"].root_prefix == "postgres/order_items/"
    assert TABLES["fx_rates"].root_prefix == "api/fx-rates/"


def test_logical_date_from_key_parses_both_layouts() -> None:
    assert (
        TABLES["orders"].logical_date_from_key("postgres/orders/2026/09/18/data.parquet")
        == LOGICAL_DATE
    )
    assert (
        TABLES["fx_rates"].logical_date_from_key("api/fx-rates/20260918/page_0001.json")
        == LOGICAL_DATE
    )


def test_logical_date_from_key_ignores_foreign_and_malformed_keys() -> None:
    spec = TABLES["orders"]
    assert spec.logical_date_from_key("postgres/orders/notes.txt") is None
    assert spec.logical_date_from_key("postgres/orders/2026/13/45/data.parquet") is None
    assert spec.logical_date_from_key("postgres/shipments/2026/09/18/data.parquet") is None
    assert TABLES["fx_rates"].logical_date_from_key("api/fx-rates/20261345/page_0001.json") is None


def test_service_columns_are_last_and_partition_on_batch_date() -> None:
    assert [column.name for column in SERVICE_COLUMNS] == [
        "_batch_id",
        "_batch_date",
        "_source_object",
        "_source_object_row_position",
        "_ingested_at",
    ]
    assert SERVICE_COLUMNS[1].trino_type == "date"
    assert TABLES["orders"].all_columns[-5:] == SERVICE_COLUMNS


def test_api_specs_declare_envelope_fields() -> None:
    assert TABLES["fx_rates"].envelope_field == "rates"
    assert TABLES["campaigns"].envelope_field == "campaigns"
    assert TABLES["deliveries"].envelope_field == "deliveries"
    assert TABLES["orders"].envelope_field is None


def test_spec_by_source_resolves_cli_keys() -> None:
    assert spec_by_source("orders").name == "orders"
    assert spec_by_source("fx-rates").name == "fx_rates"
    assert spec_by_source("marketing-campaigns").name == "campaigns"
    assert spec_by_source("deliveries").name == "deliveries"
    assert len(all_sources()) == 10


def test_spec_by_source_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="unknown bronze source"):
        spec_by_source("nope")
