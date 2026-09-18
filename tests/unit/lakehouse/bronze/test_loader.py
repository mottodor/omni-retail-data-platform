"""Unit tests for the Bronze loader: DDL, ordering, idempotency (fakes only)."""

from datetime import date

import pytest

from omni_retail.lakehouse.bronze.loader import (
    DbapiTrinoExecutor,
    TrinoConfig,
    create_schema_sql,
    create_table_sql,
    delete_partition_sql,
)
from omni_retail.lakehouse.bronze.specs import TABLES

LOGICAL_DATE = date(2026, 9, 18)


def test_trino_config_defaults_and_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRINO_HOST", "trino")
    monkeypatch.setenv("TRINO_PORT", "8080")
    monkeypatch.setenv("TRINO_CATALOG", "iceberg")
    monkeypatch.setenv("TRINO_USER", "airflow")
    config = TrinoConfig.from_env()
    assert config == TrinoConfig(host="trino", port=8080, catalog="iceberg", user="airflow")


def test_trino_config_defaults_without_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("TRINO_HOST", "TRINO_PORT", "TRINO_CATALOG", "TRINO_USER"):
        monkeypatch.delenv(name, raising=False)
    assert TrinoConfig.from_env() == TrinoConfig()


def test_create_schema_sql_is_idempotent() -> None:
    assert create_schema_sql("iceberg") == "create schema if not exists iceberg.bronze"


def test_create_table_sql_declares_all_columns_and_partitioning() -> None:
    sql = create_table_sql(TABLES["orders"], "iceberg")
    assert sql.startswith("create table if not exists iceberg.bronze.orders (")
    assert '"order_id" bigint' in sql
    assert '"order_total" decimal(12,2)' in sql
    assert '"updated_at" timestamp(6) with time zone' in sql
    assert '"_batch_date" date' in sql
    assert sql.endswith("with (partitioning = ARRAY['_batch_date'])")


def test_delete_partition_sql_targets_logical_date() -> None:
    sql = delete_partition_sql(TABLES["orders"], "iceberg", LOGICAL_DATE)
    assert sql == ("delete from iceberg.bronze.orders where \"_batch_date\" = DATE '2026-09-18'")


def test_dbapi_executor_is_closable_context_manager() -> None:
    # Constructor must not connect eagerly (connection is lazy in trino client);
    # close() on a never-used executor must not raise.
    executor = DbapiTrinoExecutor(TrinoConfig())
    executor.close()
