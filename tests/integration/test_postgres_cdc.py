"""Live PostgreSQL -> Debezium -> Kafka -> Iceberg restart/replay scenario."""

# pyright: reportAttributeAccessIssue=false, reportMissingImports=false, reportMissingTypeStubs=false

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

import psycopg
import pytest
from confluent_kafka import Consumer, TopicPartition

from omni_retail.ingestion.postgres_snapshot.config import PostgresSourceConfig
from omni_retail.lakehouse.trino import DbapiTrinoExecutor, TrinoConfig
from omni_retail.streaming.cdc.consumer import CdcBatchWriter
from omni_retail.streaming.cdc.model import KafkaRecord, parse_debezium_record

TABLE = "iceberg.bronze.postgres_cdc_events"
SCHEMA_EVOLUTION_COLUMN = "cdc_schema_evolution_note"
SCHEMA_EVOLUTION_LOCK = 8_610_008
REPO_ROOT = Path(__file__).resolve().parents[2]
CDC_DBT_MODELS = (
    "stg_cdc_customers",
    "stg_cdc_orders",
    "stg_cdc_payments",
    "int_cdc_customers_current",
    "int_cdc_orders_current",
    "int_cdc_payments_current",
    "int_cdc_customer_versions",
    "dim_customer",
    "fact_orders",
    "fact_payments",
)


class Namespace(Protocol):
    silver: str
    gold: str

    def dbt_env(self) -> dict[str, str]: ...


def as_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise AssertionError(f"expected integer query value, got {value!r}")
    return value


def as_str(value: object) -> str:
    if not isinstance(value, str):
        raise AssertionError(f"expected string query value, got {value!r}")
    return value


def wait_until(description: str, predicate: Callable[[], bool], timeout: float = 120) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except Exception as exc:  # readiness probes retain the last useful failure
            last_error = exc
        time.sleep(1)
    suffix = f": {last_error}" if last_error is not None else ""
    raise AssertionError(f"timed out waiting for {description}{suffix}")


def run_cdc_dbt(namespace: Namespace, *, target_path: Path) -> None:
    env = {
        **os.environ,
        **namespace.dbt_env(),
        "DBT_BRONZE_SCHEMA": "bronze",
        "TRINO_HOST": os.environ.get("TRINO_HOST", "127.0.0.1"),
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
            "--target-path",
            str(target_path),
            "--select",
            *CDC_DBT_MODELS,
        ],
        cwd=REPO_ROOT,
        check=False,
        timeout=300,
        env=env,
    )
    assert completed.returncode == 0, "CDC Silver and delete-aware core graph must build"


def connector_running() -> bool:
    port = os.environ.get("KAFKA_CONNECT_PORT", "8083")
    request = urllib.request.Request(f"http://127.0.0.1:{port}/connectors/omni-postgres-cdc/status")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=2) as response:  # noqa: S310 -- loopback only
            status = json.loads(response.read())
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return False
    connector = status.get("connector", {})
    tasks = status.get("tasks", [])
    return (
        connector.get("state") == "RUNNING"
        and bool(tasks)
        and all(task.get("state") == "RUNNING" for task in tasks)
    )


def owned_predicate(customer_ids: list[int], order_id: int, payment_id: int) -> str:
    customers = ",".join(str(value) for value in customer_ids)
    return (
        "((source_table = 'customers' and "
        f"cast(json_extract_scalar(key_json, '$.customer_id') as bigint) in ({customers})) "
        "or (source_table = 'orders' and "
        f"cast(json_extract_scalar(key_json, '$.order_id') as bigint) = {order_id}) "
        "or (source_table = 'payments' and "
        f"cast(json_extract_scalar(key_json, '$.payment_id') as bigint) = {payment_id}))"
    )


def count_owned(executor: DbapiTrinoExecutor, predicate: str) -> int:
    rows = executor.fetch(f"select count(*) from {TABLE} where {predicate}")  # nosec B608
    return as_int(rows[0][0])


def count_distinct_owned(executor: DbapiTrinoExecutor, predicate: str) -> tuple[int, int]:
    rows = executor.fetch(  # nosec B608 -- predicate contains only DB-returned integer IDs
        f"select count(*), count(distinct event_id) from {TABLE} where {predicate}"
    )
    return as_int(rows[0][0]), as_int(rows[0][1])


def test_postgres_cdc_additive_schema_evolution_without_restart() -> None:
    if not connector_running():
        pytest.skip("streaming profile is not healthy; run `make streaming-up`")

    token = uuid.uuid4().hex[:12]
    marker = f"cdc-schema-it-{token}"
    customer_id: int | None = None
    expected_events = 0
    source_config = PostgresSourceConfig.from_env()
    trino_config = TrinoConfig.from_env()

    with (
        psycopg.connect(source_config.conninfo()) as source,
        DbapiTrinoExecutor(trino_config) as executor,
    ):
        source.execute("select pg_advisory_lock(%s)", (SCHEMA_EVOLUTION_LOCK,))
        try:
            # Repair only the reserved test column if a killed prior run left it behind.
            source.execute(
                f"alter table public.customers drop column if exists {SCHEMA_EVOLUTION_COLUMN}"
            )
            source.commit()

            # Use a high UUID-derived bigint so deleted fixture IDs are never reused
            # against immutable Bronze history from an interrupted prior run.
            customer_id = 8_000_000_000_000_000_000 + int(token, 16)
            source.execute(
                "insert into customers "
                "(customer_id, email, first_name, last_name, region, city, status, segment, "
                "registered_at) values (%s, %s, 'CDC', 'Schema', 'it-region', 'it-city', "
                "'active', 'standard', timestamptz '2026-10-01 12:00:00+00')",
                (customer_id, f"{marker}@example.test"),
            )
            source.commit()
            expected_events += 1
            predicate = (
                "source_table = 'customers' and "
                "cast(json_extract_scalar(key_json, '$.customer_id') as bigint) = "
                f"{customer_id}"
            )
            wait_until(
                "pre-DDL customer event",
                lambda: count_owned(executor, predicate) >= expected_events,
            )
            pre_ddl = executor.fetch(  # nosec B608 -- customer_id is DB-returned integer
                f"select after_json from {TABLE} where {predicate} order by kafka_offset limit 1"
            )
            assert pre_ddl and pre_ddl[0][0] is not None
            assert SCHEMA_EVOLUTION_COLUMN not in json.loads(as_str(pre_ddl[0][0]))

            source.execute(
                f"alter table public.customers add column {SCHEMA_EVOLUTION_COLUMN} text"
            )
            source.execute(
                f"update public.customers set {SCHEMA_EVOLUTION_COLUMN} = %s, "
                "updated_at = now() where customer_id = %s",
                (marker, customer_id),
            )
            source.commit()
            expected_events += 1

            def additive_event_arrived() -> bool:
                rows = executor.fetch(  # nosec B608 -- customer_id is DB-returned integer
                    f"select after_json from {TABLE} where {predicate} and after_json is not null"
                )
                return any(
                    json.loads(as_str(row_value[0])).get(SCHEMA_EVOLUTION_COLUMN) == marker
                    for row_value in rows
                )

            wait_until("post-DDL additive customer event", additive_event_arrived)
            total, distinct = count_distinct_owned(executor, predicate)
            assert total == distinct == expected_events
            slot = source.execute(
                "select active from pg_replication_slots where slot_name = 'omni_cdc_slot'"
            ).fetchone()
            assert slot == (True,)
            assert connector_running()
        finally:
            source.rollback()
            source.execute(
                f"alter table public.customers drop column if exists {SCHEMA_EVOLUTION_COLUMN}"
            )
            if customer_id is not None:
                deleted = source.execute(
                    "delete from public.customers where customer_id = %s", (customer_id,)
                ).rowcount
                if deleted:
                    expected_events += 1
            source.commit()
            if customer_id is not None:
                predicate = (
                    "source_table = 'customers' and "
                    "cast(json_extract_scalar(key_json, '$.customer_id') as bigint) = "
                    f"{customer_id}"
                )
                try:
                    wait_until(
                        "schema-evolution fixture cleanup event",
                        lambda: count_owned(executor, predicate) >= expected_events,
                        timeout=60,
                    )
                finally:
                    executor.execute(f"delete from {TABLE} where {predicate}")  # nosec B608
            source.execute("select pg_advisory_unlock(%s)", (SCHEMA_EVOLUTION_LOCK,))
            source.commit()


def test_postgres_cdc_create_update_delete_replay_and_restart(
    lakehouse_namespace: Namespace,
    tmp_path: Path,
) -> None:
    if not connector_running():
        pytest.skip("streaming profile is not healthy; run `make streaming-up`")

    token = uuid.uuid4().hex[:12]
    marker = f"cdc-it-{token}"
    customer_ids: list[int] = []
    order_id = -1
    payment_id = -1
    expected_events = 0
    source_config = PostgresSourceConfig.from_env()
    trino_config = TrinoConfig.from_env()

    with (
        psycopg.connect(source_config.conninfo()) as source,
        DbapiTrinoExecutor(trino_config) as executor,
    ):
        try:
            replication_user = os.environ.get("DEBEZIUM_POSTGRES_USER", "omni_debezium")
            role = source.execute(
                "select rolcanlogin, rolreplication from pg_roles where rolname = %s",
                (replication_user,),
            ).fetchone()
            assert role == (True, True)
            publication_tables = set(
                source.execute(
                    "select schemaname, tablename from pg_publication_tables "
                    "where pubname = 'omni_cdc_publication'"
                ).fetchall()
            )
            assert publication_tables == {
                ("public", "customers"),
                ("public", "orders"),
                ("public", "payments"),
            }
            slot = source.execute(
                "select plugin, database from pg_replication_slots "
                "where slot_name = 'omni_cdc_slot'"
            ).fetchone()
            assert slot == ("pgoutput", source_config.dbname)
            for table in ("customers", "orders", "payments"):
                privilege = source.execute(
                    "select has_table_privilege(%s, %s, 'SELECT')",
                    (replication_user, f"public.{table}"),
                ).fetchone()
                assert privilege == (True,)
            product_privilege = source.execute(
                "select has_table_privilege(%s, 'public.products', 'SELECT')",
                (replication_user,),
            ).fetchone()
            assert product_privilege == (False,)

            # Immutable Bronze history can outlive a failed test, so every run
            # owns never-reused bigint IDs rather than max(source_id) + N.
            suffix = int(token, 16)
            customer_ids.append(7_000_000_000_000_000_000 + suffix)
            order_id = 6_000_000_000_000_000_000 + suffix
            payment_id = 5_000_000_000_000_000_000 + suffix

            source.execute(
                "insert into customers "
                "(customer_id, email, first_name, last_name, region, city, status, segment, "
                "registered_at) values (%s, %s, 'CDC', 'Integration', 'it-region', 'it-city', "
                "'active', 'standard', timestamptz '2026-09-30 12:00:00+00')",
                (customer_ids[0], f"{marker}@example.test"),
            )
            source.execute(
                "insert into orders "
                "(order_id, customer_id, status, currency, shipping_cost, order_total) "
                "values (%s, %s, 'pending', 'USD', 0, 10.50)",
                (order_id, customer_ids[0]),
            )
            source.execute(
                "insert into payments "
                "(payment_id, order_id, method, status, amount, transaction_id) "
                "values (%s, %s, 'card', 'pending', 10.50, %s)",
                (payment_id, order_id, f"{marker}-tx"),
            )
            source.commit()
            expected_events += 3

            source.execute(
                "update customers set segment='premium', "
                "updated_at=timestamptz '2020-01-01 00:00:00+00' where customer_id=%s",
                (customer_ids[0],),
            )
            source.execute(
                "update orders set status='cancelled', "
                "updated_at=timestamptz '2020-01-01 00:00:00+00' where order_id=%s",
                (order_id,),
            )
            source.execute(
                "update payments set status='cancelled', "
                "updated_at=timestamptz '2020-01-01 00:00:00+00' where payment_id=%s",
                (payment_id,),
            )
            source.commit()
            expected_events += 3

            predicate = owned_predicate(customer_ids, order_id, payment_id)
            wait_until(
                "six create/update Bronze events",
                lambda: count_owned(executor, predicate) >= expected_events,
            )
            run_cdc_dbt(lakehouse_namespace, target_path=tmp_path / "dbt-target")
            silver = lakehouse_namespace.silver
            customer_state = executor.fetch(
                f"select segment, updated_at from iceberg.{silver}.int_cdc_customers_current "
                f"where customer_id = {customer_ids[0]}"  # nosec B608 -- DB-owned integer
            )
            assert len(customer_state) == 1
            assert as_str(customer_state[0][0]) == "premium"
            assert customer_state[0][1] == datetime(2020, 1, 1, tzinfo=UTC)
            order_state = executor.fetch(
                f"select status from iceberg.{silver}.int_cdc_orders_current "
                f"where order_id = {order_id}"  # nosec B608 -- DB-owned integer
            )
            assert len(order_state) == 1 and as_str(order_state[0][0]) == "cancelled"
            payment_state = executor.fetch(
                f"select status from iceberg.{silver}.int_cdc_payments_current "
                f"where payment_id = {payment_id}"  # nosec B608 -- DB-owned integer
            )
            assert len(payment_state) == 1 and as_str(payment_state[0][0]) == "cancelled"
            gold = lakehouse_namespace.gold
            gold_order = executor.fetch(
                f"select order_status, customer_key from iceberg.{gold}.fact_orders "
                f"where order_id = {order_id}"  # nosec B608 -- DB-owned integer
            )
            assert len(gold_order) == 1
            assert as_str(gold_order[0][0]) == "cancelled"
            assert as_str(gold_order[0][1]).startswith(f"{customer_ids[0]}_")
            gold_payment = executor.fetch(
                f"select payment_status from iceberg.{gold}.fact_payments "
                f"where payment_id = {payment_id}"  # nosec B608 -- DB-owned integer
            )
            assert len(gold_payment) == 1 and as_str(gold_payment[0][0]) == "cancelled"

            source.execute("delete from orders where order_id=%s", (order_id,))
            source.execute("delete from customers where customer_id=%s", (customer_ids[0],))
            source.commit()
            expected_events += 3

            predicate = owned_predicate(customer_ids, order_id, payment_id)
            wait_until(
                "nine c/u/d Bronze events",
                lambda: count_owned(executor, predicate) >= expected_events,
            )
            run_cdc_dbt(lakehouse_namespace, target_path=tmp_path / "dbt-target-deleted")
            gold = lakehouse_namespace.gold
            deleted_customer_count = executor.fetch(
                f"select count(*) from iceberg.{silver}.int_cdc_customers_current "
                f"where customer_id = {customer_ids[0]}"  # nosec B608 -- DB-owned integer
            )
            assert as_int(deleted_customer_count[0][0]) == 0
            deleted_order_count = executor.fetch(
                f"select count(*) from iceberg.{silver}.int_cdc_orders_current "
                f"where order_id = {order_id}"  # nosec B608 -- DB-owned integer
            )
            assert as_int(deleted_order_count[0][0]) == 0
            deleted_payment_count = executor.fetch(
                f"select count(*) from iceberg.{silver}.int_cdc_payments_current "
                f"where payment_id = {payment_id}"  # nosec B608 -- DB-owned integer
            )
            assert as_int(deleted_payment_count[0][0]) == 0
            deleted_dimension_current = executor.fetch(
                f"select count(*) from iceberg.{gold}.dim_customer "
                f"where customer_id = {customer_ids[0]} and is_current"  # nosec B608
            )
            assert as_int(deleted_dimension_current[0][0]) == 0
            deleted_fact_order = executor.fetch(
                f"select count(*) from iceberg.{gold}.fact_orders where order_id = {order_id}"  # nosec B608 -- DB-owned integer
            )
            assert as_int(deleted_fact_order[0][0]) == 0
            deleted_fact_payment = executor.fetch(
                f"select count(*) from iceberg.{gold}.fact_payments where payment_id = {payment_id}"  # nosec B608 -- DB-owned integer
            )
            assert as_int(deleted_fact_payment[0][0]) == 0
            rows = executor.fetch(  # nosec B608 -- predicate contains owned integer IDs only
                f"select source_table, operation, source_lsn from {TABLE} "
                f"where {predicate} order by kafka_topic, kafka_offset"
            )
            assert {str(row[0]) for row in rows} == {"customers", "orders", "payments"}
            assert {str(row[1]) for row in rows} >= {"c", "u", "d"}
            assert all(row[2] is not None for row in rows)
            assert (
                count_distinct_owned(executor, predicate)[0]
                == count_distinct_owned(executor, predicate)[1]
            )

            coordinate = executor.fetch(  # nosec B608 -- owned predicate
                f"select kafka_topic, kafka_partition, kafka_offset from {TABLE} "
                f"where {predicate} order by kafka_topic, kafka_offset limit 1"
            )[0]
            replay_consumer = Consumer(
                {
                    "bootstrap.servers": os.environ.get(
                        "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
                    ),
                    "group.id": f"omni-cdc-replay-it-{uuid.uuid4().hex}",
                    "enable.auto.commit": False,
                    "auto.offset.reset": "earliest",
                }
            )
            replay_consumer.assign(
                [
                    TopicPartition(
                        as_str(coordinate[0]), as_int(coordinate[1]), as_int(coordinate[2])
                    )
                ]
            )
            message = replay_consumer.poll(30)
            replay_consumer.close()
            assert message is not None and message.error() is None
            event = parse_debezium_record(
                cast(KafkaRecord, message),
                topic_tables={
                    "omni.oltp.public.customers": "customers",
                    "omni.oltp.public.orders": "orders",
                    "omni.oltp.public.payments": "payments",
                },
                ingested_at=datetime.now(UTC),
            )
            writer = CdcBatchWriter(
                executor, catalog=trino_config.catalog, schema="bronze", attempts=3
            )
            before_replay = count_owned(executor, predicate)
            writer.write([event])
            writer.write([event])
            assert count_owned(executor, predicate) == before_replay

            subprocess.run(
                [
                    "docker",
                    "compose",
                    "--profile",
                    "core",
                    "--profile",
                    "streaming",
                    "restart",
                    "kafka",
                    "debezium-connect",
                    "cdc-consumer",
                ],
                check=True,
                timeout=180,
            )
            wait_until("connector recovery after restart", connector_running, timeout=180)

            second_customer_id = customer_ids[0] + 1
            source.execute(
                "insert into customers "
                "(customer_id, email, first_name, last_name, region, city, status, segment, "
                "registered_at) values (%s, %s, 'CDC', 'Restart', 'it-region', 'it-city', "
                "'active', 'standard', timestamptz '2026-09-30 12:00:00+00')",
                (second_customer_id, f"{marker}-restart@example.test"),
            )
            customer_ids.append(second_customer_id)
            source.commit()
            expected_events += 1
            predicate = owned_predicate(customer_ids, order_id, payment_id)
            wait_until(
                "post-restart committed event",
                lambda: count_owned(executor, predicate) >= expected_events,
            )
            total, distinct = count_distinct_owned(executor, predicate)
            assert total == distinct
        finally:
            cleanup_events = 0
            if payment_id >= 0:
                cleanup_events += source.execute(
                    "delete from payments where payment_id=%s", (payment_id,)
                ).rowcount
            if order_id >= 0:
                cleanup_events += source.execute(
                    "delete from orders where order_id=%s", (order_id,)
                ).rowcount
            if customer_ids:
                cleanup_events += source.execute(
                    "delete from customers where customer_id = any(%s)", (customer_ids,)
                ).rowcount
            source.commit()
            expected_events += cleanup_events
            if order_id >= 0 and payment_id >= 0 and customer_ids:
                predicate = owned_predicate(customer_ids, order_id, payment_id)
                try:
                    if cleanup_events:
                        wait_until(
                            "CDC cleanup deletes",
                            lambda: count_owned(executor, predicate) >= expected_events,
                            timeout=60,
                        )
                finally:
                    executor.execute(f"delete from {TABLE} where {predicate}")  # nosec B608
