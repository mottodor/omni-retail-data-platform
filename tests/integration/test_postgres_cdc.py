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
from typing import cast

import psycopg
import pytest
from confluent_kafka import Consumer, TopicPartition

from omni_retail.ingestion.postgres_snapshot.config import PostgresSourceConfig
from omni_retail.lakehouse.trino import DbapiTrinoExecutor, TrinoConfig
from omni_retail.streaming.cdc.consumer import CdcBatchWriter
from omni_retail.streaming.cdc.model import KafkaRecord, parse_debezium_record

TABLE = "iceberg.bronze.postgres_cdc_events"


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


def test_postgres_cdc_create_update_delete_replay_and_restart() -> None:
    if not connector_running():
        pytest.skip("streaming profile is not healthy; run `make streaming-up`")

    marker = f"cdc-it-{uuid.uuid4().hex[:12]}"
    customer_ids: list[int] = []
    order_id = -1
    payment_id = -1
    completed_scenario = False
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

            reserved_ids = source.execute(
                "select "
                "(select coalesce(max(customer_id), 0) + 1000 from customers), "
                "(select coalesce(max(order_id), 0) + 1000 from orders), "
                "(select coalesce(max(payment_id), 0) + 1000 from payments)"
            ).fetchone()
            assert reserved_ids is not None
            customer_ids.append(int(reserved_ids[0]))
            order_id = int(reserved_ids[1])
            payment_id = int(reserved_ids[2])

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

            source.execute(
                "update customers set segment='premium', updated_at=now() where customer_id=%s",
                (customer_ids[0],),
            )
            source.execute(
                "update orders set status='cancelled', updated_at=now() where order_id=%s",
                (order_id,),
            )
            source.execute(
                "update payments set status='cancelled', updated_at=now() where payment_id=%s",
                (payment_id,),
            )
            source.commit()
            source.execute("delete from orders where order_id=%s", (order_id,))
            source.execute("delete from customers where customer_id=%s", (customer_ids[0],))
            source.commit()

            predicate = owned_predicate(customer_ids, order_id, payment_id)
            wait_until("nine c/u/d Bronze events", lambda: count_owned(executor, predicate) >= 9)
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
            predicate = owned_predicate(customer_ids, order_id, payment_id)
            wait_until(
                "post-restart committed event",
                lambda: count_owned(executor, predicate) >= 10,
            )
            total, distinct = count_distinct_owned(executor, predicate)
            assert total == distinct
            completed_scenario = True
        finally:
            if order_id >= 0:
                source.execute("delete from orders where order_id=%s", (order_id,))
            if customer_ids:
                source.execute("delete from customers where customer_id = any(%s)", (customer_ids,))
            source.commit()
            if order_id >= 0 and payment_id >= 0 and customer_ids:
                predicate = owned_predicate(customer_ids, order_id, payment_id)
                try:
                    if completed_scenario:
                        wait_until(
                            "CDC cleanup deletes",
                            lambda: count_owned(executor, predicate) >= 11,
                            timeout=60,
                        )
                finally:
                    executor.execute(f"delete from {TABLE} where {predicate}")  # nosec B608
