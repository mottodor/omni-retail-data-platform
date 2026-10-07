"""Command-line entry points for the PostgreSQL CDC consumer."""

# pyright: reportMissingImports=false

import argparse
import signal
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from threading import Event

from confluent_kafka import Consumer

from omni_retail.ingestion.common.logging import configure_logging
from omni_retail.lakehouse.trino import DbapiTrinoExecutor, TrinoConfig

from .boundary import (
    CdcBoundaryProbeError,
    ConfluentKafkaBoundaryReader,
    HttpConnectStatusReader,
    evaluate_streaming_health,
)
from .config import CdcConfig, CdcConfigError
from .consumer import CdcBatchWriter, CdcRunner, as_kafka_consumer


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume Debezium records into Iceberg Bronze")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run the CDC consumer")
    run.add_argument("--max-batches", type=int)
    run.add_argument("--max-messages", type=int)
    run.add_argument("--idle-timeout-seconds", type=float)
    health = commands.add_parser("healthcheck", help="check the consumer readiness heartbeat")
    health.add_argument("--path", default="/tmp/cdc-consumer.ready")
    health.add_argument("--max-age-seconds", type=float, default=60.0)
    commands.add_parser(
        "status",
        help="report connector, task, consumer-group, offset, and lag health",
    )
    return parser


def readiness_is_fresh(path: Path, *, max_age_seconds: float) -> bool:
    if max_age_seconds <= 0:
        raise ValueError("max_age_seconds must be greater than zero")
    try:
        age = time.time() - path.stat().st_mtime
    except FileNotFoundError:
        return False
    return 0 <= age <= max_age_seconds


def _status() -> int:
    config = CdcConfig.from_env()
    connect_reader = HttpConnectStatusReader(config.connect_url)
    try:
        connect = connect_reader.status(timeout_seconds=config.boundary_request_timeout_seconds)
        with ConfluentKafkaBoundaryReader(
            config.bootstrap_servers, config.group_id
        ) as kafka_reader:
            kafka = kafka_reader.sample(
                topics=config.topics,
                group_id=config.group_id,
                timeout_seconds=config.boundary_request_timeout_seconds,
            )
    except CdcBoundaryProbeError as exc:
        print(f"streaming health: UNHEALTHY: {exc}", file=sys.stderr)
        return 1

    health = evaluate_streaming_health(
        connect,
        kafka,
        topics=config.topics,
        group_id=config.group_id,
    )
    print(f"connector state={connect.connector_state}")
    if connect.task_states:
        for index, state in enumerate(connect.task_states):
            print(f"connector task={index} state={state}")
    else:
        print("connector tasks=none")
    print(
        f"consumer_group id={kafka.group_id} state={kafka.group_state} members={kafka.member_count}"
    )
    for item in sorted(kafka.partitions, key=lambda value: (value.topic, value.partition)):
        print(
            f"topic={item.topic} partition={item.partition} "
            f"committed={item.committed_offset} high={item.high_watermark} lag={item.lag}"
        )
    if health.is_healthy:
        total_lag = sum(item.lag for item in kafka.partitions)
        suffix = f" (catching up: total_lag={total_lag})" if total_lag else ""
        print(f"streaming health: HEALTHY{suffix}")
        return 0
    for problem in health.problems:
        print(f"problem: {problem}", file=sys.stderr)
    print("streaming health: UNHEALTHY", file=sys.stderr)
    return 1


def _run(args: argparse.Namespace) -> int:
    config = CdcConfig.from_env()
    stop = Event()

    def request_stop(_signum: int, _frame: object) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    consumer = Consumer(
        {
            "bootstrap.servers": config.bootstrap_servers,
            "group.id": config.group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "max.poll.interval.ms": 300_000,
            "session.timeout.ms": 45_000,
        }
    )
    trino_config = TrinoConfig.from_env()
    with DbapiTrinoExecutor(trino_config) as executor:
        writer = CdcBatchWriter(
            executor,
            catalog=trino_config.catalog,
            schema=config.bronze_schema,
            attempts=config.write_attempts,
        )
        runner = CdcRunner(
            as_kafka_consumer(consumer),
            writer,
            topics=config.topics,
            topic_tables=config.topic_tables,
            batch_size=config.batch_size,
            poll_timeout_seconds=config.poll_timeout_seconds,
            readiness_file=Path(config.readiness_file),
        )
        runner.run(
            should_stop=stop.is_set,
            max_batches=args.max_batches,
            max_messages=args.max_messages,
            idle_timeout_seconds=args.idle_timeout_seconds,
        )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    configure_logging()
    args = _parser().parse_args(argv)
    try:
        if args.command == "healthcheck":
            return (
                0
                if readiness_is_fresh(Path(args.path), max_age_seconds=args.max_age_seconds)
                else 1
            )
        if args.command == "status":
            return _status()
        return _run(args)
    except CdcConfigError as exc:
        print(f"streaming health: UNHEALTHY: {exc}", file=sys.stderr)
        return 1
