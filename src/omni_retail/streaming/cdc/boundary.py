"""Stable Kafka consumer boundaries for coordinated CDC analytical refreshes.

The boundary is transport progress, not a cross-topic ordering or exactly-once
claim. PostgreSQL LSN remains the source-order field used by downstream dbt
models (ADR 0008).
"""

# pyright: reportMissingTypeStubs=false

from __future__ import annotations

import json
import time
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, cast

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from omni_retail.ingestion.common.logging import context_logger

from .config import CdcConfig

CONNECTOR_NAME = "omni-postgres-cdc"
PARTITION_ID = 0
_PROBE_INTERVAL_SECONDS = 1.0


class CdcBoundaryProbeError(RuntimeError):
    """A bounded Kafka or Connect probe could not return usable state."""


class CdcBoundaryTimeoutError(RuntimeError):
    """No stable lag-zero CDC boundary appeared before the configured deadline."""


@dataclass(frozen=True)
class ConnectStatus:
    """Non-secret connector/task state returned by Kafka Connect's status API."""

    connector_state: str
    task_states: tuple[str, ...]

    @property
    def is_running(self) -> bool:
        return (
            self.connector_state == "RUNNING"
            and bool(self.task_states)
            and all(state == "RUNNING" for state in self.task_states)
        )

    @property
    def summary(self) -> str:
        return f"connector={self.connector_state} tasks={list(self.task_states)}"


@dataclass(frozen=True)
class PartitionSample:
    """One consumer-group position paired with the broker's exclusive high offset."""

    topic: str
    partition: int
    committed_offset: int
    high_watermark: int

    @property
    def lag(self) -> int:
        return self.high_watermark - self.committed_offset


@dataclass(frozen=True)
class KafkaBoundarySample:
    """One point-in-time probe of group membership and selected topic positions."""

    group_id: str
    group_state: str
    member_count: int
    partitions: tuple[PartitionSample, ...]


@dataclass(frozen=True)
class StreamingHealth:
    """One non-secret point-in-time health result for the streaming profile."""

    connect: ConnectStatus
    kafka: KafkaBoundarySample
    problems: tuple[str, ...]

    @property
    def is_healthy(self) -> bool:
        return not self.problems


@dataclass(frozen=True)
class BoundaryPosition:
    """A partition and its exclusive upper offset for a frozen dbt read."""

    topic: str
    partition: int
    offset_exclusive: int


@dataclass(frozen=True)
class CdcBoundary:
    """Stable, XCom-serializable transport frontier for all captured topics."""

    group_id: str
    captured_at: datetime
    positions: tuple[BoundaryPosition, ...]

    def as_dict(self) -> dict[str, dict[str, int]]:
        """Return the topic-keyed mapping passed to Airflow XCom and dbt vars."""
        return {
            position.topic: {
                "partition": position.partition,
                "offset_exclusive": position.offset_exclusive,
            }
            for position in sorted(self.positions, key=lambda item: item.topic)
        }


class KafkaBoundaryReader(Protocol):
    """Adapter boundary for Kafka metadata, group membership, and offsets."""

    def sample(
        self,
        *,
        topics: Sequence[str],
        group_id: str,
        timeout_seconds: float,
    ) -> KafkaBoundarySample: ...


class ConnectStatusReader(Protocol):
    """Adapter boundary for the non-secret Kafka Connect status endpoint."""

    def status(self, *, timeout_seconds: float) -> ConnectStatus: ...


class _HttpResponse(Protocol):
    def read(self) -> bytes: ...

    def close(self) -> None: ...


class _HttpOpener(Protocol):
    def open(self, request: urllib.request.Request, *, timeout: float) -> _HttpResponse: ...


class HttpConnectStatusReader:
    """Read only connector status, bypassing ambient HTTP proxy settings."""

    def __init__(
        self,
        base_url: str,
        *,
        connector_name: str = CONNECTOR_NAME,
        opener: _HttpOpener | None = None,
    ) -> None:
        self._status_url = f"{base_url.rstrip('/')}/connectors/{connector_name}/status"
        self._opener = opener or cast(
            _HttpOpener,
            urllib.request.build_opener(urllib.request.ProxyHandler({})),
        )

    def status(self, *, timeout_seconds: float) -> ConnectStatus:
        request = urllib.request.Request(self._status_url)
        try:
            response = self._opener.open(request, timeout=timeout_seconds)  # noqa: S310
            try:
                payload = json.loads(response.read().decode("utf-8"))
            finally:
                response.close()
        except OSError as exc:
            raise CdcBoundaryProbeError("Kafka Connect status request failed") from exc
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CdcBoundaryProbeError("Kafka Connect status response is not valid JSON") from exc

        if not isinstance(payload, dict):
            raise CdcBoundaryProbeError("Kafka Connect status response must be an object")
        connector = payload.get("connector")
        tasks = payload.get("tasks")
        if not isinstance(connector, dict) or not isinstance(tasks, list):
            raise CdcBoundaryProbeError("Kafka Connect status response has invalid shape")
        connector_state = connector.get("state")
        if not isinstance(connector_state, str):
            raise CdcBoundaryProbeError("Kafka Connect connector state is missing")

        task_states: list[str] = []
        for task in tasks:
            if not isinstance(task, dict) or not isinstance(task.get("state"), str):
                raise CdcBoundaryProbeError("Kafka Connect task state is missing")
            task_states.append(cast(str, task["state"]))
        return ConnectStatus(connector_state=connector_state, task_states=tuple(task_states))


class ConfluentKafkaBoundaryReader:
    """Probe Kafka without subscribing or joining the production consumer group."""

    def __init__(self, bootstrap_servers: str, group_id: str) -> None:
        common_config: dict[str, str | int | float | bool] = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "omni-cdc-boundary",
        }
        self._consumer = Consumer(
            {
                **common_config,
                "group.id": group_id,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            }
        )
        self._admin = AdminClient(common_config)

    def close(self) -> None:
        self._consumer.close()

    def __enter__(self) -> ConfluentKafkaBoundaryReader:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _traceback: object) -> None:
        self.close()

    def sample(
        self,
        *,
        topics: Sequence[str],
        group_id: str,
        timeout_seconds: float,
    ) -> KafkaBoundarySample:
        try:
            metadata = self._consumer.list_topics(timeout=timeout_seconds)
            coordinates: list[TopicPartition] = []
            for topic in topics:
                topic_metadata = metadata.topics.get(topic)
                if topic_metadata is None:
                    raise CdcBoundaryProbeError(f"Kafka topic is missing: {topic}")
                if topic_metadata.error is not None:
                    raise CdcBoundaryProbeError(
                        f"Kafka topic metadata failed for {topic}: {topic_metadata.error}"
                    )
                coordinates.extend(
                    TopicPartition(topic, partition)
                    for partition in sorted(topic_metadata.partitions)
                )

            descriptions = self._admin.describe_consumer_groups(
                [group_id], request_timeout=timeout_seconds
            )
            description_future = descriptions.get(group_id)
            if description_future is None:
                raise CdcBoundaryProbeError(
                    f"Kafka consumer group description is missing: {group_id}"
                )
            description = description_future.result()
            committed = self._consumer.committed(coordinates, timeout=timeout_seconds)
            committed_by_coordinate = {
                (position.topic, position.partition): position for position in committed
            }

            partition_samples: list[PartitionSample] = []
            for coordinate in coordinates:
                key = (coordinate.topic, coordinate.partition)
                position = committed_by_coordinate.get(key)
                if position is None:
                    raise CdcBoundaryProbeError(
                        "Kafka committed offset is missing for "
                        f"{coordinate.topic}[{coordinate.partition}]"
                    )
                if position.error is not None:
                    raise CdcBoundaryProbeError(
                        "Kafka committed offset failed for "
                        f"{coordinate.topic}[{coordinate.partition}]: {position.error}"
                    )
                _low, high = self._consumer.get_watermark_offsets(
                    coordinate,
                    timeout=timeout_seconds,
                    cached=False,
                )
                partition_samples.append(
                    PartitionSample(
                        topic=coordinate.topic,
                        partition=coordinate.partition,
                        committed_offset=position.offset,
                        high_watermark=high,
                    )
                )
        except KafkaException as exc:
            raise CdcBoundaryProbeError("Kafka boundary probe failed") from exc

        return KafkaBoundarySample(
            group_id=group_id,
            group_state=str(description.state),
            member_count=len(description.members),
            partitions=tuple(partition_samples),
        )


def _expected_coordinates(topics: Sequence[str]) -> set[tuple[str, int]]:
    return {(topic, PARTITION_ID) for topic in topics}


def evaluate_streaming_health(
    connect: ConnectStatus,
    sample: KafkaBoundarySample,
    *,
    topics: Sequence[str],
    group_id: str,
) -> StreamingHealth:
    """Evaluate service health without requiring the stricter lag-zero boundary."""
    problems: list[str] = []
    if connect.connector_state != "RUNNING":
        problems.append(f"connector is not RUNNING: {connect.connector_state}")
    if not connect.task_states:
        problems.append("connector has no tasks")
    problems.extend(
        f"connector task {index} is not RUNNING: {state}"
        for index, state in enumerate(connect.task_states)
        if state != "RUNNING"
    )

    if sample.group_id != group_id:
        problems.append(f"consumer group mismatch: expected={group_id} actual={sample.group_id}")
    if sample.member_count <= 0:
        problems.append(f"consumer group has no active member: state={sample.group_state}")

    expected = _expected_coordinates(topics)
    actual = {(item.topic, item.partition) for item in sample.partitions}
    if actual != expected or len(sample.partitions) != len(expected):
        problems.append(
            "topic partition topology mismatch: "
            f"expected={sorted(expected)} actual={sorted(actual)} "
            f"sample_count={len(sample.partitions)}"
        )

    for item in sample.partitions:
        coordinate = f"{item.topic}[{item.partition}]"
        if item.committed_offset < 0:
            problems.append(f"invalid committed offset for {coordinate}: {item.committed_offset}")
        if item.high_watermark < 0:
            problems.append(f"invalid high watermark for {coordinate}: {item.high_watermark}")
        if item.committed_offset > item.high_watermark:
            problems.append(
                f"committed offset exceeds high watermark for {coordinate}: "
                f"committed={item.committed_offset} high={item.high_watermark}"
            )

    return StreamingHealth(connect=connect, kafka=sample, problems=tuple(problems))


def _sample_not_ready_reason(
    sample: KafkaBoundarySample,
    *,
    topics: Sequence[str],
    group_id: str,
) -> str | None:
    health = evaluate_streaming_health(
        ConnectStatus(connector_state="RUNNING", task_states=("RUNNING",)),
        sample,
        topics=topics,
        group_id=group_id,
    )
    if health.problems:
        return health.problems[0]

    for item in sample.partitions:
        if item.lag != 0:
            coordinate = f"{item.topic}[{item.partition}]"
            return (
                f"consumer lag is non-zero for {coordinate}: "
                f"committed={item.committed_offset} high={item.high_watermark} "
                f"lag={item.lag}"
            )
    return None


def _watermarks(sample: KafkaBoundarySample) -> tuple[tuple[str, int, int], ...]:
    return tuple(
        sorted((item.topic, item.partition, item.high_watermark) for item in sample.partitions)
    )


def _boundary_from_sample(sample: KafkaBoundarySample, captured_at: datetime) -> CdcBoundary:
    return CdcBoundary(
        group_id=sample.group_id,
        captured_at=captured_at,
        positions=tuple(
            BoundaryPosition(
                topic=item.topic,
                partition=item.partition,
                offset_exclusive=item.high_watermark,
            )
            for item in sorted(sample.partitions, key=lambda value: value.topic)
        ),
    )


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _log_partition_samples(sample: KafkaBoundarySample, *, attempt: int, elapsed: float) -> None:
    for item in sorted(sample.partitions, key=lambda value: (value.topic, value.partition)):
        context_logger(
            __name__,
            attempt=attempt,
            committed_offset=item.committed_offset,
            elapsed_seconds=round(elapsed, 3),
            group_member_count=sample.member_count,
            group_state=sample.group_state,
            high_watermark=item.high_watermark,
            kafka_partition=item.partition,
            kafka_topic=item.topic,
            lag=item.lag,
        ).info("CDC boundary partition sampled")


def wait_for_stable_boundary(
    config: CdcConfig,
    kafka_reader: KafkaBoundaryReader,
    connect_reader: ConnectStatusReader,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] = _utc_now,
) -> CdcBoundary:
    """Wait for two identical, lag-zero samples inside a finite deadline."""
    started_at = monotonic()
    deadline = started_at + config.boundary_timeout_seconds
    candidate_watermarks: tuple[tuple[str, int, int], ...] | None = None
    candidate_since: float | None = None
    attempt = 0
    last_reason = "no boundary sample completed"

    while True:
        before_probe = monotonic()
        remaining = deadline - before_probe
        if remaining <= 0:
            raise CdcBoundaryTimeoutError(
                f"CDC boundary timed out after {config.boundary_timeout_seconds:g}s: {last_reason}"
            )
        request_timeout = min(config.boundary_request_timeout_seconds, remaining)
        attempt += 1

        try:
            connect_status = connect_reader.status(timeout_seconds=request_timeout)
            if not connect_status.is_running:
                last_reason = f"Kafka Connect is not running: {connect_status.summary}"
                candidate_watermarks = None
                candidate_since = None
            else:
                kafka_remaining = deadline - monotonic()
                if kafka_remaining <= 0:
                    raise CdcBoundaryTimeoutError(
                        "CDC boundary timed out while checking Kafka after a healthy "
                        "Connect response"
                    )
                sample = kafka_reader.sample(
                    topics=config.topics,
                    group_id=config.group_id,
                    timeout_seconds=min(
                        config.boundary_request_timeout_seconds,
                        kafka_remaining,
                    ),
                )
                sampled_at = monotonic()
                if sampled_at > deadline:
                    raise CdcBoundaryTimeoutError(
                        "CDC boundary timed out while waiting for the Kafka probe"
                    )
                elapsed = sampled_at - started_at
                _log_partition_samples(sample, attempt=attempt, elapsed=elapsed)
                reason = _sample_not_ready_reason(
                    sample,
                    topics=config.topics,
                    group_id=config.group_id,
                )
                if reason is not None:
                    last_reason = reason
                    candidate_watermarks = None
                    candidate_since = None
                else:
                    watermarks = _watermarks(sample)
                    if watermarks != candidate_watermarks:
                        candidate_watermarks = watermarks
                        candidate_since = sampled_at
                        last_reason = "stable window has not elapsed"
                    elif candidate_since is None:
                        raise AssertionError("candidate timestamp missing")
                    elif sampled_at - candidate_since >= config.boundary_stability_seconds:
                        boundary = _boundary_from_sample(sample, clock())
                        context_logger(
                            __name__,
                            attempt=attempt,
                            elapsed_seconds=round(elapsed, 3),
                            group_id=sample.group_id,
                            topic_count=len(boundary.positions),
                        ).info("Stable CDC boundary captured")
                        return boundary
                    else:
                        last_reason = "stable window has not elapsed"
        except CdcBoundaryProbeError as exc:
            last_reason = str(exc)
            candidate_watermarks = None
            candidate_since = None

        elapsed = monotonic() - started_at
        context_logger(
            __name__,
            attempt=attempt,
            elapsed_seconds=round(elapsed, 3),
            reason=last_reason,
        ).info("CDC boundary not ready")
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise CdcBoundaryTimeoutError(
                f"CDC boundary timed out after {config.boundary_timeout_seconds:g}s: {last_reason}"
            )
        sleep(min(_PROBE_INTERVAL_SECONDS, config.boundary_stability_seconds, remaining))
