"""Stable CDC boundary probes, validation, and bounded-wait tests."""

# pyright: reportMissingImports=false, reportMissingTypeStubs=false

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.request import Request

import pytest
from confluent_kafka import TopicPartition

import omni_retail.streaming.cdc.boundary as boundary_module
from omni_retail.streaming.cdc.boundary import (
    CdcBoundary,
    CdcBoundaryProbeError,
    CdcBoundaryTimeoutError,
    ConfluentKafkaBoundaryReader,
    ConnectStatus,
    HttpConnectStatusReader,
    KafkaBoundarySample,
    PartitionSample,
    wait_for_stable_boundary,
)
from omni_retail.streaming.cdc.config import CdcConfig

TOPICS = (
    "omni.oltp.public.customers",
    "omni.oltp.public.orders",
    "omni.oltp.public.payments",
)
GROUP_ID = "omni-iceberg-bronze-cdc-v1"
CAPTURED_AT = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
RUNNING = ConnectStatus(connector_state="RUNNING", task_states=("RUNNING",))


def config(
    *,
    stability_seconds: float = 1.0,
    timeout_seconds: float = 10.0,
) -> CdcConfig:
    return CdcConfig(
        boundary_stability_seconds=stability_seconds,
        boundary_timeout_seconds=timeout_seconds,
        boundary_request_timeout_seconds=2.0,
    )


def sample(
    high_watermarks: tuple[int, int, int] = (10, 20, 30),
    *,
    committed_offsets: tuple[int, int, int] | None = None,
    member_count: int = 1,
    partitions: tuple[PartitionSample, ...] | None = None,
) -> KafkaBoundarySample:
    commits = high_watermarks if committed_offsets is None else committed_offsets
    effective_partitions = partitions or tuple(
        PartitionSample(
            topic=topic,
            partition=0,
            committed_offset=committed,
            high_watermark=high,
        )
        for topic, committed, high in zip(TOPICS, commits, high_watermarks, strict=True)
    )
    return KafkaBoundarySample(
        group_id=GROUP_ID,
        group_state="STABLE",
        member_count=member_count,
        partitions=effective_partitions,
    )


class FakeTime:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class FakeKafkaReader:
    def __init__(
        self,
        samples: Sequence[KafkaBoundarySample | CdcBoundaryProbeError],
    ) -> None:
        self._samples = list(samples)
        self._last = self._samples[-1]
        self.calls = 0

    def sample(
        self,
        *,
        topics: Sequence[str],
        group_id: str,
        timeout_seconds: float,
    ) -> KafkaBoundarySample:
        assert tuple(topics) == TOPICS
        assert group_id == GROUP_ID
        assert 0 < timeout_seconds <= 2.0
        self.calls += 1
        value = self._samples.pop(0) if self._samples else self._last
        if isinstance(value, CdcBoundaryProbeError):
            raise value
        return value


class FakeConnectReader:
    def __init__(
        self,
        statuses: Sequence[ConnectStatus | CdcBoundaryProbeError] = (RUNNING,),
    ) -> None:
        self._statuses = list(statuses)
        self._last = self._statuses[-1]
        self.calls = 0

    def status(self, *, timeout_seconds: float) -> ConnectStatus:
        assert 0 < timeout_seconds <= 2.0
        self.calls += 1
        value = self._statuses.pop(0) if self._statuses else self._last
        if isinstance(value, CdcBoundaryProbeError):
            raise value
        return value


def wait(
    kafka: FakeKafkaReader,
    connect: FakeConnectReader | None = None,
    *,
    runtime_config: CdcConfig | None = None,
    fake_time: FakeTime | None = None,
) -> tuple[CdcBoundary, FakeTime]:
    timeline = fake_time or FakeTime()
    result = wait_for_stable_boundary(
        runtime_config or config(),
        kafka,
        connect or FakeConnectReader(),
        monotonic=timeline.monotonic,
        sleep=timeline.sleep,
        clock=lambda: CAPTURED_AT,
    )
    return result, timeline


def test_two_identical_lag_zero_samples_capture_exclusive_offsets(
    caplog: pytest.LogCaptureFixture,
) -> None:
    kafka = FakeKafkaReader([sample(), sample()])

    with caplog.at_level(logging.INFO):
        result, timeline = wait(kafka)

    assert kafka.calls == 2
    assert timeline.sleeps == [1.0]
    assert result.group_id == GROUP_ID
    assert result.captured_at == CAPTURED_AT
    assert result.as_dict() == {
        TOPICS[0]: {"partition": 0, "offset_exclusive": 10},
        TOPICS[1]: {"partition": 0, "offset_exclusive": 20},
        TOPICS[2]: {"partition": 0, "offset_exclusive": 30},
    }
    assert "kafka_topic=omni.oltp.public.customers" in caplog.text
    assert "committed_offset=10" in caplog.text
    assert "high_watermark=10" in caplog.text
    assert "lag=0" in caplog.text
    assert "Stable CDC boundary captured" in caplog.text


def test_lag_must_clear_before_the_stability_window_starts() -> None:
    lagging = sample(committed_offsets=(9, 20, 30))
    kafka = FakeKafkaReader([lagging, sample(), sample()])

    result, timeline = wait(kafka)

    assert kafka.calls == 3
    assert timeline.sleeps == [1.0, 1.0]
    assert result.as_dict()[TOPICS[0]]["offset_exclusive"] == 10


def test_watermark_movement_restarts_the_stability_window() -> None:
    moved = sample((11, 20, 30))
    kafka = FakeKafkaReader([sample(), moved, moved])

    result, timeline = wait(kafka)

    assert kafka.calls == 3
    assert timeline.sleeps == [1.0, 1.0]
    assert result.as_dict()[TOPICS[0]]["offset_exclusive"] == 11


def test_connector_and_probe_failures_reset_the_candidate() -> None:
    stopped = ConnectStatus(connector_state="RUNNING", task_states=("FAILED",))
    connect = FakeConnectReader(
        [stopped, CdcBoundaryProbeError("Connect unavailable"), RUNNING, RUNNING]
    )
    kafka = FakeKafkaReader([sample(), sample()])

    result, timeline = wait(kafka, connect)

    assert connect.calls == 4
    assert kafka.calls == 2
    assert timeline.sleeps == [1.0, 1.0, 1.0]
    assert result.as_dict()[TOPICS[2]]["offset_exclusive"] == 30


def test_unhealthy_connector_times_out_without_querying_kafka() -> None:
    failed = ConnectStatus(connector_state="FAILED", task_states=("FAILED",))
    connect = FakeConnectReader([failed])
    kafka = FakeKafkaReader([sample()])

    with pytest.raises(CdcBoundaryTimeoutError, match="Kafka Connect is not running"):
        wait(kafka, connect, runtime_config=config(timeout_seconds=2.5))

    assert connect.calls == 3
    assert kafka.calls == 0


@pytest.mark.parametrize(
    ("unready_sample", "expected_reason"),
    [
        (sample(member_count=0), "no active member"),
        (
            sample(partitions=sample().partitions[:-1]),
            "topology mismatch",
        ),
        (
            sample(
                partitions=(
                    *sample().partitions,
                    PartitionSample(TOPICS[0], 1, 0, 0),
                )
            ),
            "topology mismatch",
        ),
        (
            sample(partitions=(*sample().partitions, sample().partitions[0])),
            "topology mismatch",
        ),
        (
            sample(
                partitions=(
                    PartitionSample(TOPICS[0], 0, -1001, 10),
                    *sample().partitions[1:],
                )
            ),
            "invalid committed offset",
        ),
        (
            sample(committed_offsets=(11, 20, 30)),
            "exceeds high watermark",
        ),
    ],
)
def test_invalid_group_topology_or_offsets_time_out_with_context(
    unready_sample: KafkaBoundarySample,
    expected_reason: str,
) -> None:
    kafka = FakeKafkaReader([unready_sample])
    runtime_config = config(timeout_seconds=2.5)

    with pytest.raises(CdcBoundaryTimeoutError, match=expected_reason):
        wait(kafka, runtime_config=runtime_config)

    assert kafka.calls == 3


def test_timeout_is_bounded_when_lag_never_clears() -> None:
    kafka = FakeKafkaReader([sample(committed_offsets=(9, 20, 30))])
    timeline = FakeTime()

    with pytest.raises(CdcBoundaryTimeoutError, match="timed out after 2.5s"):
        wait(
            kafka,
            runtime_config=config(timeout_seconds=2.5),
            fake_time=timeline,
        )

    assert timeline.now == 2.5
    assert kafka.calls == 3


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.closed = False

    def read(self) -> bytes:
        return self.payload

    def close(self) -> None:
        self.closed = True


class FakeOpener:
    def __init__(self, payload: bytes | OSError) -> None:
        self.payload = payload
        self.requests: list[tuple[str, float]] = []
        self.response: FakeResponse | None = None

    def open(self, request: Request, *, timeout: float) -> FakeResponse:
        self.requests.append((request.full_url, timeout))
        if isinstance(self.payload, OSError):
            raise self.payload
        self.response = FakeResponse(self.payload)
        return self.response


def test_connect_reader_fetches_only_status_and_parses_task_states() -> None:
    payload = json.dumps(
        {
            "name": "omni-postgres-cdc",
            "connector": {"state": "RUNNING", "worker_id": "worker"},
            "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "worker"}],
        }
    ).encode()
    opener = FakeOpener(payload)
    reader = HttpConnectStatusReader("http://connect:8083/", opener=opener)

    status = reader.status(timeout_seconds=3.0)

    assert status == RUNNING
    assert opener.requests == [("http://connect:8083/connectors/omni-postgres-cdc/status", 3.0)]
    assert opener.response is not None and opener.response.closed is True


@pytest.mark.parametrize("payload", [b"not-json", b"[]", b'{"connector": {}}'])
def test_connect_reader_rejects_unusable_responses(payload: bytes) -> None:
    reader = HttpConnectStatusReader("http://connect:8083", opener=FakeOpener(payload))

    with pytest.raises(CdcBoundaryProbeError, match="Kafka Connect"):
        reader.status(timeout_seconds=1.0)


def test_connect_reader_wraps_network_failures_without_response_details() -> None:
    reader = HttpConnectStatusReader(
        "http://connect:8083",
        opener=FakeOpener(OSError("network unavailable")),
    )

    with pytest.raises(CdcBoundaryProbeError, match="status request failed"):
        reader.status(timeout_seconds=1.0)


@dataclass
class FakeTopicMetadata:
    partitions: dict[int, object]
    error: object | None = None


@dataclass
class FakeClusterMetadata:
    topics: dict[str, FakeTopicMetadata]


@dataclass
class FakeGroupDescription:
    state: str = "STABLE"
    members: tuple[str, ...] = ("consumer-1",)


class FakeFuture:
    def result(self) -> FakeGroupDescription:
        return FakeGroupDescription()


class FakeConfluentConsumer:
    def __init__(self, configuration: dict[str, object]) -> None:
        self.configuration = configuration
        self.closed = False

    def list_topics(self, *, timeout: float) -> FakeClusterMetadata:
        assert timeout == 4.0
        return FakeClusterMetadata(
            topics={topic: FakeTopicMetadata(partitions={0: object()}) for topic in TOPICS}
        )

    def committed(
        self, coordinates: list[TopicPartition], *, timeout: float
    ) -> list[TopicPartition]:
        assert timeout == 4.0
        return [
            TopicPartition(position.topic, position.partition, (index + 1) * 10)
            for index, position in enumerate(coordinates)
        ]

    def get_watermark_offsets(
        self,
        coordinate: TopicPartition,
        *,
        timeout: float,
        cached: bool,
    ) -> tuple[int, int]:
        assert timeout == 4.0
        assert cached is False
        return 0, (TOPICS.index(coordinate.topic) + 1) * 10

    def close(self) -> None:
        self.closed = True


class FakeAdminClient:
    def __init__(self, configuration: dict[str, object]) -> None:
        self.configuration = configuration

    def describe_consumer_groups(
        self, group_ids: list[str], *, request_timeout: float
    ) -> dict[str, FakeFuture]:
        assert group_ids == [GROUP_ID]
        assert request_timeout == 4.0
        return {GROUP_ID: FakeFuture()}


def test_confluent_reader_reports_membership_commits_and_fresh_watermarks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumers: list[FakeConfluentConsumer] = []

    def consumer_factory(configuration: dict[str, object]) -> FakeConfluentConsumer:
        consumer = FakeConfluentConsumer(configuration)
        consumers.append(consumer)
        return consumer

    monkeypatch.setattr(boundary_module, "Consumer", consumer_factory)
    monkeypatch.setattr(boundary_module, "AdminClient", FakeAdminClient)

    with ConfluentKafkaBoundaryReader("kafka:29092", GROUP_ID) as reader:
        observed = reader.sample(
            topics=TOPICS,
            group_id=GROUP_ID,
            timeout_seconds=4.0,
        )

    assert observed.member_count == 1
    assert observed.group_state == "STABLE"
    assert [item.committed_offset for item in observed.partitions] == [10, 20, 30]
    assert [item.high_watermark for item in observed.partitions] == [10, 20, 30]
    assert consumers[0].configuration["group.id"] == GROUP_ID
    assert consumers[0].closed is True
