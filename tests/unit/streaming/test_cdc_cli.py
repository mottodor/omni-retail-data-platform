"""Host-side CDC status command tests."""

from types import TracebackType

import pytest

import omni_retail.streaming.cdc.cli as cli_module
from omni_retail.streaming.cdc.boundary import (
    CdcBoundaryProbeError,
    ConnectStatus,
    KafkaBoundarySample,
    PartitionSample,
)
from omni_retail.streaming.cdc.config import CdcConfig

TOPICS = (
    "omni.oltp.public.customers",
    "omni.oltp.public.orders",
    "omni.oltp.public.payments",
)
GROUP_ID = "omni-iceberg-bronze-cdc-v1"


class FakeConnectReader:
    result: ConnectStatus | CdcBoundaryProbeError = ConnectStatus(
        connector_state="RUNNING", task_states=("RUNNING",)
    )

    def __init__(self, base_url: str) -> None:
        assert base_url == "http://127.0.0.1:8083"

    def status(self, *, timeout_seconds: float) -> ConnectStatus:
        assert timeout_seconds == 2.0
        if isinstance(self.result, CdcBoundaryProbeError):
            raise self.result
        return self.result


class FakeKafkaReader:
    result = KafkaBoundarySample(
        group_id=GROUP_ID,
        group_state="STABLE",
        member_count=1,
        partitions=tuple(
            PartitionSample(topic, 0, committed, high)
            for topic, committed, high in zip(TOPICS, (8, 20, 30), (10, 20, 30), strict=True)
        ),
    )

    def __init__(self, bootstrap_servers: str, group_id: str) -> None:
        assert bootstrap_servers == "127.0.0.1:9092"
        assert group_id == GROUP_ID

    def __enter__(self) -> "FakeKafkaReader":
        return self

    def __exit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        return None

    def sample(
        self,
        *,
        topics: tuple[str, ...],
        group_id: str,
        timeout_seconds: float,
    ) -> KafkaBoundarySample:
        assert topics == TOPICS
        assert group_id == GROUP_ID
        assert timeout_seconds == 2.0
        return self.result


@pytest.fixture(autouse=True)
def status_fakes(monkeypatch: pytest.MonkeyPatch) -> None:
    config = CdcConfig(boundary_request_timeout_seconds=2.0)
    monkeypatch.setattr(CdcConfig, "from_env", classmethod(lambda _cls: config))
    monkeypatch.setattr(cli_module, "HttpConnectStatusReader", FakeConnectReader)
    monkeypatch.setattr(cli_module, "ConfluentKafkaBoundaryReader", FakeKafkaReader)
    FakeConnectReader.result = ConnectStatus(connector_state="RUNNING", task_states=("RUNNING",))
    FakeKafkaReader.result = KafkaBoundarySample(
        group_id=GROUP_ID,
        group_state="STABLE",
        member_count=1,
        partitions=tuple(
            PartitionSample(topic, 0, committed, high)
            for topic, committed, high in zip(TOPICS, (8, 20, 30), (10, 20, 30), strict=True)
        ),
    )


def test_status_reports_non_zero_lag_without_failing_health(
    capsys: pytest.CaptureFixture[str],
) -> None:
    result = cli_module.main(["status"])

    captured = capsys.readouterr()
    assert result == 0
    assert "connector task=0 state=RUNNING" in captured.out
    assert "members=1" in captured.out
    assert "topic=omni.oltp.public.customers partition=0 committed=8 high=10 lag=2" in captured.out
    assert "streaming health: HEALTHY (catching up: total_lag=2)" in captured.out
    assert captured.err == ""


def test_status_returns_non_zero_for_failed_task_and_missing_member(
    capsys: pytest.CaptureFixture[str],
) -> None:
    FakeConnectReader.result = ConnectStatus(connector_state="RUNNING", task_states=("FAILED",))
    FakeKafkaReader.result = KafkaBoundarySample(
        group_id=GROUP_ID,
        group_state="EMPTY",
        member_count=0,
        partitions=FakeKafkaReader.result.partitions,
    )

    result = cli_module.main(["status"])

    captured = capsys.readouterr()
    assert result == 1
    assert "connector task 0 is not RUNNING: FAILED" in captured.err
    assert "consumer group has no active member" in captured.err
    assert "streaming health: UNHEALTHY" in captured.err


def test_status_returns_non_zero_when_connect_is_unreachable(
    capsys: pytest.CaptureFixture[str],
) -> None:
    FakeConnectReader.result = CdcBoundaryProbeError("Kafka Connect status request failed")

    result = cli_module.main(["status"])

    captured = capsys.readouterr()
    assert result == 1
    assert "Kafka Connect status request failed" in captured.err
