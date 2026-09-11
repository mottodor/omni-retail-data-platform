"""Unit tests for the API HTTP client retry/backoff/rate-limit policy.

All HTTP interaction is mocked with httpx.MockTransport (no network, no extra
dev dependencies — Phase 3 design spec §12).
"""

import random
from collections.abc import Callable

import httpx
import pytest

from omni_retail.ingestion.api.client import (
    ApiClient,
    ApiClientConfig,
    NonRetryableApiError,
    RetryExhaustedError,
    _parse_retry_after,
)


class FakeClock:
    """Monotonic clock the tests advance explicitly through the sleeper."""

    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class RecordingSleeper:
    """Sleeper that records delays and advances the fake clock."""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.sleeps: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.clock.now += seconds


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def sleeper(clock: FakeClock) -> RecordingSleeper:
    return RecordingSleeper(clock)


def make_client(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    clock: FakeClock,
    sleeper: RecordingSleeper,
    **config_overrides: object,
) -> ApiClient:
    config = ApiClientConfig(
        min_request_interval_seconds=0.0,
        **config_overrides,  # type: ignore[arg-type]
    )
    return ApiClient(
        config,
        transport=httpx.MockTransport(handler),
        sleeper=sleeper,
        clock=clock,
        rng=random.Random(42),
    )


def test_get_returns_successful_response(clock: FakeClock, sleeper: RecordingSleeper) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"ok": True})

    client = make_client(handler, clock=clock, sleeper=sleeper)
    response = client.get("/api/v1/fx-rates", {"page": 1})

    assert response.status_code == 200
    assert len(requests) == 1
    assert requests[0].url.params["page"] == "1"
    assert sleeper.sleeps == []


def test_get_retries_429_and_honors_retry_after(
    clock: FakeClock, sleeper: RecordingSleeper
) -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) <= 2:
            return httpx.Response(
                429, headers={"Retry-After": "0"}, json={"detail": "rate limited"}
            )
        return httpx.Response(200, json={"ok": True})

    client = make_client(handler, clock=clock, sleeper=sleeper)
    response = client.get("/api/v1/fx-rates")

    assert response.status_code == 200
    assert len(attempts) == 3
    # Retry-After: 0 replaces the exponential backoff for 429 responses.
    assert sleeper.sleeps == [0.0, 0.0]


def test_get_retries_500_with_exponential_backoff(
    clock: FakeClock, sleeper: RecordingSleeper
) -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) <= 3:
            return httpx.Response(500, json={"detail": "boom"})
        return httpx.Response(200, json={"ok": True})

    client = make_client(handler, clock=clock, sleeper=sleeper)
    response = client.get("/api/v1/fx-rates")

    assert response.status_code == 200
    assert len(attempts) == 4
    # base * 2**(attempt-1) + jitter(0..base): strictly increasing floors 0.5/1.0/2.0.
    first, second, third = sleeper.sleeps
    assert 0.5 <= first < 1.0
    assert 1.0 <= second < 1.5
    assert 2.0 <= third < 2.5


def test_get_retries_transport_errors(clock: FakeClock, sleeper: RecordingSleeper) -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) == 1:
            raise httpx.ConnectError("connection refused", request=request)
        if len(attempts) == 2:
            raise httpx.ReadTimeout("read timed out", request=request)
        return httpx.Response(200, json={"ok": True})

    client = make_client(handler, clock=clock, sleeper=sleeper)
    response = client.get("/api/v1/deliveries")

    assert response.status_code == 200
    assert len(attempts) == 3


def test_get_fails_immediately_on_404(clock: FakeClock, sleeper: RecordingSleeper) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(404, json={"detail": "not found"})

    client = make_client(handler, clock=clock, sleeper=sleeper)
    with pytest.raises(NonRetryableApiError) as excinfo:
        client.get("/api/v1/fx-rates")

    assert excinfo.value.status_code == 404
    assert len(requests) == 1
    assert sleeper.sleeps == []


def test_get_fails_immediately_on_422(clock: FakeClock, sleeper: RecordingSleeper) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"detail": "validation error"})

    client = make_client(handler, clock=clock, sleeper=sleeper)
    with pytest.raises(NonRetryableApiError) as excinfo:
        client.get("/api/v1/fx-rates")

    assert excinfo.value.status_code == 422


def test_retry_exhausted_after_max_attempts(clock: FakeClock, sleeper: RecordingSleeper) -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(500, json={"detail": "boom"})

    client = make_client(handler, clock=clock, sleeper=sleeper, max_attempts=3)
    with pytest.raises(RetryExhaustedError) as excinfo:
        client.get("/api/v1/fx-rates")

    assert excinfo.value.attempts == 3
    assert len(attempts) == 3
    assert "HTTP 500" in str(excinfo.value)


def test_overall_timeout_stops_retry_loop(clock: FakeClock, sleeper: RecordingSleeper) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(429, headers={"Retry-After": "60"}, json={"detail": "limited"})

    client = make_client(handler, clock=clock, sleeper=sleeper, overall_timeout_seconds=10.0)
    with pytest.raises(RetryExhaustedError) as excinfo:
        client.get("/api/v1/fx-rates")

    assert "overall timeout" in str(excinfo.value)
    # the capped Retry-After wait (30s) would exceed the 10s budget: no sleep happens
    assert sleeper.sleeps == []


def test_rate_limiter_enforces_min_interval_between_requests(
    clock: FakeClock, sleeper: RecordingSleeper
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    config = ApiClientConfig(min_request_interval_seconds=0.2)
    client = ApiClient(config, transport=httpx.MockTransport(handler), sleeper=sleeper, clock=clock)
    for _ in range(3):
        assert client.get("/api/v1/fx-rates").status_code == 200

    assert sleeper.sleeps == pytest.approx([0.2, 0.2])


def test_backoff_delay_respects_cap(clock: FakeClock, sleeper: RecordingSleeper) -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        return httpx.Response(500, json={"detail": "boom"})

    client = make_client(
        handler, clock=clock, sleeper=sleeper, max_attempts=5, backoff_max_seconds=1.0
    )
    with pytest.raises(RetryExhaustedError):
        client.get("/api/v1/fx-rates")

    assert len(sleeper.sleeps) == 4
    assert all(0.5 <= delay <= 1.5 for delay in sleeper.sleeps)


def test_parse_retry_after_caps_and_rejects_invalid_values() -> None:
    assert _parse_retry_after(None, 30.0) is None
    assert _parse_retry_after("not-a-number", 30.0) is None
    assert _parse_retry_after("2", 30.0) == 2.0
    assert _parse_retry_after("-5", 30.0) == 0.0
    assert _parse_retry_after("120", 30.0) == 30.0


def test_config_validation_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="max_attempts"):
        ApiClientConfig(max_attempts=0).validate()
    with pytest.raises(ValueError, match="min_request_interval_seconds"):
        ApiClientConfig(min_request_interval_seconds=-1).validate()
    with pytest.raises(ValueError, match="overall_timeout_seconds"):
        ApiClientConfig(overall_timeout_seconds=0).validate()


def test_config_from_env_reads_base_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MOCK_API_BASE_URL", "http://mock-api:9002")
    assert ApiClientConfig.from_env().base_url == "http://mock-api:9002"
    monkeypatch.delenv("MOCK_API_BASE_URL")
    assert ApiClientConfig.from_env().base_url == "http://127.0.0.1:9002"
