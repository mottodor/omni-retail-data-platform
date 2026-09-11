"""HTTP client for external API sources: timeout, retry, backoff, rate limit.

Retry policy (Phase 3 design spec §7):
- retryable: HTTP 429, HTTP 5xx, timeouts, connection errors;
- non-retryable: every other 4xx (fails immediately with ``NonRetryableApiError``);
- retries are bounded by ``max_attempts`` and an overall per-request timeout;
- ``Retry-After`` (seconds form) is honored, capped at ``retry_after_cap_seconds``.

Backoff is exponential with jitter: ``min(base * 2**(attempt-1), max) + U(0, base)``.
The rate limiter enforces a minimum interval between request starts (retries
included) so clients do not hammer rate-limited endpoints.
"""

import os
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import httpx

DEFAULT_BASE_URL = "http://127.0.0.1:9002"

Sleeper = Callable[[float], None]
MonotonicClock = Callable[[], float]


class ApiClientError(Exception):
    """Base class for API client failures."""


class NonRetryableApiError(ApiClientError):
    """Raised for non-retryable responses (4xx other than 429)."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class RetryExhaustedError(ApiClientError):
    """Raised when retryable failures persist beyond the allowed attempts."""

    def __init__(self, message: str, attempts: int) -> None:
        super().__init__(message)
        self.attempts = attempts


class ApiContractError(ApiClientError):
    """Raised when a response envelope violates the source contract."""


@dataclass(frozen=True)
class ApiClientConfig:
    """Settings of one external API endpoint; conservative production-like defaults."""

    base_url: str = DEFAULT_BASE_URL
    request_timeout_seconds: float = 10.0
    overall_timeout_seconds: float = 120.0
    max_attempts: int = 5
    backoff_base_seconds: float = 0.5
    backoff_max_seconds: float = 30.0
    retry_after_cap_seconds: float = 30.0
    min_request_interval_seconds: float = 0.2

    def validate(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
        for name in (
            "request_timeout_seconds",
            "overall_timeout_seconds",
            "backoff_base_seconds",
            "backoff_max_seconds",
            "retry_after_cap_seconds",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0, got {getattr(self, name)}")
        if self.min_request_interval_seconds < 0:
            interval = self.min_request_interval_seconds
            raise ValueError(f"min_request_interval_seconds must be >= 0, got {interval}")

    @classmethod
    def from_env(cls) -> "ApiClientConfig":
        return cls(base_url=os.environ.get("MOCK_API_BASE_URL", DEFAULT_BASE_URL))


class ApiClient:
    """Synchronous GET client with bounded retries and a client-side rate limit."""

    def __init__(
        self,
        config: ApiClientConfig,
        *,
        transport: httpx.BaseTransport | None = None,
        sleeper: Sleeper = time.sleep,
        clock: MonotonicClock = time.monotonic,
        rng: random.Random | None = None,
    ) -> None:
        config.validate()
        self._config = config
        self._sleeper = sleeper
        self._clock = clock
        self._rng = rng or random.Random()
        self._next_allowed_at = 0.0
        self._http = httpx.Client(
            base_url=config.base_url,
            timeout=httpx.Timeout(config.request_timeout_seconds),
            transport=transport,
        )

    def get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        """GET ``path`` with retries; returns the first successful 2xx response."""
        url = httpx.URL(path, params=params)
        deadline = self._clock() + self._config.overall_timeout_seconds
        attempts = 0
        last_failure = "no request was attempted"

        while attempts < self._config.max_attempts:
            self._respect_rate_limit()
            attempts += 1
            retry_after: float | None = None

            try:
                response = self._http.get(url)
            except httpx.TransportError as error:
                last_failure = f"transport error: {type(error).__name__}: {error}"
            else:
                if 200 <= response.status_code < 300:
                    return response
                if response.status_code == 429 or response.status_code >= 500:
                    last_failure = _failure_description(response)
                    retry_after = _parse_retry_after(
                        response.headers.get("Retry-After"), self._config.retry_after_cap_seconds
                    )
                else:
                    raise NonRetryableApiError(
                        f"GET {url} failed: {_failure_description(response)}",
                        response.status_code,
                    )

            if attempts >= self._config.max_attempts:
                break
            delay = retry_after if retry_after is not None else self._backoff_delay(attempts)
            if self._clock() + delay >= deadline:
                raise RetryExhaustedError(
                    f"GET {url}: overall timeout of "
                    f"{self._config.overall_timeout_seconds}s exceeded before retry "
                    f"(attempts={attempts}, last failure: {last_failure})",
                    attempts=attempts,
                )
            self._sleeper(delay)

        raise RetryExhaustedError(
            f"GET {url} failed after {attempts} attempts; last failure: {last_failure}",
            attempts=attempts,
        )

    def close(self) -> None:
        self._http.close()

    def _respect_rate_limit(self) -> None:
        interval = self._config.min_request_interval_seconds
        if interval <= 0:
            return
        now = self._clock()
        wait = self._next_allowed_at - now
        if wait > 0:
            self._sleeper(wait)
            now = self._clock()
        self._next_allowed_at = now + interval

    def _backoff_delay(self, attempt: int) -> float:
        exponential = min(
            self._config.backoff_base_seconds * (2.0 ** (attempt - 1)),
            self._config.backoff_max_seconds,
        )
        return exponential + self._rng.uniform(0, self._config.backoff_base_seconds)


def _failure_description(response: httpx.Response) -> str:
    body = response.text.strip().replace("\n", " ")
    if len(body) > 200:
        body = body[:200] + "..."
    return f"HTTP {response.status_code}: {body}"


def _parse_retry_after(header_value: str | None, cap_seconds: float) -> float | None:
    if header_value is None:
        return None
    try:
        seconds = float(header_value)
    except ValueError:
        return None  # HTTP-date form is not supported; fall back to backoff
    return min(max(seconds, 0.0), cap_seconds)
