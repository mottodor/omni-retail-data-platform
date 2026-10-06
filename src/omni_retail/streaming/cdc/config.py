"""Environment-driven configuration for CDC ingestion and boundary checks."""

import math
import os
from dataclasses import dataclass
from urllib.parse import urlsplit

DEFAULT_TABLES = ("customers", "orders", "payments")


class CdcConfigError(ValueError):
    """CDC configuration is missing or invalid."""


def _positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise CdcConfigError(f"{name} must be an integer") from exc
    if value <= 0:
        raise CdcConfigError(f"{name} must be greater than zero")
    return value


def _positive_float(name: str, default: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise CdcConfigError(f"{name} must be numeric") from exc
    if not math.isfinite(value) or value <= 0:
        raise CdcConfigError(f"{name} must be a finite number greater than zero")
    return value


def _http_url(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip().rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise CdcConfigError(f"{name} must be an absolute HTTP(S) URL")
    if parsed.username is not None or parsed.password is not None:
        raise CdcConfigError(f"{name} must not contain credentials")
    return value


@dataclass(frozen=True)
class CdcConfig:
    """Validated runtime settings; secrets are not represented here."""

    bootstrap_servers: str = "127.0.0.1:9092"
    group_id: str = "omni-iceberg-bronze-cdc-v1"
    topic_prefix: str = "omni.oltp"
    batch_size: int = 500
    poll_timeout_seconds: float = 1.0
    write_attempts: int = 3
    readiness_file: str = "/tmp/cdc-consumer.ready"
    bronze_schema: str = "bronze"
    connect_url: str = "http://127.0.0.1:8083"
    boundary_stability_seconds: float = 10.0
    boundary_timeout_seconds: float = 300.0
    boundary_request_timeout_seconds: float = 10.0

    @classmethod
    def from_env(cls) -> "CdcConfig":
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers).strip()
        group_id = os.environ.get("CDC_CONSUMER_GROUP_ID", cls.group_id).strip()
        topic_prefix = os.environ.get("CDC_TOPIC_PREFIX", cls.topic_prefix).strip()
        readiness_file = os.environ.get("CDC_READINESS_FILE", cls.readiness_file).strip()
        bronze_schema = os.environ.get("ICEBERG_BRONZE_SCHEMA", cls.bronze_schema).strip()
        connect_url = _http_url("KAFKA_CONNECT_URL", cls.connect_url)
        for name, value in (
            ("KAFKA_BOOTSTRAP_SERVERS", bootstrap_servers),
            ("CDC_CONSUMER_GROUP_ID", group_id),
            ("CDC_TOPIC_PREFIX", topic_prefix),
            ("CDC_READINESS_FILE", readiness_file),
            ("ICEBERG_BRONZE_SCHEMA", bronze_schema),
        ):
            if not value:
                raise CdcConfigError(f"{name} must not be empty")
        if topic_prefix != cls.topic_prefix:
            raise CdcConfigError(
                f"CDC_TOPIC_PREFIX must remain {cls.topic_prefix!r} in the v1 event namespace"
            )
        return cls(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topic_prefix=topic_prefix,
            batch_size=_positive_int("CDC_BATCH_SIZE", cls.batch_size),
            poll_timeout_seconds=_positive_float(
                "CDC_POLL_TIMEOUT_SECONDS", cls.poll_timeout_seconds
            ),
            write_attempts=_positive_int("CDC_WRITE_ATTEMPTS", cls.write_attempts),
            readiness_file=readiness_file,
            bronze_schema=bronze_schema,
            connect_url=connect_url,
            boundary_stability_seconds=_positive_float(
                "CDC_BOUNDARY_STABILITY_SECONDS", cls.boundary_stability_seconds
            ),
            boundary_timeout_seconds=_positive_float(
                "CDC_BOUNDARY_TIMEOUT_SECONDS", cls.boundary_timeout_seconds
            ),
            boundary_request_timeout_seconds=_positive_float(
                "CDC_BOUNDARY_REQUEST_TIMEOUT_SECONDS",
                cls.boundary_request_timeout_seconds,
            ),
        )

    @property
    def topic_tables(self) -> dict[str, str]:
        return {f"{self.topic_prefix}.public.{table}": table for table in DEFAULT_TABLES}

    @property
    def topics(self) -> tuple[str, ...]:
        return tuple(self.topic_tables)
