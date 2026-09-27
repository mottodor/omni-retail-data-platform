"""Env-driven configuration for the serving-layer ClickHouse clients."""

import os
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class ClickHouseConfig:
    """HTTP connection settings (clickhouse-connect) for one service account.

    Defaults address the compose `bi` profile from the host: loopback HTTP
    port 8123, the `analytics` serving database, and the publisher account.
    """

    host: str = "127.0.0.1"
    port: int = 8123
    database: str = "analytics"
    user: str = "omni_publisher"
    password: str = ""

    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        """Publisher account settings (the default client of this module)."""
        return cls(
            host=os.environ.get("CLICKHOUSE_HOST", cls.host),
            port=int(os.environ.get("CLICKHOUSE_PORT", str(cls.port))),
            database=os.environ.get("CLICKHOUSE_DB", cls.database),
            user=os.environ.get("CLICKHOUSE_PUBLISHER_USER", cls.user),
            password=os.environ.get("CLICKHOUSE_PUBLISHER_PASSWORD", cls.password),
        )

    @classmethod
    def reader_from_env(cls) -> "ClickHouseConfig":
        """Read-only BI account settings (``superset_reader``)."""
        return replace(
            cls.from_env(),
            user=os.environ.get("CLICKHOUSE_READER_USER", "superset_reader"),
            password=os.environ.get("CLICKHOUSE_READER_PASSWORD", ""),
        )
