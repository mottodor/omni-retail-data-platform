"""clickhouse-connect boundary for the serving publisher.

The publisher never talks to ClickHouse directly: everything goes through
the ClickHouseExecutor protocol, so unit tests run against an in-memory
fake (no network at test time, AGENTS §48) while production code uses
clickhouse-connect over the HTTP interface — the official driver and the
Superset-recommended one (ADR 0004).
"""

from dataclasses import dataclass
from typing import Protocol

import clickhouse_connect
from clickhouse_connect.driver import Client

from omni_retail.serving.clickhouse.config import ClickHouseConfig


@dataclass(frozen=True)
class InsertSummary:
    """Measurable outcome of one insert (guide §26.3)."""

    written_rows: int
    written_bytes: int


class ClickHouseExecutor(Protocol):
    """SQL execution boundary (clickhouse-connect client or a test fake)."""

    def command(self, sql: str) -> None: ...

    def query(self, sql: str) -> list[tuple[object, ...]]: ...

    def insert(
        self, table: str, columns: tuple[str, ...], rows: list[tuple[object, ...]]
    ) -> InsertSummary: ...


class ClickHouseConnectClient:
    """Lazily-connecting wrapper over one clickhouse-connect client."""

    def __init__(self, config: ClickHouseConfig) -> None:
        self._config = config
        self._client: Client | None = None

    def _connect(self) -> Client:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self._config.host,
                port=self._config.port,
                username=self._config.user,
                password=self._config.password,
                database=self._config.database,
            )
        return self._client

    def command(self, sql: str) -> None:
        self._connect().command(sql)

    def query(self, sql: str) -> list[tuple[object, ...]]:
        return [tuple(row) for row in self._connect().query(sql).result_rows]

    def insert(
        self, table: str, columns: tuple[str, ...], rows: list[tuple[object, ...]]
    ) -> InsertSummary:
        summary = self._connect().insert(
            table=table, data=rows, column_names=list(columns), database=self._config.database
        )
        return InsertSummary(
            written_rows=summary.written_rows, written_bytes=summary.written_bytes()
        )

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self) -> "ClickHouseConnectClient":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
