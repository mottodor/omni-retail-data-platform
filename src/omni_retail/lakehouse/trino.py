"""Focused Trino DB-API primitives shared by batch and streaming ingestion."""

import os
import re
from dataclasses import dataclass
from types import TracebackType
from typing import Protocol

import trino

_SCHEMA_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_TRANSIENT_CATALOG_MARKERS: tuple[str, ...] = (
    "Not authorized",
    # Trino 483 can hide Polaris's empty-body OAuth2 401 as an Iceberg
    # catalog load error; it is safe to resume after a Polaris restart.
    "Failed to load table:",
)
_TRANSIENT_TRINO_ERRORS = (
    trino.exceptions.TrinoConnectionError,
    trino.exceptions.Http502Error,
    trino.exceptions.Http503Error,
    trino.exceptions.Http504Error,
)


@dataclass(frozen=True)
class TrinoConfig:
    host: str = "127.0.0.1"
    port: int = 8080
    catalog: str = "iceberg"
    user: str = "omni"

    @classmethod
    def from_env(cls) -> "TrinoConfig":
        return cls(
            host=os.environ.get("TRINO_HOST", cls.host),
            port=int(os.environ.get("TRINO_PORT", str(cls.port))),
            catalog=os.environ.get("TRINO_CATALOG", cls.catalog),
            user=os.environ.get("TRINO_USER", cls.user),
        )


class TrinoExecutor(Protocol):
    def execute(self, sql: str) -> None: ...

    def fetch(self, sql: str) -> list[tuple[object, ...]]: ...


class DbapiTrinoExecutor:
    """Lazily-connecting autocommit executor over one trino.dbapi connection."""

    def __init__(self, config: TrinoConfig) -> None:
        self._config = config
        self._connection: trino.dbapi.Connection | None = None

    def _connect(self) -> trino.dbapi.Connection:
        if self._connection is None:
            self._connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
                host=self._config.host,
                port=self._config.port,
                user=self._config.user,
                catalog=self._config.catalog,
            )
        return self._connection

    def execute(self, sql: str) -> None:
        self._connect().cursor().execute(sql)

    def execute_with_params(self, sql: str, params: list[object]) -> None:
        """Execute parameterized SQL for callers that handle live event payloads."""
        # pi-lens-ignore: python-sql-injection
        self._connect().cursor().execute(sql, params)

    def fetch(self, sql: str) -> list[tuple[object, ...]]:
        cursor = self._connect().cursor()
        cursor.execute(sql)
        return list(cursor.fetchall())

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()  # type: ignore[no-untyped-call]
            self._connection = None

    def __enter__(self) -> "DbapiTrinoExecutor":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback
        self.close()


def validate_schema_name(schema: str) -> str:
    if not _SCHEMA_NAME_PATTERN.fullmatch(schema):
        raise ValueError(
            f"invalid schema name {schema!r}; expected lower-case letters, digits, "
            "underscores, starting with a letter"
        )
    return schema


def is_transient_catalog_error(error: BaseException) -> bool:
    if not isinstance(error, trino.exceptions.Error):
        return False
    message = getattr(error, "message", "")
    return isinstance(message, str) and any(
        marker in message for marker in _TRANSIENT_CATALOG_MARKERS
    )


def is_transient_trino_error(error: BaseException) -> bool:
    """Whether a failed Trino HTTP connection is safe for bounded retry."""
    return isinstance(error, _TRANSIENT_TRINO_ERRORS)
