"""Idempotent Bronze loader: raw archive objects -> Iceberg via Trino SQL.

Load semantics (Phase 5 design spec §4, §6):
- raw objects are read and verified (schema, manifest row count) BEFORE any
  DML, so a failed verification never touches the existing partition;
- ``load(source, date)`` replaces the day's partition: ``DELETE`` by
  ``_batch_date`` followed by batched ``INSERT`` statements — re-running the
  same logical date (retry, backfill) cannot create duplicate rows;
- an empty day (no raw objects) is a warning-level no-op.
"""

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal, Protocol

import trino

from omni_retail.lakehouse.bronze.literals import date_literal
from omni_retail.lakehouse.bronze.specs import BATCH_DATE, SCHEMA_BRONZE, BronzeTableSpec

Clock = Callable[[], datetime]

logger = logging.getLogger(__name__)


class LoadError(Exception):
    """Explicit Bronze load failure (manifest missing, row-count mismatch)."""


@dataclass(frozen=True)
class TrinoConfig:
    """Connection settings for the local (unauthenticated) Trino coordinator."""

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
    """SQL execution boundary (trino.dbapi connection or a test fake)."""

    def execute(self, sql: str) -> None: ...


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
        cursor = self._connect().cursor()
        cursor.execute(sql)

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()  # type: ignore[no-untyped-call]
            self._connection = None

    def __enter__(self) -> "DbapiTrinoExecutor":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()


@dataclass(frozen=True)
class LoadResult:
    """Outcome of one (source, logical date) Bronze load."""

    source: str
    batch_id: str
    logical_date: date
    row_count: int
    status: Literal["loaded", "empty"]


def create_schema_sql(catalog: str) -> str:
    return f"create schema if not exists {catalog}.{SCHEMA_BRONZE}"


def create_table_sql(spec: BronzeTableSpec, catalog: str) -> str:
    columns = ", ".join(f'"{column.name}" {column.trino_type}' for column in spec.all_columns)
    return (
        f"create table if not exists {catalog}.{SCHEMA_BRONZE}.{spec.name} "
        f"({columns}) with (partitioning = ARRAY['_batch_date'])"
    )


def delete_partition_sql(spec: BronzeTableSpec, catalog: str, logical_date: date) -> str:
    return (
        f"delete from {catalog}.{SCHEMA_BRONZE}.{spec.name} "
        f'where "{BATCH_DATE}" = {date_literal(logical_date)}'
    )
