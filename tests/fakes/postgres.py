"""In-memory fake of a psycopg server-side cursor for unit tests."""

from typing import Any

from omni_retail.ingestion.postgres_snapshot.extract import Row


class FakeSnapshotCursor:
    """Deterministic named-cursor fake: records executed SQL, serves fetchmany."""

    def __init__(self, rows: list[Row]) -> None:
        self._rows = list(rows)
        self.itersize = -1  # set by the extractor; asserted in tests
        self.executed_sql: str | None = None
        self.executed_params: tuple[object, ...] | None = None

    def execute(self, query: str, params: tuple[object, ...]) -> None:
        self.executed_sql = query
        self.executed_params = params

    def fetchmany(self, size: int) -> list[Row]:
        taken, self._rows = self._rows[:size], self._rows[size:]
        return taken

    def __enter__(self) -> "FakeSnapshotCursor":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        return None


class FakeSnapshotConnection:
    """Connection fake handing out one prepared cursor per ``cursor()`` call."""

    def __init__(self, rows: list[Row]) -> None:
        self._rows = list(rows)
        self.cursors: list[FakeSnapshotCursor] = []
        self.requested_names: list[str | None] = []

    def cursor(self, name: str | None = None) -> FakeSnapshotCursor:
        self.requested_names.append(name)
        cursor = FakeSnapshotCursor(self._rows)
        self.cursors.append(cursor)
        return cursor
