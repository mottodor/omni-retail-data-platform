"""In-memory fake implementing the ClickHouseExecutor protocol for unit tests."""

from omni_retail.serving.clickhouse.client import InsertSummary


class FakeClickHouseExecutor:
    """Records every command/insert in order (no ClickHouse involved).

    ``fail_inserts`` simulates a mid-publish fault to assert failure
    isolation; ``written_rows_override`` simulates a driver/server summary
    disagreeing with the inserted payload. ``query`` records the SQL and
    answers from ``query_rows`` (default: no rows).
    """

    def __init__(
        self, *, fail_inserts: bool = False, written_rows_override: int | None = None
    ) -> None:
        self.commands: list[str] = []
        self.inserts: list[tuple[str, tuple[str, ...], list[tuple[object, ...]]]] = []
        self.fail_inserts = fail_inserts
        self.written_rows_override = written_rows_override
        self.query_rows: list[tuple[object, ...]] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def query(self, sql: str) -> list[tuple[object, ...]]:
        self.commands.append(sql)
        return list(self.query_rows)

    def insert(
        self, table: str, columns: tuple[str, ...], rows: list[tuple[object, ...]]
    ) -> InsertSummary:
        if self.fail_inserts:
            raise RuntimeError("simulated ClickHouse insert failure")
        self.inserts.append((table, columns, list(rows)))
        written = len(rows) if self.written_rows_override is None else self.written_rows_override
        return InsertSummary(written_rows=written, written_bytes=0)

    def commands_matching(self, prefix: str) -> list[str]:
        return [sql for sql in self.commands if sql.startswith(prefix)]
