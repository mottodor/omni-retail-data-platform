"""In-memory fake implementing the TrinoExecutor protocol for unit tests."""

from collections.abc import Callable

FetchHandler = Callable[[str], list[tuple[object, ...]] | None]


class FakeTrinoExecutor:
    """Records SQL and supplies declarative or callback-based query answers."""

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.fetch_by_prefix: dict[str, list[tuple[object, ...]]] = {}
        self.fetch_handler: FetchHandler | None = None

    def execute(self, sql: str) -> None:
        self.statements.append(sql)

    def fetch(self, sql: str) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if self.fetch_handler is not None:
            result = self.fetch_handler(sql)
            if result is not None:
                return result
        for prefix, rows in self.fetch_by_prefix.items():
            if sql.startswith(prefix):
                return list(rows)
        return []

    def statements_matching(self, prefix: str) -> list[str]:
        return [sql for sql in self.statements if sql.startswith(prefix)]
