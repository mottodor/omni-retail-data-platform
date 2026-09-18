"""In-memory fake implementing the TrinoExecutor protocol for unit tests."""


class FakeTrinoExecutor:
    """Records every executed SQL statement in order (no Trino involved)."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)

    def statements_matching(self, prefix: str) -> list[str]:
        return [sql for sql in self.statements if sql.startswith(prefix)]
