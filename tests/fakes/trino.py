"""In-memory fake implementing the TrinoExecutor protocol for unit tests."""


class FakeTrinoExecutor:
    """Records every SQL statement in order (no Trino involved).

    ``fetch`` answers from ``fetch_by_prefix``: the first prefix matching the
    SQL returns its rows, everything else returns no rows. This keeps query
    fakes declarative while the statements themselves stay recorded.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []
        self.fetch_by_prefix: dict[str, list[tuple[object, ...]]] = {}

    def execute(self, sql: str) -> None:
        self.statements.append(sql)

    def fetch(self, sql: str) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        for prefix, rows in self.fetch_by_prefix.items():
            if sql.startswith(prefix):
                return list(rows)
        return []

    def statements_matching(self, prefix: str) -> list[str]:
        return [sql for sql in self.statements if sql.startswith(prefix)]
