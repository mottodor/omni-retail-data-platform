"""Trino SQL literal builders and batched INSERT statements (Phase 5 spec §4).

Values travel as Python objects and are rendered as *typed* SQL literals so a
batched multi-row INSERT is a single deterministic statement per chunk — no
driver-side parameter protocol involved.
"""

from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal

from omni_retail.lakehouse.bronze.specs import BronzeTableSpec

#: Rows per multi-row INSERT statement. Larger batches mean fewer catalog
#: commits per load (per-operation OAuth2 churn, trinodb/trino#30816) and
#: fewer tiny parquet files per Iceberg snapshot (AGENTS §22/§25).
ROWS_PER_STATEMENT = 5000

#: Character budget per INSERT statement: Trino rejects query text larger
#: than 1MB (QUERY_TEXT_TOO_LARGE), so batches must stay well under it.
MAX_STATEMENT_CHARS = 768_000

Row = Mapping[str, object]


def varchar_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def timestamp_literal(value: datetime) -> str:
    """``TIMESTAMP '... UTC'`` literal; naive values are assumed UTC."""
    moment = value if value.tzinfo else value.replace(tzinfo=UTC)
    return f"TIMESTAMP '{moment.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f} UTC'"


def date_literal(value: date) -> str:
    return f"DATE '{value:%Y-%m-%d}'"


def sql_literal(value: object, trino_type: str) -> str:
    """Render one Python value as a Trino SQL literal for ``trino_type``."""
    if value is None:
        return "null"
    if trino_type.startswith("varchar"):
        if not isinstance(value, str):
            raise TypeError(f"varchar column expects str, got {type(value).__name__}")
        return varchar_literal(value)
    if trino_type.startswith("decimal"):
        if isinstance(value, bool) or not isinstance(value, Decimal):
            raise TypeError(f"decimal column expects Decimal, got {type(value).__name__}")
        return f"DECIMAL '{value}'"
    if trino_type.startswith("timestamp"):
        if not isinstance(value, datetime):
            raise TypeError(f"timestamp column expects datetime, got {type(value).__name__}")
        return timestamp_literal(value)
    if trino_type == "date":
        if not isinstance(value, date) or isinstance(value, datetime):
            raise TypeError(f"date column expects datetime.date, got {type(value).__name__}")
        return date_literal(value)
    if trino_type == "boolean":
        if not isinstance(value, bool):
            raise TypeError(f"boolean column expects bool, got {type(value).__name__}")
        return "true" if value else "false"
    if trino_type in ("bigint", "integer"):
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{trino_type} column expects int, got {type(value).__name__}")
        return str(value)
    if trino_type == "double":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"double column expects float, got {type(value).__name__}")
        return repr(float(value))
    raise TypeError(f"unsupported trino type for literals: {trino_type!r}")


def insert_statements(
    spec: BronzeTableSpec,
    rows: Sequence[Row],
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
    rows_per_statement: int = ROWS_PER_STATEMENT,
    max_statement_chars: int = MAX_STATEMENT_CHARS,
) -> list[str]:
    """Batched multi-row INSERTs covering every row in declaration order.

    A batch is cut at ``rows_per_statement`` rows OR when the rendered
    statement would exceed ``max_statement_chars`` — whichever comes first.
    """
    if rows_per_statement < 1:
        raise ValueError(f"rows_per_statement must be >= 1, got {rows_per_statement}")
    columns = spec.all_columns
    names = [column.name for column in columns]
    for row in rows:
        missing = [name for name in names if name not in row]
        if missing:
            raise ValueError(f"{spec.name}: row missing columns {missing}")

    column_list = ", ".join(f'"{name}"' for name in names)
    header = f"insert into {catalog}.{schema}.{spec.name} ({column_list}) values "

    statements: list[str] = []
    chunk: list[str] = []
    chunk_chars = len(header)

    def flush() -> None:
        nonlocal chunk, chunk_chars
        if chunk:
            statements.append(header + ", ".join(chunk))
            chunk = []
            chunk_chars = len(header)

    for row in rows:
        rendered = (
            "("
            + ", ".join(
                sql_literal(row[name], column.trino_type)
                for name, column in zip(names, columns, strict=True)
            )
            + ")"
        )
        separator_chars = 2 if chunk else 0
        if chunk and (
            chunk_chars + separator_chars + len(rendered) > max_statement_chars
            or len(chunk) >= rows_per_statement
        ):
            flush()
        chunk.append(rendered)
        chunk_chars += (2 if len(chunk) > 1 else 0) + len(rendered)
    flush()
    return statements
