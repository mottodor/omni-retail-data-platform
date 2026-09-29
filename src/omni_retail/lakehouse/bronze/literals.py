"""Typed Trino SQL literal and resumable INSERT-chunk builders."""

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

from omni_retail.lakehouse.bronze.specs import (
    SOURCE_OBJECT,
    SOURCE_OBJECT_ROW_POSITION,
    BronzeTableSpec,
)

#: Rows per multi-row INSERT. Fewer bounded Iceberg commits constrain catalog
#: metadata growth during a full rebuild; the SQL remains below Trino's 2 MB limit.
ROWS_PER_STATEMENT = 10_000
MAX_STATEMENT_CHARS = 1_750_000

Row = Mapping[str, object]


@dataclass(frozen=True)
class ObjectRange:
    """Contiguous, deterministic row positions from one archived object."""

    source_object: str
    first_row_position: int
    last_row_position: int


@dataclass(frozen=True)
class InsertChunk:
    """One INSERT plus the exact source-object coordinates it replaces."""

    sql: str
    row_count: int
    ranges: tuple[ObjectRange, ...]


def varchar_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def timestamp_literal(value: datetime) -> str:
    moment = value if value.tzinfo else value.replace(tzinfo=UTC)
    return f"TIMESTAMP '{moment.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f} UTC'"


def date_literal(value: date) -> str:
    return f"DATE '{value:%Y-%m-%d}'"


def sql_literal(value: object, trino_type: str) -> str:
    """Render one Python value as a typed Trino SQL literal."""
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


def _chunk_ranges(rows: Sequence[Row]) -> tuple[ObjectRange, ...]:
    ranges: list[ObjectRange] = []
    for row in rows:
        source_object = row.get(SOURCE_OBJECT)
        position = row.get(SOURCE_OBJECT_ROW_POSITION)
        if (
            not isinstance(source_object, str)
            or isinstance(position, bool)
            or not isinstance(position, int)
        ):
            raise ValueError("INSERT chunks require deterministic source-object row positions")
        if (
            ranges
            and ranges[-1].source_object == source_object
            and position == ranges[-1].last_row_position + 1
        ):
            previous = ranges[-1]
            ranges[-1] = ObjectRange(source_object, previous.first_row_position, position)
        else:
            ranges.append(ObjectRange(source_object, position, position))
    return tuple(ranges)


def insert_chunks(
    spec: BronzeTableSpec,
    rows: Sequence[Row],
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
    rows_per_statement: int = ROWS_PER_STATEMENT,
    max_statement_chars: int = MAX_STATEMENT_CHARS,
) -> list[InsertChunk]:
    """Build bounded INSERT chunks with their exact replacement coordinates."""
    if rows_per_statement < 1:
        raise ValueError(f"rows_per_statement must be >= 1, got {rows_per_statement}")
    columns = spec.all_columns
    names = [column.name for column in columns]
    for row in rows:
        missing = [name for name in names if name not in row]
        if missing:
            raise ValueError(f"{spec.name}: row missing columns {missing}")

    header = (
        f"insert into {catalog}.{schema}.{spec.name} "
        f"({', '.join(f'"{name}"' for name in names)}) values "
    )
    chunks: list[InsertChunk] = []
    chunk_rows: list[Row] = []
    rendered_rows: list[str] = []
    chunk_chars = len(header)

    def flush() -> None:
        nonlocal chunk_rows, rendered_rows, chunk_chars
        if chunk_rows:
            chunks.append(
                InsertChunk(
                    sql=header + ", ".join(rendered_rows),
                    row_count=len(chunk_rows),
                    ranges=_chunk_ranges(chunk_rows),
                )
            )
            chunk_rows, rendered_rows, chunk_chars = [], [], len(header)

    for row in rows:
        rendered = (
            "("
            + ", ".join(
                sql_literal(row[name], column.trino_type)
                for name, column in zip(names, columns, strict=True)
            )
            + ")"
        )
        separator_chars = 2 if rendered_rows else 0
        if rendered_rows and (
            chunk_chars + separator_chars + len(rendered) > max_statement_chars
            or len(chunk_rows) >= rows_per_statement
        ):
            flush()
        chunk_rows.append(row)
        rendered_rows.append(rendered)
        chunk_chars += (2 if len(rendered_rows) > 1 else 0) + len(rendered)
    flush()
    return chunks


def insert_statements(
    spec: BronzeTableSpec,
    rows: Sequence[Row],
    *,
    catalog: str = "iceberg",
    schema: str = "bronze",
    rows_per_statement: int = ROWS_PER_STATEMENT,
    max_statement_chars: int = MAX_STATEMENT_CHARS,
) -> list[str]:
    """Backward-compatible SQL-only view of :func:`insert_chunks`."""
    return [
        chunk.sql
        for chunk in insert_chunks(
            spec,
            rows,
            catalog=catalog,
            schema=schema,
            rows_per_statement=rows_per_statement,
            max_statement_chars=max_statement_chars,
        )
    ]
