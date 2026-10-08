"""Strict raw-object readers for snapshots, API pages, and supplier files.

A raw object that does not match its Bronze/source contract raises
:class:`BronzeReadError` before any partition is modified. Supplier readers
reuse the ingestion validator so CSV, JSON, Parquet, and XLSX acceptance
semantics cannot drift between archive and Bronze.
"""

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE
from omni_retail.ingestion.common.storage import ObjectStorage
from omni_retail.ingestion.files.schemas import schema_by_name
from omni_retail.ingestion.files.validation import validate_payload
from omni_retail.lakehouse.bronze.specs import BronzeColumnSpec, BronzeTableSpec

Row = dict[str, object]


@dataclass(frozen=True)
class PositionedRow:
    """One typed raw record with its stable zero-based object coordinate."""

    row_position: int
    values: Row


@dataclass(frozen=True)
class FileReadResult:
    """Typed accepted rows plus source validation counts for one file object."""

    rows: tuple[PositionedRow, ...]
    row_count: int
    rejected_row_count: int


class BronzeReadError(Exception):
    """Raw object does not match the Bronze contract (schema drift / bad envelope)."""


def list_data_objects(
    storage: ObjectStorage, spec: BronzeTableSpec, logical_date: date
) -> tuple[str, ...]:
    """Sorted raw data-object keys of the logical date (manifests excluded by suffix)."""
    keys = storage.list_object_keys(BUCKET_ARCHIVE, spec.object_prefix(logical_date))
    suffix = "." + spec.data_object_suffix
    return tuple(key for key in keys if key.endswith(suffix))


def read_rows(spec: BronzeTableSpec, object_key: str, body: bytes) -> list[Row]:
    """Dispatch to the raw-format reader declared by the spec kind."""
    if spec.kind == "postgres":
        return read_parquet_rows(spec, object_key, body)
    if spec.kind == "api":
        return read_json_rows(spec, object_key, body)
    return [row.values for row in read_file_rows(spec, object_key, body).rows]


def read_file_rows(spec: BronzeTableSpec, object_key: str, body: bytes) -> FileReadResult:
    """Validate one archived vendor file and type only its accepted source rows."""
    try:
        schema = schema_by_name(spec.source_name)
    except ValueError as error:
        raise BronzeReadError(f"{object_key}: {error}") from error
    if spec.kind != "file" or spec.file_format != schema.format:
        raise BronzeReadError(
            f"{object_key}: file spec mismatch for {spec.source_name} "
            f"(kind={spec.kind}, format={spec.file_format}, contract={schema.format})"
        )
    expected = [field.name for field in schema.fields]
    actual = [column.name for column in spec.columns]
    if actual != expected:
        raise BronzeReadError(
            f"{object_key}: Bronze columns drifted from file contract "
            f"(expected={expected}, actual={actual})"
        )

    result = validate_payload(schema, body)
    if not result.file_valid:
        raise BronzeReadError(
            f"{object_key}: archived file no longer satisfies schema {schema.schema_version}: "
            + "; ".join(result.file_errors)
        )

    rows: list[PositionedRow] = []
    for accepted in result.accepted_rows:
        values: Row = {}
        for column in spec.columns:
            values[column.name] = _coerce_file(
                object_key, accepted.row_position, column, accepted.values[column.name]
            )
        rows.append(PositionedRow(accepted.row_position, values))
    return FileReadResult(tuple(rows), result.row_count, result.rejected_row_count)


def read_parquet_rows(spec: BronzeTableSpec, object_key: str, body: bytes) -> list[Row]:
    """Read a PG-snapshot Parquet object; column set must equal the spec exactly."""
    table = pq.read_table(pa.BufferReader(body))
    expected = {column.name for column in spec.columns}
    actual = set(table.column_names)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise BronzeReadError(f"{object_key}: schema drift (missing={missing}, extra={extra})")
    rows: list[Row] = []
    for record in table.to_pylist():
        rows.append(
            {
                column.name: _validate_native(object_key, column, record[column.name])
                for column in spec.columns
            }
        )
    return rows


def read_json_rows(spec: BronzeTableSpec, object_key: str, body: bytes) -> list[Row]:
    """Read one raw API page and flatten its envelope into typed rows."""
    try:
        payload: Any = json.loads(body)
    except json.JSONDecodeError as error:
        raise BronzeReadError(f"{object_key}: invalid JSON: {error}") from error
    envelope = payload.get(spec.envelope_field) if isinstance(payload, dict) else None
    if not isinstance(envelope, list):
        field = spec.envelope_field
        raise BronzeReadError(f"{object_key}: envelope field {field!r} must be a list")

    rows: list[Row] = []
    for position, record in enumerate(envelope):
        if not isinstance(record, dict):
            raise BronzeReadError(f"{object_key}: row {position} is not an object")
        row: Row = {}
        for column in spec.columns:
            if column.name not in record:
                raise BronzeReadError(f"{object_key}: row {position} missing field {column.name!r}")
            row[column.name] = _coerce_json(object_key, column, record[column.name])
        rows.append(row)
    return rows


def _type_family(trino_type: str) -> str:
    if trino_type.startswith("varchar"):
        return "varchar"
    if trino_type.startswith("decimal"):
        return "decimal"
    if trino_type.startswith("timestamp"):
        return "timestamp"
    known = {
        "bigint": "bigint",
        "integer": "bigint",
        "boolean": "boolean",
        "date": "date",
        "double": "double",
    }
    family = known.get(trino_type)
    if family is None:
        raise BronzeReadError(f"unsupported trino type in bronze spec: {trino_type!r}")
    return family


def _check_nullable(object_key: str, column: BronzeColumnSpec, value: object) -> None:
    if value is None and not column.nullable:
        raise BronzeReadError(f"{object_key}: {column.name} is null but not nullable")


def _validate_native(object_key: str, column: BronzeColumnSpec, value: object) -> object:
    """Parquet values already carry types; validate the Python-side family."""
    _check_nullable(object_key, column, value)
    if value is None:
        return None
    family = _type_family(column.trino_type)
    valid = (
        (family == "varchar" and isinstance(value, str))
        or (family == "bigint" and isinstance(value, int) and not isinstance(value, bool))
        or (family == "boolean" and isinstance(value, bool))
        or (family == "decimal" and isinstance(value, Decimal))
        or (family == "timestamp" and isinstance(value, datetime))
        or (family == "date" and isinstance(value, date) and not isinstance(value, datetime))
        or (family == "double" and isinstance(value, float))
    )
    if not valid:
        raise BronzeReadError(
            f"{object_key}: {column.name} expects {family}, got {type(value).__name__}"
        )
    return value


def _coerce_file(
    object_key: str,
    row_position: int,
    column: BronzeColumnSpec,
    value: str,
) -> object:
    """Convert one validator-canonical string into its Bronze Python type."""
    family = _type_family(column.trino_type)
    try:
        if family == "varchar":
            return value
        if family == "bigint":
            return int(value)
        if family == "decimal":
            return Decimal(value)
        if family == "date":
            return date.fromisoformat(value)
        if family == "boolean":
            lowered = value.lower()
            if lowered not in {"true", "false"}:
                raise ValueError("expected true or false")
            return lowered == "true"
        if family == "timestamp":
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        if family == "double":
            return float(value)
    except (ValueError, ArithmeticError, InvalidOperation) as error:
        raise BronzeReadError(
            f"{object_key}: row {row_position} column {column.name!r} "
            f"cannot be coerced to {family}: {error}"
        ) from error
    raise BronzeReadError(
        f"{object_key}: row {row_position} column {column.name!r} uses unsupported family {family}"
    )


def _coerce_json(object_key: str, column: BronzeColumnSpec, value: object) -> object:
    """Coerce one JSON value into the Python type of the column's Trino type."""
    _check_nullable(object_key, column, value)
    if value is None:
        return None
    family = _type_family(column.trino_type)
    try:
        if family == "varchar":
            if not isinstance(value, str):
                raise BronzeReadError("expected str")
            return value
        if family == "bigint":
            if isinstance(value, bool) or not isinstance(value, int):
                raise BronzeReadError("expected int")
            return value
        if family == "boolean":
            if not isinstance(value, bool):
                raise BronzeReadError("expected bool")
            return value
        if family == "decimal":
            if isinstance(value, bool) or not isinstance(value, (int, float, str)):
                raise BronzeReadError("expected number")
            return Decimal(str(value))
        if family == "timestamp":
            if not isinstance(value, str):
                raise BronzeReadError("expected ISO-8601 string")
            parsed = datetime.fromisoformat(value)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        if family == "date":
            if not isinstance(value, str):
                raise BronzeReadError("expected ISO-8601 date string")
            return date.fromisoformat(value)
        if family == "double":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise BronzeReadError("expected number")
            return float(value)
    except (ValueError, ArithmeticError, BronzeReadError) as error:
        raise BronzeReadError(
            f"{object_key}: {column.name} cannot be coerced to {family}: {error}"
        ) from error
    raise BronzeReadError(f"{object_key}: {column.name} unsupported family {family}")
