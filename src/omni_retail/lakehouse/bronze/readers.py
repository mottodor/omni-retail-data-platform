"""Raw-object readers: PG snapshot Parquet and API JSON pages (Phase 5 spec §4).

Readers are strict on purpose: a raw object that does not match the Bronze
contract (column names for Parquet, envelope shape and field types for JSON)
raises :class:`BronzeReadError` before any partition is modified.
"""

import json
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE
from omni_retail.ingestion.common.storage import ObjectStorage
from omni_retail.lakehouse.bronze.specs import BronzeColumnSpec, BronzeTableSpec

Row = dict[str, object]


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
    return read_json_rows(spec, object_key, body)


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
    except (ValueError, BronzeReadError) as error:
        raise BronzeReadError(
            f"{object_key}: {column.name} cannot be coerced to {family}: {error}"
        ) from error
    raise BronzeReadError(f"{object_key}: {column.name} unsupported family {family}")
