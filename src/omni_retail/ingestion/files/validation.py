"""Validation of file payloads: structural (file-level) and row-level checks."""

import csv
import io
import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

from omni_retail.ingestion.files.schemas import FieldSpec, FileSourceSchema

BAD_ROWS_REASON_COLUMN = "_rejection_reason"


@dataclass(frozen=True)
class BadRow:
    """One malformed row quarantined with the reason it was rejected."""

    row_number: int
    values: dict[str, str]
    reason: str


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of validating one file payload against its source schema."""

    file_valid: bool
    file_errors: tuple[str, ...] = ()
    rows: tuple[dict[str, str], ...] = ()
    bad_rows: tuple[BadRow, ...] = ()

    @property
    def row_count(self) -> int:
        return len(self.rows) + len(self.bad_rows)

    @property
    def rejected_row_count(self) -> int:
        return len(self.bad_rows)


def validate_payload(schema: FileSourceSchema, payload: bytes) -> ValidationResult:
    """Validate a raw payload; never raises on malformed content."""
    if schema.format == "csv":
        return _validate_csv(schema, payload)
    return _validate_json(schema, payload)


def serialize_bad_rows_csv(schema: FileSourceSchema, bad_rows: tuple[BadRow, ...]) -> bytes:
    """Render quarantined rows as CSV with an extra reason column."""
    if not bad_rows:
        return b""
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=schema.delimiter)
    writer.writerow([spec.name for spec in schema.fields] + [BAD_ROWS_REASON_COLUMN])
    for bad_row in bad_rows:
        writer.writerow(
            [bad_row.values.get(spec.name, "") for spec in schema.fields] + [bad_row.reason]
        )
    return buffer.getvalue().encode()


def serialize_rejection_json(file_errors: tuple[str, ...]) -> bytes:
    """Render whole-file rejection reasons as a JSON sidecar payload."""
    return json.dumps(list(file_errors), sort_keys=True).encode()


def _validate_csv(schema: FileSourceSchema, payload: bytes) -> ValidationResult:
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        return ValidationResult(file_valid=False, file_errors=(f"malformed utf-8: {error}",))
    reader = csv.DictReader(io.StringIO(text), delimiter=schema.delimiter)
    if reader.fieldnames is None:
        return ValidationResult(file_valid=False, file_errors=("empty file: no header",))

    file_errors = _header_errors(schema, set(reader.fieldnames))
    if file_errors:
        return ValidationResult(file_valid=False, file_errors=file_errors)

    rows: list[dict[str, str]] = []
    bad_rows: list[BadRow] = []
    for row_number, raw_row in enumerate(reader, start=1):
        if None in raw_row:
            bad_rows.append(
                BadRow(
                    row_number=row_number,
                    values=dict(raw_row),
                    reason="row has more values than header columns",
                )
            )
            continue
        reason = _row_error(schema, raw_row)
        if reason is None:
            rows.append(dict(raw_row))
        else:
            bad_rows.append(BadRow(row_number=row_number, values=dict(raw_row), reason=reason))
    return ValidationResult(file_valid=True, rows=tuple(rows), bad_rows=tuple(bad_rows))


def _validate_json(schema: FileSourceSchema, payload: bytes) -> ValidationResult:
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        return ValidationResult(file_valid=False, file_errors=(f"malformed json: {error}",))
    if not isinstance(document, list):
        return ValidationResult(
            file_valid=False,
            file_errors=("malformed json: root element must be an array of records",),
        )

    rows: list[dict[str, str]] = []
    bad_rows: list[BadRow] = []
    for row_number, record in enumerate(document, start=1):
        if not isinstance(record, dict):
            bad_rows.append(
                BadRow(
                    row_number=row_number,
                    values={},
                    reason="record is not a json object",
                )
            )
            continue
        reason = _json_row_error(schema, record)
        if reason is None:
            rows.append({name: str(value) for name, value in record.items()})
        else:
            bad_rows.append(
                BadRow(
                    row_number=row_number,
                    values={name: str(value) for name, value in record.items()},
                    reason=reason,
                )
            )
    return ValidationResult(file_valid=True, rows=tuple(rows), bad_rows=tuple(bad_rows))


def _header_errors(schema: FileSourceSchema, header: set[str]) -> tuple[str, ...]:
    expected = {spec.name for spec in schema.fields}
    missing = sorted(expected - header)
    unexpected = sorted(header - expected)
    errors = [f"missing required column: {name}" for name in missing]
    errors.extend(f"unexpected column: {name}" for name in unexpected)
    return tuple(errors)


def _row_error(schema: FileSourceSchema, raw_row: dict[str, Any]) -> str | None:
    reasons = [
        reason for reason in (_field_error(spec, raw_row) for spec in schema.fields) if reason
    ]
    if not reasons:
        return None
    return "; ".join(reasons)


def _json_row_error(schema: FileSourceSchema, record: dict[str, Any]) -> str | None:
    reasons = [
        reason for reason in (_field_error(spec, record) for spec in schema.fields) if reason
    ]
    if not reasons:
        return None
    return "; ".join(reasons)


def _field_error(spec: FieldSpec, raw_row: dict[str, Any]) -> str | None:
    value = raw_row.get(spec.name)
    if value is None or (isinstance(value, str) and not value.strip()):
        return f"{spec.name}: required value is missing" if spec.required else None
    if not _value_matches_type(spec.type, value):
        return f"{spec.name}: value {value!r} does not match type {spec.type}"
    return None


def _value_matches_type(field_type: str, value: Any) -> bool:
    if field_type == "string":
        return isinstance(value, str)
    if field_type == "integer":
        try:
            int(value)
        except (TypeError, ValueError):
            return False
        return True
    if field_type == "decimal":
        try:
            amount = Decimal(str(value))
        except (InvalidOperation, ValueError):
            return False
        return amount >= 0
    if field_type == "date":
        try:
            date.fromisoformat(str(value))
        except ValueError:
            return False
        return True
    if field_type == "boolean":
        return str(value).lower() in {"true", "false"}
    raise ValueError(f"unsupported field type: {field_type!r}")
