"""Deterministic vendor file generation.

The generators produce realistic payloads that match the source schemas
defined in ``omni_retail.ingestion.files.schemas``. Given the same seed,
row count and run date, both the payload and the filename are reproducible,
so re-seeding and re-ingesting are fully deterministic.

Parquet is byte-deterministic by itself (typed columns, snappy, pinned
pyarrow). XLSX is not: ``openpyxl`` stamps the workbook ``modified``
property and every zip entry with the current time, so the payload is
normalized after saving — zip entry timestamps and the ``modified`` stamp
are rewritten to fixed values, keeping checksum-addressed deduplication
meaningful for Excel files.
"""

import csv
import io
import json
import random
import re
import zipfile
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from openpyxl import Workbook

from omni_retail.ingestion.common.paths import BUCKET_LANDING, incoming_key
from omni_retail.ingestion.common.storage import ObjectStorage
from omni_retail.ingestion.files.schemas import FieldSpec, FieldType, FileSourceSchema

SUPPLIERS = ("acme", "globex", "initech", "umbrella", "hooli")
CURRENCIES = ("EUR", "USD", "GBP")
PARTNERS = ("northwind", "contoso", "fabrikam", "adventure-works")
BRANDS = ("Acme", "Globex", "Initech", "Umbrella", "Hooli", "Vandelay")
CATEGORIES = ("tools", "outdoor", "kitchen", "electronics", "toys", "office")
TITLE_ADJECTIVES = ("Premium", "Classic", "Compact", "Deluxe", "Ergonomic", "Wireless")
TITLE_NOUNS = ("Widget", "Gadget", "Toolkit", "Backpack", "Lamp", "Speaker")
ORDER_STATUSES = ("pending", "paid", "shipped", "delivered", "cancelled")
VALID_FROM_BASE = date(2026, 1, 1)
VALID_FROM_SPAN_DAYS = 254

XLSX_EPOCH_TEXT = "2026-01-01T00:00:00Z"
XLSX_EPOCH_ZIP = (1980, 1, 1, 0, 0, 0)
_XLSX_MODIFIED_TAG = re.compile(rb"(<dcterms:modified[^>]*>)[^<]*(</dcterms:modified>)")


def generate_payload(schema: FileSourceSchema, *, seed: int, rows: int, run_date: date) -> bytes:
    """Generate ``rows`` deterministic records serialized in the schema format."""
    if rows < 1:
        raise ValueError("rows must be >= 1")
    rng = random.Random(f"{schema.name}|{seed}|{rows}|{run_date.isoformat()}")
    if schema.name == "supplier-prices":
        return _csv_payload(schema, _supplier_price_rows(rng, rows))
    if schema.name == "partner-products":
        return _json_payload(_partner_product_rows(rng, rows))
    if schema.name == "historical-orders":
        return _parquet_payload(schema, _historical_order_rows(rng, rows))
    if schema.name == "supplier-stock":
        return _xlsx_payload(schema, _supplier_stock_rows(rng, rows))
    raise ValueError(f"no generator registered for source: {schema.name!r}")


def make_filename(schema: FileSourceSchema, *, seed: int, rows: int, run_date: date) -> str:
    """Deterministic filename encoding the generation inputs."""
    return f"{schema.name}_{run_date:%Y%m%d}_s{seed}_n{rows}.{schema.format}"


def upload_payload(
    storage: ObjectStorage,
    schema: FileSourceSchema,
    payload: bytes,
    *,
    seed: int,
    rows: int,
    run_date: date,
) -> str:
    """Put the generated payload into the source drop zone; returns the object key."""
    key = incoming_key(schema.name, make_filename(schema, seed=seed, rows=rows, run_date=run_date))
    storage.put_object(BUCKET_LANDING, key, payload)
    return key


def _supplier_price_rows(rng: random.Random, rows: int) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for position in range(1, rows + 1):
        supplier = rng.choice(SUPPLIERS)
        price = Decimal(str(round(rng.uniform(0.5, 999.0), 2))).quantize(Decimal("0.01"))
        valid_from = VALID_FROM_BASE + timedelta(days=rng.randrange(VALID_FROM_SPAN_DAYS))
        records.append(
            {
                "supplier_id": supplier,
                "sku": f"{supplier[:2].upper()}-{position:05d}",
                "price": str(price),
                "currency": rng.choice(CURRENCIES),
                "valid_from": valid_from.isoformat(),
            }
        )
    return records


def _partner_product_rows(rng: random.Random, rows: int) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for position in range(1, rows + 1):
        partner = rng.choice(PARTNERS)
        records.append(
            {
                "partner_id": partner,
                "sku": f"{partner[:3].upper()}-{position:05d}",
                "title": f"{rng.choice(TITLE_ADJECTIVES)} {rng.choice(TITLE_NOUNS)} "
                f"{rng.randrange(100, 1000)}",
                "brand": rng.choice(BRANDS),
                "category": rng.choice(CATEGORIES),
            }
        )
    return records


def _historical_order_rows(rng: random.Random, rows: int) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for position in range(1, rows + 1):
        order_total = Decimal(str(round(rng.uniform(5.0, 2500.0), 2))).quantize(Decimal("0.01"))
        order_date = VALID_FROM_BASE + timedelta(days=rng.randrange(VALID_FROM_SPAN_DAYS))
        records.append(
            {
                "order_id": 100_000 + position,
                "customer_id": rng.randrange(1, 10_001),
                "status": rng.choice(ORDER_STATUSES),
                "order_total": order_total,
                "order_date": order_date,
            }
        )
    return records


def _supplier_stock_rows(rng: random.Random, rows: int) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for position in range(1, rows + 1):
        supplier = rng.choice(SUPPLIERS)
        records.append(
            {
                "supplier_id": supplier,
                "sku": f"{supplier[:2].upper()}-S{position:05d}",
                "quantity": rng.randrange(0, 5_000),
                "updated_at": VALID_FROM_BASE + timedelta(days=rng.randrange(VALID_FROM_SPAN_DAYS)),
            }
        )
    return records


def _csv_payload(schema: FileSourceSchema, records: list[dict[str, str]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=schema.delimiter)
    writer.writerow([field.name for field in schema.fields])
    for record in records:
        writer.writerow([record[field.name] for field in schema.fields])
    return buffer.getvalue().encode()


def _json_payload(records: list[dict[str, str]]) -> bytes:
    return json.dumps(records, sort_keys=True, separators=(",", ":")).encode()


_ARROW_FIELD_TYPES: dict[FieldType, pa.DataType] = {
    "string": pa.string(),
    "integer": pa.int64(),
    "decimal": pa.decimal128(12, 2),
    "date": pa.date32(),
    "boolean": pa.bool_(),
}


def _arrow_field(spec: FieldSpec) -> pa.Field:
    return pa.field(spec.name, _ARROW_FIELD_TYPES[spec.type], nullable=not spec.required)


def _parquet_payload(schema: FileSourceSchema, records: list[dict[str, Any]]) -> bytes:
    arrow_schema = pa.schema([_arrow_field(spec) for spec in schema.fields])
    table = pa.Table.from_pylist(records, schema=arrow_schema)
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="snappy")
    payload: bytes = sink.getvalue().to_pybytes()
    return payload


def _xlsx_payload(schema: FileSourceSchema, records: list[dict[str, Any]]) -> bytes:
    workbook = Workbook()
    workbook.properties.creator = "omni-retail-vendor-files-generator"
    workbook.properties.created = datetime(2026, 1, 1, tzinfo=UTC)
    sheet = workbook.active
    sheet.append([spec.name for spec in schema.fields])
    for record in records:
        sheet.append([record[spec.name] for spec in schema.fields])
    buffer = io.BytesIO()
    workbook.save(buffer)
    return _normalize_xlsx(buffer.getvalue())


def _normalize_xlsx(payload: bytes) -> bytes:
    """Rewrite volatile timestamps (zip entries, ``modified`` property) to fixed values."""
    source = io.BytesIO(payload)
    normalized = io.BytesIO()
    with (
        zipfile.ZipFile(source) as archive,
        zipfile.ZipFile(normalized, "w", zipfile.ZIP_DEFLATED) as stable,
    ):
        for name in archive.namelist():
            data = archive.read(name)
            if name == "docProps/core.xml":
                data = _XLSX_MODIFIED_TAG.sub(rf"\g<1>{XLSX_EPOCH_TEXT}\g<2>".encode(), data)
            entry = zipfile.ZipInfo(name, date_time=XLSX_EPOCH_ZIP)
            entry.compress_type = zipfile.ZIP_DEFLATED
            stable.writestr(entry, data)
    return normalized.getvalue()
