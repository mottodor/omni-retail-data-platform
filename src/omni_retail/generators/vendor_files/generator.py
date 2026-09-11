"""Deterministic vendor file generation.

The generators produce realistic payloads that match the source schemas
defined in ``omni_retail.ingestion.files.schemas``. Given the same seed,
row count and run date, both the payload and the filename are reproducible,
so re-seeding and re-ingesting are fully deterministic.
"""

import csv
import io
import json
import random
from datetime import date, timedelta
from decimal import Decimal

from omni_retail.ingestion.common.paths import BUCKET_LANDING, incoming_key
from omni_retail.ingestion.common.storage import ObjectStorage
from omni_retail.ingestion.files.schemas import FileSourceSchema

SUPPLIERS = ("acme", "globex", "initech", "umbrella", "hooli")
CURRENCIES = ("EUR", "USD", "GBP")
PARTNERS = ("northwind", "contoso", "fabrikam", "adventure-works")
BRANDS = ("Acme", "Globex", "Initech", "Umbrella", "Hooli", "Vandelay")
CATEGORIES = ("tools", "outdoor", "kitchen", "electronics", "toys", "office")
TITLE_ADJECTIVES = ("Premium", "Classic", "Compact", "Deluxe", "Ergonomic", "Wireless")
TITLE_NOUNS = ("Widget", "Gadget", "Toolkit", "Backpack", "Lamp", "Speaker")
VALID_FROM_BASE = date(2026, 1, 1)
VALID_FROM_SPAN_DAYS = 254


def generate_payload(schema: FileSourceSchema, *, seed: int, rows: int, run_date: date) -> bytes:
    """Generate ``rows`` deterministic records serialized in the schema format."""
    if rows < 1:
        raise ValueError("rows must be >= 1")
    rng = random.Random(f"{schema.name}|{seed}|{rows}|{run_date.isoformat()}")
    if schema.name == "supplier-prices":
        return _csv_payload(schema, _supplier_price_rows(rng, rows))
    if schema.name == "partner-products":
        return _json_payload(_partner_product_rows(rng, rows))
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


def _csv_payload(schema: FileSourceSchema, records: list[dict[str, str]]) -> bytes:
    buffer = io.StringIO()
    writer = csv.writer(buffer, delimiter=schema.delimiter)
    writer.writerow([field.name for field in schema.fields])
    for record in records:
        writer.writerow([record[field.name] for field in schema.fields])
    return buffer.getvalue().encode()


def _json_payload(records: list[dict[str, str]]) -> bytes:
    return json.dumps(records, sort_keys=True, separators=(",", ":")).encode()
