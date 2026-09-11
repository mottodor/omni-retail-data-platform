"""Declarative schemas for external file sources (CSV/JSON vendors)."""

from dataclasses import dataclass
from typing import Literal

FieldType = Literal["string", "integer", "decimal", "date", "boolean"]
FileFormat = Literal["csv", "json"]


@dataclass(frozen=True)
class FieldSpec:
    """One expected column of a source file."""

    name: str
    type: FieldType
    required: bool = True


@dataclass(frozen=True)
class FileSourceSchema:
    """Contract of one external file source.

    The schema drives both the vendor file generators and the ingestion
    validators, so a single definition describes "what the source sends"
    and "what the platform accepts".
    """

    name: str
    format: FileFormat
    schema_version: str
    fields: tuple[FieldSpec, ...]
    delimiter: str = ","


SUPPLIER_PRICES = FileSourceSchema(
    name="supplier-prices",
    format="csv",
    schema_version="1.0",
    fields=(
        FieldSpec("supplier_id", "string"),
        FieldSpec("sku", "string"),
        FieldSpec("price", "decimal"),
        FieldSpec("currency", "string"),
        FieldSpec("valid_from", "date"),
    ),
)

PARTNER_PRODUCTS = FileSourceSchema(
    name="partner-products",
    format="json",
    schema_version="1.0",
    fields=(
        FieldSpec("partner_id", "string"),
        FieldSpec("sku", "string"),
        FieldSpec("title", "string"),
        FieldSpec("brand", "string"),
        FieldSpec("category", "string"),
    ),
)

KNOWN_SOURCES: dict[str, FileSourceSchema] = {
    SUPPLIER_PRICES.name: SUPPLIER_PRICES,
    PARTNER_PRODUCTS.name: PARTNER_PRODUCTS,
}


def schema_by_name(name: str) -> FileSourceSchema:
    """Return the schema registered under ``name``."""
    try:
        return KNOWN_SOURCES[name]
    except KeyError as error:
        raise ValueError(f"unknown file source: {name!r}") from error
