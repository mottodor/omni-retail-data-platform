"""Unit tests for declarative file source schemas."""

import pytest

from omni_retail.ingestion.files.schemas import (
    PARTNER_PRODUCTS,
    SUPPLIER_PRICES,
    schema_by_name,
)


def test_supplier_prices_schema_shape() -> None:
    assert SUPPLIER_PRICES.name == "supplier-prices"
    assert SUPPLIER_PRICES.format == "csv"
    assert SUPPLIER_PRICES.delimiter == ","
    assert SUPPLIER_PRICES.schema_version == "1.0"
    field_names = tuple(field.name for field in SUPPLIER_PRICES.fields)
    assert field_names == ("supplier_id", "sku", "price", "currency", "valid_from")


def test_partner_products_schema_shape() -> None:
    assert PARTNER_PRODUCTS.name == "partner-products"
    assert PARTNER_PRODUCTS.format == "json"
    field_names = tuple(field.name for field in PARTNER_PRODUCTS.fields)
    assert field_names == ("partner_id", "sku", "title", "brand", "category")


def test_all_fields_have_explicit_types() -> None:
    for schema in (SUPPLIER_PRICES, PARTNER_PRODUCTS):
        for field in schema.fields:
            assert field.type in {"string", "integer", "decimal", "date", "boolean"}
            assert field.required is True


def test_schema_by_name_returns_known_sources() -> None:
    assert schema_by_name("supplier-prices") is SUPPLIER_PRICES
    assert schema_by_name("partner-products") is PARTNER_PRODUCTS


def test_schema_by_name_rejects_unknown_source() -> None:
    with pytest.raises(ValueError, match="unknown file source"):
        schema_by_name("does-not-exist")
