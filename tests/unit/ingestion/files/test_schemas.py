"""Unit tests for declarative file source schemas."""

import pytest

from omni_retail.ingestion.files.schemas import (
    HISTORICAL_ORDERS,
    PARTNER_PRODUCTS,
    SUPPLIER_PRICES,
    SUPPLIER_STOCK,
    schema_by_name,
)

ALL_SCHEMAS = (SUPPLIER_PRICES, PARTNER_PRODUCTS, HISTORICAL_ORDERS, SUPPLIER_STOCK)


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


def test_historical_orders_schema_shape() -> None:
    assert HISTORICAL_ORDERS.name == "historical-orders"
    assert HISTORICAL_ORDERS.format == "parquet"
    field_names = tuple(field.name for field in HISTORICAL_ORDERS.fields)
    assert field_names == ("order_id", "customer_id", "status", "order_total", "order_date")


def test_supplier_stock_schema_shape() -> None:
    assert SUPPLIER_STOCK.name == "supplier-stock"
    assert SUPPLIER_STOCK.format == "xlsx"
    field_names = tuple(field.name for field in SUPPLIER_STOCK.fields)
    assert field_names == ("supplier_id", "sku", "quantity", "updated_at")


def test_all_fields_have_explicit_types() -> None:
    for schema in ALL_SCHEMAS:
        for field in schema.fields:
            assert field.type in {"string", "integer", "decimal", "date", "boolean"}
            assert field.required is True


def test_schema_by_name_returns_known_sources() -> None:
    assert schema_by_name("supplier-prices") is SUPPLIER_PRICES
    assert schema_by_name("partner-products") is PARTNER_PRODUCTS
    assert schema_by_name("historical-orders") is HISTORICAL_ORDERS
    assert schema_by_name("supplier-stock") is SUPPLIER_STOCK


def test_schema_by_name_rejects_unknown_source() -> None:
    with pytest.raises(ValueError, match="unknown file source"):
        schema_by_name("does-not-exist")
