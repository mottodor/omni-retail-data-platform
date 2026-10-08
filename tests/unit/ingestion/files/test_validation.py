"""Unit tests for file payload validation (CSV, JSON, Parquet and XLSX sources)."""

import csv
import io
import json
from datetime import date
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
from openpyxl import Workbook

from omni_retail.ingestion.files.schemas import (
    HISTORICAL_ORDERS,
    PARTNER_PRODUCTS,
    SUPPLIER_PRICES,
    SUPPLIER_STOCK,
)
from omni_retail.ingestion.files.validation import (
    serialize_bad_rows_csv,
    serialize_rejection_json,
    validate_payload,
)

CSV_HEADER = "supplier_id,sku,price,currency,valid_from\n"


def csv_payload(rows: list[str]) -> bytes:
    return (CSV_HEADER + "".join(rows)).encode()


def parquet_payload(columns: dict[str, list[object]]) -> bytes:
    table = pa.table(
        {name: pa.array(values) for name, values in columns.items()},
        schema=pa.schema([pa.field(name, pa.string(), nullable=True) for name in columns]),
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    payload: bytes = sink.getvalue().to_pybytes()
    return payload


def xlsx_payload(rows: list[list[object]]) -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    assert sheet is not None
    for row in rows:
        sheet.append(row)
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def test_valid_csv_file_passes() -> None:
    payload = csv_payload(
        [
            "acme,SKU-1,9.99,EUR,2026-09-01\n",
            "acme,SKU-2,15.50,EUR,2026-09-01\n",
        ]
    )

    result = validate_payload(SUPPLIER_PRICES, payload)

    assert result.file_valid
    assert result.file_errors == ()
    assert result.row_count == 2
    assert result.bad_rows == ()
    assert result.rows[0] == {
        "supplier_id": "acme",
        "sku": "SKU-1",
        "price": "9.99",
        "currency": "EUR",
        "valid_from": "2026-09-01",
    }


def test_csv_missing_required_column_rejects_whole_file() -> None:
    payload = b"supplier_id,price,currency,valid_from\nacme,9.99,EUR,2026-09-01\n"

    result = validate_payload(SUPPLIER_PRICES, payload)

    assert not result.file_valid
    assert "missing required column: sku" in result.file_errors


def test_csv_unexpected_extra_column_rejects_whole_file() -> None:
    payload = b"supplier_id,sku,price,currency,valid_from,extra\nacme,SKU-1,9.99,EUR,2026-09-01,v\n"

    result = validate_payload(SUPPLIER_PRICES, payload)

    assert not result.file_valid
    assert any("unexpected column" in error for error in result.file_errors)


def test_csv_row_with_more_values_than_header_is_bad_row() -> None:
    payload = csv_payload(["acme,SKU-1,9.99,EUR,2026-09-01,extra\n"])

    result = validate_payload(SUPPLIER_PRICES, payload)

    assert result.file_valid
    assert len(result.bad_rows) == 1
    assert "more values" in result.bad_rows[0].reason


def test_csv_row_with_non_numeric_price_is_bad_row() -> None:
    payload = csv_payload(["acme,SKU-1,not-a-price,EUR,2026-09-01\n"])

    result = validate_payload(SUPPLIER_PRICES, payload)

    assert result.file_valid
    assert result.row_count == 1
    assert len(result.bad_rows) == 1
    assert "price" in result.bad_rows[0].reason


def test_csv_row_with_negative_price_is_bad_row() -> None:
    payload = csv_payload(["acme,SKU-1,-5.00,EUR,2026-09-01\n"])

    result = validate_payload(SUPPLIER_PRICES, payload)

    assert result.file_valid
    assert len(result.bad_rows) == 1
    assert "price" in result.bad_rows[0].reason


def test_csv_row_with_invalid_date_is_bad_row() -> None:
    payload = csv_payload(["acme,SKU-1,9.99,EUR,2026-13-01\n"])

    result = validate_payload(SUPPLIER_PRICES, payload)

    assert result.file_valid
    assert len(result.bad_rows) == 1
    assert "valid_from" in result.bad_rows[0].reason


def test_csv_row_with_empty_required_value_is_bad_row() -> None:
    payload = csv_payload(["acme,,9.99,EUR,2026-09-01\n"])

    result = validate_payload(SUPPLIER_PRICES, payload)

    assert result.file_valid
    assert len(result.bad_rows) == 1
    assert "sku" in result.bad_rows[0].reason


def test_csv_with_header_only_is_valid_with_zero_rows() -> None:
    result = validate_payload(SUPPLIER_PRICES, csv_payload([]))

    assert result.file_valid
    assert result.row_count == 0


def test_empty_csv_payload_is_invalid_file() -> None:
    result = validate_payload(SUPPLIER_PRICES, b"")

    assert not result.file_valid


def test_mixed_valid_and_bad_rows_keep_good_rows() -> None:
    payload = csv_payload(
        [
            "acme,SKU-1,9.99,EUR,2026-09-01\n",
            "acme,SKU-2,free,EUR,2026-09-01\n",
            "acme,SKU-3,25.00,EUR,2026-09-02\n",
        ]
    )

    result = validate_payload(SUPPLIER_PRICES, payload)

    assert result.file_valid
    assert result.row_count == 3
    assert [row["sku"] for row in result.rows] == ["SKU-1", "SKU-3"]
    assert [row.row_position for row in result.accepted_rows] == [0, 2]
    assert [bad.row_number for bad in result.bad_rows] == [2]


def test_valid_json_file_passes() -> None:
    payload = json.dumps(
        [
            {
                "partner_id": "p1",
                "sku": "SKU-1",
                "title": "Widget",
                "brand": "Acme",
                "category": "tools",
            },
        ]
    ).encode()

    result = validate_payload(PARTNER_PRODUCTS, payload)

    assert result.file_valid
    assert result.row_count == 1
    assert result.rows[0]["sku"] == "SKU-1"


def test_malformed_json_rejects_whole_file() -> None:
    result = validate_payload(PARTNER_PRODUCTS, b"{not json")

    assert not result.file_valid
    assert any("malformed" in error for error in result.file_errors)


def test_json_root_must_be_array() -> None:
    result = validate_payload(PARTNER_PRODUCTS, json.dumps({"sku": "SKU-1"}).encode())

    assert not result.file_valid
    assert any("array" in error for error in result.file_errors)


def test_json_row_with_missing_field_is_bad_row() -> None:
    payload = json.dumps(
        [{"partner_id": "p1", "sku": "SKU-1", "title": "Widget", "brand": "Acme"}]
    ).encode()

    result = validate_payload(PARTNER_PRODUCTS, payload)

    assert result.file_valid
    assert len(result.bad_rows) == 1
    assert "category" in result.bad_rows[0].reason


def test_json_row_with_non_string_value_is_bad_row() -> None:
    payload = json.dumps(
        [{"partner_id": "p1", "sku": 17, "title": "Widget", "brand": "Acme", "category": "tools"}]
    ).encode()

    result = validate_payload(PARTNER_PRODUCTS, payload)

    assert result.file_valid
    assert len(result.bad_rows) == 1
    assert "sku" in result.bad_rows[0].reason


def test_bad_rows_csv_contains_original_values_and_reason() -> None:
    payload = csv_payload(
        [
            "acme,SKU-1,9.99,EUR,2026-09-01\n",
            "acme,SKU-2,broken,EUR,2026-09-01\n",
        ]
    )
    result = validate_payload(SUPPLIER_PRICES, payload)

    bad_rows_payload = serialize_bad_rows_csv(SUPPLIER_PRICES, result.bad_rows)
    reader = csv.DictReader(io.StringIO(bad_rows_payload.decode()))

    quarantined = list(reader)
    assert len(quarantined) == 1
    assert quarantined[0]["sku"] == "SKU-2"
    assert quarantined[0]["price"] == "broken"
    assert quarantined[0]["_rejection_reason"]


def test_rejection_json_contains_reasons() -> None:
    payload = b"supplier_id,price\nacme,9.99\n"
    result = validate_payload(SUPPLIER_PRICES, payload)

    reasons = json.loads(serialize_rejection_json(result.file_errors).decode())

    assert isinstance(reasons, list)
    assert all(isinstance(reason, str) for reason in reasons)
    assert len(reasons) >= 1


def test_valid_parquet_file_passes() -> None:
    payload = parquet_payload(
        {
            "order_id": ["100001", "100002"],
            "customer_id": ["7052", "123"],
            "status": ["paid", "shipped"],
            "order_total": ["17.00", "250.50"],
            "order_date": ["2026-04-20", "2026-04-21"],
        }
    )

    result = validate_payload(HISTORICAL_ORDERS, payload)

    assert result.file_valid
    assert result.row_count == 2
    assert result.rows[0] == {
        "order_id": "100001",
        "customer_id": "7052",
        "status": "paid",
        "order_total": "17.00",
        "order_date": "2026-04-20",
    }


def test_malformed_parquet_rejects_whole_file() -> None:
    result = validate_payload(HISTORICAL_ORDERS, b"definitely not a parquet file")

    assert not result.file_valid
    assert any("malformed parquet" in error for error in result.file_errors)


def test_parquet_missing_required_column_rejects_whole_file() -> None:
    payload = parquet_payload({"order_id": ["100001"], "customer_id": ["7052"]})

    result = validate_payload(HISTORICAL_ORDERS, payload)

    assert not result.file_valid
    assert "missing required column: status" in result.file_errors


def test_parquet_row_with_null_required_value_is_bad_row() -> None:
    payload = parquet_payload(
        {
            "order_id": ["100001", "100002"],
            "customer_id": ["7052", "123"],
            "status": ["paid", None],
            "order_total": ["17.00", "20.00"],
            "order_date": ["2026-04-20", "2026-04-21"],
        }
    )

    result = validate_payload(HISTORICAL_ORDERS, payload)

    assert result.file_valid
    assert result.row_count == 2
    assert len(result.bad_rows) == 1
    assert "status" in result.bad_rows[0].reason


def test_parquet_row_with_non_numeric_total_is_bad_row() -> None:
    payload = parquet_payload(
        {
            "order_id": ["100001"],
            "customer_id": ["7052"],
            "status": ["paid"],
            "order_total": ["free"],
            "order_date": ["2026-04-20"],
        }
    )

    result = validate_payload(HISTORICAL_ORDERS, payload)

    assert result.file_valid
    assert len(result.bad_rows) == 1
    assert "order_total" in result.bad_rows[0].reason


def test_valid_xlsx_file_passes() -> None:
    payload = xlsx_payload(
        [
            ["supplier_id", "sku", "quantity", "updated_at"],
            ["acme", "AC-S0001", 42, date(2026, 3, 5)],
            ["globex", "GL-S0002", 0, date(2026, 3, 6)],
        ]
    )

    result = validate_payload(SUPPLIER_STOCK, payload)

    assert result.file_valid
    assert result.row_count == 2
    assert result.rows[0] == {
        "supplier_id": "acme",
        "sku": "AC-S0001",
        "quantity": "42",
        "updated_at": "2026-03-05",
    }


def test_malformed_xlsx_rejects_whole_file() -> None:
    result = validate_payload(SUPPLIER_STOCK, b"not a zip archive")

    assert not result.file_valid
    assert any("malformed xlsx" in error for error in result.file_errors)


def test_xlsx_missing_required_column_rejects_whole_file() -> None:
    payload = xlsx_payload([["supplier_id", "sku"], ["acme", "AC-S0001"]])

    result = validate_payload(SUPPLIER_STOCK, payload)

    assert not result.file_valid
    assert "missing required column: quantity" in result.file_errors


def test_xlsx_header_only_is_valid_with_zero_rows() -> None:
    payload = xlsx_payload([["supplier_id", "sku", "quantity", "updated_at"]])

    result = validate_payload(SUPPLIER_STOCK, payload)

    assert result.file_valid
    assert result.row_count == 0


def test_xlsx_row_with_non_numeric_quantity_is_bad_row() -> None:
    payload = xlsx_payload(
        [
            ["supplier_id", "sku", "quantity", "updated_at"],
            ["acme", "AC-S0001", "many", date(2026, 3, 5)],
        ]
    )

    result = validate_payload(SUPPLIER_STOCK, payload)

    assert result.file_valid
    assert len(result.bad_rows) == 1
    assert "quantity" in result.bad_rows[0].reason


def test_xlsx_row_with_empty_required_value_is_bad_row() -> None:
    payload = xlsx_payload(
        [
            ["supplier_id", "sku", "quantity", "updated_at"],
            ["acme", None, 42, date(2026, 3, 5)],
        ]
    )

    result = validate_payload(SUPPLIER_STOCK, payload)

    assert result.file_valid
    assert len(result.bad_rows) == 1
    assert "sku" in result.bad_rows[0].reason


def test_decimal_values_survive_string_coercion() -> None:
    assert Decimal("17.00") >= 0  # sanity: coercion target stays parseable
    payload = parquet_payload(
        {
            "order_id": ["100001"],
            "customer_id": ["7052"],
            "status": ["paid"],
            "order_total": [str(Decimal("17.00"))],
            "order_date": ["2026-04-20"],
        }
    )

    result = validate_payload(HISTORICAL_ORDERS, payload)

    assert result.file_valid
    assert result.bad_rows == ()
