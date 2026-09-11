"""Unit tests for file payload validation (CSV and JSON sources)."""

import csv
import io
import json

from omni_retail.ingestion.files.schemas import PARTNER_PRODUCTS, SUPPLIER_PRICES
from omni_retail.ingestion.files.validation import (
    serialize_bad_rows_csv,
    serialize_rejection_json,
    validate_payload,
)

CSV_HEADER = "supplier_id,sku,price,currency,valid_from\n"


def csv_payload(rows: list[str]) -> bytes:
    return (CSV_HEADER + "".join(rows)).encode()


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
