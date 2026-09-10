"""Guard tests for the OLTP DDL file."""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
DDL_PATH = REPO_ROOT / "postgres" / "init" / "01_oltp_schema.sql"

EXPECTED_TABLES = (
    "categories",
    "products",
    "customers",
    "orders",
    "order_items",
    "payments",
    "shipments",
)


def test_ddl_file_exists() -> None:
    assert DDL_PATH.is_file(), f"missing OLTP DDL: {DDL_PATH}"


def test_ddl_is_idempotent() -> None:
    sql = DDL_PATH.read_text()

    create_statements = re.findall(r"CREATE (?:TABLE|INDEX) ([^;\n]+)", sql)
    assert create_statements, "no CREATE statements found"
    for statement in create_statements:
        assert "IF NOT EXISTS" in statement, f"not idempotent: CREATE {statement.strip()}"


def test_ddl_covers_all_phase2_tables() -> None:
    sql = DDL_PATH.read_text()

    for table in EXPECTED_TABLES:
        assert re.search(rf"CREATE TABLE IF NOT EXISTS {table} \(", sql), table


def test_ddl_declares_transaction() -> None:
    sql = DDL_PATH.read_text()

    assert "BEGIN;" in sql
    assert "COMMIT;" in sql
