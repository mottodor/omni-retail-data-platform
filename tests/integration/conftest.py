"""Shared isolation fixtures for integration tests against the live stack.

The suite is opt-in via ``OMNI_INTEGRATION=1``. Tests borrow deterministic
object keys through an exact mutation journal and use disposable Iceberg
schemas, so a long-lived developer stack is restored after success or failure.
"""

# pyright: reportAttributeAccessIssue=false, reportMissingImports=false, reportMissingTypeStubs=false

import hashlib
import os
import uuid
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import pytest
import trino

from integration.namespace_ownership import ObjectMutationJournal
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE
from omni_retail.ingestion.common.storage import (
    BotoObjectStorage,
    StorageConfig,
)
from omni_retail.lakehouse.bronze.loader import (
    TrinoConfig,
    validate_schema_name,
)

INTEGRATION_ENV = "OMNI_INTEGRATION"
INTEGRATION_DIR = Path(__file__).parent


@dataclass(frozen=True)
class LakehouseNamespace:
    """Per-test Bronze/Silver/Gold/analytics schema names."""

    bronze: str
    silver: str
    gold: str
    analytics: str

    def dbt_env(self) -> dict[str, str]:
        return {
            "ICEBERG_BRONZE_SCHEMA": self.bronze,
            "DBT_BRONZE_SCHEMA": self.bronze,
            "DBT_SILVER_SCHEMA": self.silver,
            "DBT_GOLD_SCHEMA": self.gold,
            "DBT_ANALYTICS_SCHEMA": self.analytics,
        }

    def all_schemas(self) -> tuple[str, ...]:
        return (self.bronze, self.silver, self.gold, self.analytics)


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Skip integration tests unless the explicit env gate is set."""
    if os.environ.get(INTEGRATION_ENV) == "1":
        return
    skip = pytest.mark.skip(
        reason="integration tests need the live core stack: `make up` then `make integration`"
    )
    for item in items:
        if item.path.is_relative_to(INTEGRATION_DIR):
            item.add_marker(skip)


def archive_inventory(storage: BotoObjectStorage) -> dict[str, str]:
    """Return archive key -> SHA-256 without retaining object bodies."""
    return {
        key: hashlib.sha256(storage.get_object(BUCKET_ARCHIVE, key)).hexdigest()
        for key in storage.list_object_keys(BUCKET_ARCHIVE, "")
    }


def inventory_diff(before: dict[str, str], after: dict[str, str]) -> str:
    """Human-readable key-only checksum diff for a failed suite invariant."""
    before_keys = set(before)
    after_keys = set(after)
    added = sorted(after_keys - before_keys)
    removed = sorted(before_keys - after_keys)
    changed = sorted(key for key in before_keys & after_keys if before[key] != after[key])
    return f"added={added}; removed={removed}; changed={changed}"


@pytest.fixture(scope="session", autouse=True)
def preserve_archive_inventory() -> Generator[None, None, None]:
    """Assert that every archive object is byte-identical after all finalizers."""
    if os.environ.get(INTEGRATION_ENV) != "1":
        yield
        return
    storage = BotoObjectStorage(StorageConfig.from_env())
    before = archive_inventory(storage)
    yield
    after = archive_inventory(storage)
    assert after == before, "integration suite changed archive inventory: " + inventory_diff(
        before, after
    )


@pytest.fixture()
def live_storage() -> BotoObjectStorage:
    """Real MinIO-backed storage configured from the environment."""
    return BotoObjectStorage(StorageConfig.from_env())


@pytest.fixture()
def object_journal(
    live_storage: BotoObjectStorage,
) -> Generator[ObjectMutationJournal, None, None]:
    """Rollback every exact object mutation, including failed test setup."""
    journal = ObjectMutationJournal(live_storage)
    try:
        yield journal
    finally:
        journal.rollback()


def _execute_trino(sql: str) -> None:
    config = TrinoConfig.from_env()
    connection = trino.dbapi.connect(  # type: ignore[no-untyped-call]
        host=config.host,
        port=config.port,
        user=config.user,
        catalog=config.catalog,
    )
    try:
        connection.cursor().execute(sql)  # nosec B608 -- schema is regex-allowlisted
    finally:
        connection.close()


@pytest.fixture()
def lakehouse_namespace(request: pytest.FixtureRequest) -> LakehouseNamespace:
    """Create four UUID-prefixed schemas and drop only those schemas at teardown."""
    token = uuid.uuid4().hex[:12]
    namespace = LakehouseNamespace(
        bronze=f"it_{token}_bronze",
        silver=f"it_{token}_silver",
        gold=f"it_{token}_gold",
        analytics=f"it_{token}_analytics",
    )
    for schema in namespace.all_schemas():
        validate_schema_name(schema)

    def cleanup() -> None:
        errors: list[str] = []
        for schema in reversed(namespace.all_schemas()):
            try:
                _execute_trino(f"drop schema if exists iceberg.{schema} cascade")
            except Exception as error:  # try every disposable schema before failing teardown
                errors.append(f"{schema}: {error}")
        if errors:
            pytest.fail("failed to drop disposable lakehouse schemas: " + "; ".join(errors))

    request.addfinalizer(cleanup)
    for schema in namespace.all_schemas():
        _execute_trino(f"create schema if not exists iceberg.{schema}")
    return namespace
