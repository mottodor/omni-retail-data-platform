"""Shared fixtures for integration tests against the live core stack.

These tests run only when ``OMNI_INTEGRATION=1`` is set (see ``make
integration``) and require the core Docker Compose profile to be up:
MinIO (landing/archive/rejected buckets) and the mock external API.

The fixtures purge the object namespaces of the sources used by each test
(before, for hermetic start, and after, to leave local state clean), so a
test never depends on — or destroys — data of other sources.
"""

import os
from collections.abc import Callable, Generator
from pathlib import Path

import pytest

from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    BUCKET_LANDING,
    BUCKET_REJECTED,
)
from omni_retail.ingestion.common.storage import BotoObjectStorage, StorageConfig

INTEGRATION_ENV = "OMNI_INTEGRATION"
INTEGRATION_DIR = Path(__file__).parent


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


def purge_source(storage: BotoObjectStorage, source: str) -> None:
    """Delete every object belonging to one source namespace (file and API keys)."""
    for bucket, prefix in (
        (BUCKET_LANDING, f"{source}/"),
        (BUCKET_ARCHIVE, f"{source}/"),
        (BUCKET_ARCHIVE, f"_manifests/{source}/"),
        (BUCKET_ARCHIVE, f"_dedup/{source}/"),
        (BUCKET_ARCHIVE, f"api/{source}/"),
        (BUCKET_REJECTED, f"{source}/"),
    ):
        for key in storage.list_object_keys(bucket, prefix):
            storage.delete_object(bucket, key)


@pytest.fixture()
def live_storage() -> BotoObjectStorage:
    """Real MinIO-backed storage configured from the environment."""
    return BotoObjectStorage(StorageConfig.from_env())


@pytest.fixture()
def purged_sources(
    live_storage: BotoObjectStorage,
) -> Generator[Callable[..., None], None, None]:
    """Register source namespaces to purge before the test and clean after it."""
    sources: list[str] = []

    def _register(*names: str) -> None:
        sources.extend(names)
        for source in names:
            purge_source(live_storage, source)

    yield _register
    for source in sources:
        purge_source(live_storage, source)
