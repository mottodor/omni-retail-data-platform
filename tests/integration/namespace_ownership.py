"""Test-only object ownership and rollback helpers for live integration tests.

The integration suite shares a long-lived MinIO stack with developer data.  These
helpers let a test borrow deterministic object coordinates while preserving the
exact pre-test bytes, including when setup or the test body raises.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from types import TracebackType

from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE, manifest_key
from omni_retail.ingestion.common.storage import ObjectNotFoundError, ObjectStorage

ObjectRef = tuple[str, str]


class OwnershipError(RuntimeError):
    """An object falls outside a test's declared ownership boundary."""


@dataclass(frozen=True)
class _OriginalObject:
    body: bytes | None


class ObjectMutationJournal:
    """Proxy that journals exact object mutations and can roll them back.

    A key is snapshotted before its first put/copy/delete. Prefix leases are
    intentionally narrow and exist for deterministic API batch paths whose page
    count is not known before fetching. They never imply source-wide ownership.
    """

    def __init__(self, storage: ObjectStorage) -> None:
        self._storage = storage
        self._originals: dict[ObjectRef, _OriginalObject] = {}
        self._mutated: list[ObjectRef] = []
        self._leased_keys: set[ObjectRef] = set()
        self._leased_prefixes: set[ObjectRef] = set()
        self._rolled_back = False

    def lease_key(self, bucket: str, key: str) -> None:
        """Snapshot one exact key before setup starts mutating it."""
        self._ensure_active()
        ref = (bucket, key)
        if ref in self._leased_keys:
            return
        if any(
            bucket == prefix_bucket and key.startswith(prefix)
            for prefix_bucket, prefix in self._leased_prefixes
        ):
            raise OwnershipError(
                f"key lease overlaps an existing prefix lease: s3://{bucket}/{key}"
            )
        self._leased_keys.add(ref)
        self._snapshot(ref)

    def lease_keys(self, refs: Iterable[ObjectRef]) -> None:
        for bucket, key in refs:
            self.lease_key(bucket, key)

    def lease_prefix(self, bucket: str, prefix: str) -> None:
        """Lease one deterministic batch prefix, rejecting ambiguous overlap."""
        self._ensure_active()
        if not prefix or not prefix.endswith("/"):
            raise OwnershipError(f"prefix lease must be a non-empty directory prefix: {prefix!r}")
        ref = (bucket, prefix)
        if ref in self._leased_prefixes:
            raise OwnershipError(f"duplicate prefix lease: s3://{bucket}/{prefix}")
        for prefix_bucket, existing in self._leased_prefixes:
            if bucket == prefix_bucket and (
                prefix.startswith(existing) or existing.startswith(prefix)
            ):
                raise OwnershipError(
                    f"overlapping prefix leases: s3://{bucket}/{existing} and s3://{bucket}/{prefix}"
                )
        for key_bucket, key in self._leased_keys:
            if bucket == key_bucket and key.startswith(prefix):
                raise OwnershipError(
                    f"prefix lease overlaps an existing key lease: s3://{bucket}/{key}"
                )
        self._leased_prefixes.add(ref)
        for key in self._storage.list_object_keys(bucket, prefix):
            self._snapshot((bucket, key))

    def rollback(self) -> None:
        """Restore mutated keys to their exact pre-test state; idempotent."""
        if self._rolled_back:
            return
        errors: list[str] = []
        for ref in reversed(self._mutated):
            bucket, key = ref
            original = self._originals[ref]
            try:
                if original.body is None:
                    self._storage.delete_object(bucket, key)
                else:
                    self._storage.put_object(bucket, key, original.body)
            except Exception as error:  # best-effort restoration of every owned key
                errors.append(f"s3://{bucket}/{key}: {error}")
        self._rolled_back = True
        if errors:
            raise OwnershipError("object rollback failed:\n" + "\n".join(errors))

    def get_object(self, bucket: str, key: str) -> bytes:
        return self._storage.get_object(bucket, key)

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        self._before_mutation(bucket, key)
        self._storage.put_object(bucket, key, body)

    def object_exists(self, bucket: str, key: str) -> bool:
        return self._storage.object_exists(bucket, key)

    def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
        self._before_mutation(dst_bucket, dst_key)
        self._storage.copy_object(src_bucket, src_key, dst_bucket, dst_key)

    def delete_object(self, bucket: str, key: str) -> None:
        self._before_mutation(bucket, key)
        self._storage.delete_object(bucket, key)

    def list_object_keys(self, bucket: str, prefix: str) -> tuple[str, ...]:
        return self._storage.list_object_keys(bucket, prefix)

    def __enter__(self) -> ObjectMutationJournal:
        self._ensure_active()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.rollback()

    def _ensure_active(self) -> None:
        if self._rolled_back:
            raise OwnershipError("object mutation journal has already been rolled back")

    def _snapshot(self, ref: ObjectRef) -> None:
        if ref in self._originals:
            return
        bucket, key = ref
        try:
            body = self._storage.get_object(bucket, key)
        except ObjectNotFoundError:
            body = None
        self._originals[ref] = _OriginalObject(body)

    def _before_mutation(self, bucket: str, key: str) -> None:
        self._ensure_active()
        ref = (bucket, key)
        self._snapshot(ref)
        if ref not in self._mutated:
            self._mutated.append(ref)


class ScopedObjectStorage:
    """Object-storage view that exposes only declared exact keys/prefixes."""

    def __init__(
        self,
        storage: ObjectStorage,
        *,
        keys: Iterable[ObjectRef] = (),
        prefixes: Iterable[ObjectRef] = (),
    ) -> None:
        self._storage = storage
        self._keys = frozenset(keys)
        self._prefixes = tuple(prefixes)

    def get_object(self, bucket: str, key: str) -> bytes:
        self._require_owned(bucket, key)
        return self._storage.get_object(bucket, key)

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        self._require_owned(bucket, key)
        self._storage.put_object(bucket, key, body)

    def object_exists(self, bucket: str, key: str) -> bool:
        if not self._is_owned(bucket, key):
            return False
        return self._storage.object_exists(bucket, key)

    def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
        self._require_owned(src_bucket, src_key)
        self._require_owned(dst_bucket, dst_key)
        self._storage.copy_object(src_bucket, src_key, dst_bucket, dst_key)

    def delete_object(self, bucket: str, key: str) -> None:
        self._require_owned(bucket, key)
        self._storage.delete_object(bucket, key)

    def list_object_keys(self, bucket: str, prefix: str) -> tuple[str, ...]:
        return tuple(
            key
            for key in self._storage.list_object_keys(bucket, prefix)
            if self._is_owned(bucket, key)
        )

    def _is_owned(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self._keys or any(
            bucket == prefix_bucket and key.startswith(prefix)
            for prefix_bucket, prefix in self._prefixes
        )

    def _require_owned(self, bucket: str, key: str) -> None:
        if not self._is_owned(bucket, key):
            raise OwnershipError(f"object is outside the scoped test view: s3://{bucket}/{key}")


def manifest_object_refs(
    storage: ObjectStorage, manifests: Iterable[BatchManifest]
) -> frozenset[ObjectRef]:
    """Expand manifests to their exact registry and raw-data object keys."""
    refs: set[ObjectRef] = set()
    for manifest in manifests:
        refs.add((BUCKET_ARCHIVE, manifest_key(manifest.source, manifest.batch_id)))
        if manifest.object_key.endswith("/"):
            refs.update(
                (BUCKET_ARCHIVE, key)
                for key in storage.list_object_keys(BUCKET_ARCHIVE, manifest.object_key)
            )
        else:
            refs.add((BUCKET_ARCHIVE, manifest.object_key))
    return frozenset(refs)


def manifest_scoped_storage(
    storage: ObjectStorage, manifests: Iterable[BatchManifest]
) -> ScopedObjectStorage:
    """Build a read/write view restricted to manifest-owned registry/data keys."""
    return ScopedObjectStorage(storage, keys=manifest_object_refs(storage, manifests))
