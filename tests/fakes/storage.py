"""In-memory fake implementing the ObjectStorage protocol for unit tests."""

from omni_retail.ingestion.common.storage import ObjectNotFoundError


class FakeStorage:
    """Deterministic in-memory object storage.

    Mirrors S3 semantics relevant to the ingestion flow:
    - get/copy raise ObjectNotFoundError for missing objects;
    - delete is idempotent (missing objects are ignored, like S3 204);
    - listing returns sorted keys under a prefix.
    """

    def __init__(self) -> None:
        self._objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        self._objects[(bucket, key)] = body

    def get_object(self, bucket: str, key: str) -> bytes:
        try:
            return self._objects[(bucket, key)]
        except KeyError as error:
            raise ObjectNotFoundError(f"s3://{bucket}/{key} not found") from error

    def object_exists(self, bucket: str, key: str) -> bool:
        return (bucket, key) in self._objects

    def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
        body = self.get_object(src_bucket, src_key)
        self._objects[(dst_bucket, dst_key)] = body

    def delete_object(self, bucket: str, key: str) -> None:
        self._objects.pop((bucket, key), None)

    def list_object_keys(self, bucket: str, prefix: str) -> tuple[str, ...]:
        keys = sorted(key for b, key in self._objects if b == bucket and key.startswith(prefix))
        return tuple(keys)

    def stored_objects(self) -> dict[tuple[str, str], bytes]:
        """Read-only view over stored objects (assertions in tests)."""
        return dict(self._objects)
