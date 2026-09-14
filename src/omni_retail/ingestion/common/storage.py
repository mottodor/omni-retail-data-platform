"""Thin boto3 wrapper over S3-compatible object storage (MinIO)."""

import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

DEFAULT_ENDPOINT_URL = "http://127.0.0.1:9000"
DEFAULT_REGION = "us-east-1"

_NOT_FOUND_CODES = {"404", "NotFound", "NoSuchKey"}


class StorageError(Exception):
    """Base class for object storage failures."""


class ObjectNotFoundError(StorageError):
    """Raised when the requested object does not exist."""


class ObjectStorage(Protocol):
    """Storage boundary used by ingestion flows (real S3/MinIO or a test fake)."""

    def get_object(self, bucket: str, key: str) -> bytes: ...

    def put_object(self, bucket: str, key: str, body: bytes) -> None: ...

    def object_exists(self, bucket: str, key: str) -> bool: ...

    def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None: ...

    def delete_object(self, bucket: str, key: str) -> None: ...

    def list_object_keys(self, bucket: str, prefix: str) -> tuple[str, ...]: ...


@dataclass(frozen=True)
class StorageConfig:
    """Connection settings for the S3-compatible endpoint."""

    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    region: str = DEFAULT_REGION

    def __repr__(self) -> str:
        return (
            f"StorageConfig(endpoint_url={self.endpoint_url!r}, "
            f"access_key_id={self.access_key_id!r}, "
            f"secret_access_key=***masked***, region={self.region!r})"
        )

    @classmethod
    def from_env(cls) -> "StorageConfig":
        access_key_id = os.environ.get("S3_ACCESS_KEY_ID", "")
        secret_access_key = os.environ.get("S3_SECRET_ACCESS_KEY", "")
        if not access_key_id or not secret_access_key:
            raise ValueError("S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY must be set")
        return cls(
            endpoint_url=os.environ.get("S3_ENDPOINT_URL", DEFAULT_ENDPOINT_URL),
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
        )


class BotoObjectStorage:
    """boto3 implementation of the ObjectStorage protocol."""

    def __init__(self, config: StorageConfig) -> None:
        self._config = config
        self.client: S3Client = boto3.client(
            "s3",
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.access_key_id,
            aws_secret_access_key=config.secret_access_key,
            region_name=config.region,
        )

    def get_object(self, bucket: str, key: str) -> bytes:
        try:
            response = self.client.get_object(Bucket=bucket, Key=key)
        except ClientError as error:
            if self._is_not_found(error):
                raise ObjectNotFoundError(f"s3://{bucket}/{key} not found") from error
            raise StorageError(f"failed to read s3://{bucket}/{key}") from error
        body = response["Body"]
        return body.read()

    def put_object(self, bucket: str, key: str, body: bytes) -> None:
        try:
            self.client.put_object(Bucket=bucket, Key=key, Body=body)
        except ClientError as error:
            raise StorageError(f"failed to write s3://{bucket}/{key}") from error

    def object_exists(self, bucket: str, key: str) -> bool:
        try:
            self.client.head_object(Bucket=bucket, Key=key)
        except ClientError as error:
            if self._is_not_found(error):
                return False
            raise StorageError(f"failed to stat s3://{bucket}/{key}") from error
        return True

    def copy_object(self, src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> None:
        try:
            self.client.copy_object(
                Bucket=dst_bucket,
                Key=dst_key,
                CopySource={"Bucket": src_bucket, "Key": src_key},
            )
        except ClientError as error:
            raise StorageError(
                f"failed to copy s3://{src_bucket}/{src_key} -> s3://{dst_bucket}/{dst_key}"
            ) from error

    def delete_object(self, bucket: str, key: str) -> None:
        try:
            self.client.delete_object(Bucket=bucket, Key=key)
        except ClientError as error:
            raise StorageError(f"failed to delete s3://{bucket}/{key}") from error

    def list_object_keys(self, bucket: str, prefix: str) -> tuple[str, ...]:
        keys: list[str] = []
        continuation_token: str | None = None
        try:
            while True:
                if continuation_token is None:
                    response = self.client.list_objects_v2(Bucket=bucket, Prefix=prefix)
                else:
                    response = self.client.list_objects_v2(
                        Bucket=bucket, Prefix=prefix, ContinuationToken=continuation_token
                    )
                keys.extend(item["Key"] for item in response.get("Contents", []))
                if not response.get("IsTruncated"):
                    break
                continuation_token = response["NextContinuationToken"]
        except ClientError as error:
            raise StorageError(f"failed to list s3://{bucket}/{prefix}") from error
        return tuple(sorted(keys))

    @staticmethod
    def _is_not_found(error: ClientError) -> bool:
        code = str(error.response.get("Error", {}).get("Code", ""))
        status = error.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        return code in _NOT_FOUND_CODES or status == 404
