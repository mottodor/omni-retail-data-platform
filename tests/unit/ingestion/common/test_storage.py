"""Offline tests for the boto3-backed object storage wrapper (botocore Stubber)."""

import pytest
from botocore.stub import Stubber

from omni_retail.ingestion.common.storage import (
    BotoObjectStorage,
    ObjectNotFoundError,
    StorageConfig,
)


def make_storage() -> BotoObjectStorage:
    return BotoObjectStorage(
        config=StorageConfig(
            endpoint_url="http://127.0.0.1:9000",
            access_key_id="k",
            secret_access_key="s",
        )
    )


class _FakeStream:
    """Minimal readable body matching botocore response semantics."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


def test_put_object_sends_body_bytes() -> None:
    instance = make_storage()
    with Stubber(instance.client) as stubber:
        stubber.add_response(
            "put_object",
            {},
            {"Bucket": "landing", "Key": "a/b.csv", "Body": b"data"},
        )
        instance.put_object("landing", "a/b.csv", b"data")
        stubber.assert_no_pending_responses()


def test_get_object_returns_bytes() -> None:
    instance = make_storage()
    with Stubber(instance.client) as stubber:
        stubber.add_response(
            "get_object",
            {"Body": _FakeStream(b"payload")},
            {"Bucket": "archive", "Key": "a/b.csv"},
        )
        assert instance.get_object("archive", "a/b.csv") == b"payload"
        stubber.assert_no_pending_responses()


def test_missing_get_object_raises_object_not_found() -> None:
    instance = make_storage()
    with Stubber(instance.client) as stubber:
        stubber.add_client_error(
            "get_object",
            service_error_code="NoSuchKey",
            http_status_code=404,
            expected_params={"Bucket": "landing", "Key": "missing"},
        )
        with pytest.raises(ObjectNotFoundError):
            instance.get_object("landing", "missing")


def test_object_exists_maps_missing_head_to_false() -> None:
    instance = make_storage()
    with Stubber(instance.client) as stubber:
        stubber.add_client_error(
            "head_object",
            service_error_code="404",
            http_status_code=404,
            expected_params={"Bucket": "landing", "Key": "missing"},
        )
        assert instance.object_exists("landing", "missing") is False


def test_copy_object_targets_bucket_and_key() -> None:
    instance = make_storage()
    with Stubber(instance.client) as stubber:
        stubber.add_response(
            "copy_object",
            {},
            {
                "Bucket": "archive",
                "Key": "dst.csv",
                "CopySource": {"Bucket": "landing", "Key": "src.csv"},
            },
        )
        instance.copy_object("landing", "src.csv", "archive", "dst.csv")
        stubber.assert_no_pending_responses()


def test_list_object_keys_is_sorted_and_paginates() -> None:
    instance = make_storage()
    with Stubber(instance.client) as stubber:
        stubber.add_response(
            "list_objects_v2",
            {
                "Contents": [{"Size": 0}, {"Key": "incoming/b.csv"}],
                "IsTruncated": True,
                "NextContinuationToken": "token-1",
            },
            {"Bucket": "landing", "Prefix": "incoming/"},
        )
        stubber.add_response(
            "list_objects_v2",
            {
                "Contents": [{"Key": "incoming/a.csv"}],
                "IsTruncated": False,
            },
            {
                "Bucket": "landing",
                "Prefix": "incoming/",
                "ContinuationToken": "token-1",
            },
        )
        assert instance.list_object_keys("landing", "incoming/") == (
            "incoming/a.csv",
            "incoming/b.csv",
        )


def test_config_from_env_reads_documented_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "omni-ingestion")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "secret-value")

    config = StorageConfig.from_env()

    assert config.endpoint_url == "http://minio:9000"
    assert config.access_key_id == "omni-ingestion"
    assert config.secret_access_key == "secret-value"
    assert config.region == "us-east-1"


def test_config_from_env_defaults_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "omni-ingestion")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "secret-value")

    assert StorageConfig.from_env().endpoint_url == "http://127.0.0.1:9000"


def test_config_from_env_requires_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("S3_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("S3_SECRET_ACCESS_KEY", raising=False)

    with pytest.raises(ValueError, match="S3_ACCESS_KEY_ID"):
        StorageConfig.from_env()


def test_config_repr_does_not_leak_secret() -> None:
    config = StorageConfig(
        endpoint_url="http://127.0.0.1:9000",
        access_key_id="k",
        secret_access_key="super-secret",
    )

    assert "super-secret" not in repr(config)
