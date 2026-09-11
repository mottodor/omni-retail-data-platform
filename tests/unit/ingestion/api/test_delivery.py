"""Unit tests for the delivery status source (cursor pagination)."""

import random
from datetime import UTC, date, datetime
from typing import Any

import httpx
import pytest

from omni_retail.ingestion.api.client import ApiClient, ApiClientConfig, ApiClientError
from omni_retail.ingestion.api.delivery import fetch_pages, updated_since_for

LOGICAL_DATE = date(2026, 9, 10)
UPDATED_SINCE = datetime(2026, 9, 9, 0, 0, 0, tzinfo=UTC)
DELIVERIES = [{"delivery_id": f"DLV-{i + 1:06d}"} for i in range(150)]


def envelope(items: list[dict[str, Any]], next_cursor: str | None) -> dict[str, Any]:
    return {
        "updated_since": UPDATED_SINCE.isoformat(),
        "limit": 100,
        "next_cursor": next_cursor,
        "deliveries": items,
    }


def make_client(
    handler_responses: list[dict[str, Any]],
) -> tuple[ApiClient, list[httpx.Request]]:
    requests: list[httpx.Request] = []
    responses = list(handler_responses)

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=responses.pop(0))

    client = ApiClient(
        ApiClientConfig(min_request_interval_seconds=0.0),
        transport=httpx.MockTransport(handler),
        rng=random.Random(1),
    )
    return client, requests


def test_updated_since_window_is_derived_from_logical_date() -> None:
    assert updated_since_for(LOGICAL_DATE) == UPDATED_SINCE


def test_fetch_pages_follows_cursor_until_exhausted() -> None:
    client, requests = make_client(
        [
            envelope(DELIVERIES[:100], next_cursor="MTAw"),
            envelope(DELIVERIES[100:], next_cursor=None),
        ]
    )

    pages = fetch_pages(client, LOGICAL_DATE, page_size=100)

    assert [page.page_number for page in pages] == [1, 2]
    assert [page.row_count for page in pages] == [100, 50]
    first, second = requests
    assert first.url.params["updated_since"] == UPDATED_SINCE.isoformat()
    assert "cursor" not in first.url.params
    assert second.url.params["cursor"] == "MTAw"


def test_fetch_pages_rejects_empty_page_with_next_cursor() -> None:
    client, _ = make_client([envelope([], next_cursor="MTAw")])

    with pytest.raises(ApiClientError, match="empty but"):
        fetch_pages(client, LOGICAL_DATE)


def test_fetch_pages_rejects_malformed_next_cursor() -> None:
    client, _ = make_client([{"next_cursor": 42, "deliveries": DELIVERIES[:10]}])

    with pytest.raises(ApiClientError, match="next_cursor"):
        fetch_pages(client, LOGICAL_DATE)
