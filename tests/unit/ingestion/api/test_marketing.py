"""Unit tests for the marketing campaigns source (offset pagination)."""

import random
from datetime import date
from typing import Any

import httpx

from omni_retail.ingestion.api.client import ApiClient, ApiClientConfig
from omni_retail.ingestion.api.marketing import fetch_pages

LOGICAL_DATE = date(2026, 9, 10)
CAMPAIGNS = [{"campaign_id": f"CMP-{i + 1:04d}", "status": "active"} for i in range(120)]


def envelope(page: int, page_size: int) -> dict[str, Any]:
    start = (page - 1) * page_size
    return {
        "date": LOGICAL_DATE.isoformat(),
        "status": None,
        "page": page,
        "page_size": page_size,
        "total_count": len(CAMPAIGNS),
        "campaigns": CAMPAIGNS[start : start + page_size],
    }


def make_client(handler_responses: list[dict[str, Any]]) -> tuple[ApiClient, list[httpx.Request]]:
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


def test_fetch_pages_walks_all_offset_pages() -> None:
    client, requests = make_client([envelope(1, 50), envelope(2, 50), envelope(3, 50)])

    pages = fetch_pages(client, LOGICAL_DATE, page_size=50)

    assert [page.page_number for page in pages] == [1, 2, 3]
    assert [page.row_count for page in pages] == [50, 50, 20]
    assert all(request.url.params["date"] == LOGICAL_DATE.isoformat() for request in requests)


def test_fetch_pages_uses_source_default_page_size() -> None:
    # a source smaller than the default page size completes in a single page
    single_page = {
        "date": LOGICAL_DATE.isoformat(),
        "status": None,
        "page": 1,
        "page_size": 50,
        "total_count": 50,
        "campaigns": CAMPAIGNS[:50],
    }
    client, requests = make_client([single_page])

    pages = fetch_pages(client, LOGICAL_DATE)

    assert len(pages) == 1
    assert pages[0].row_count == 50
    assert int(requests[0].url.params["page_size"]) == 50
