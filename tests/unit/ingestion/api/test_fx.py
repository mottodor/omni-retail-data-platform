"""Unit tests for the fx-rates source (offset pagination)."""

import json
import random
from datetime import date
from typing import Any

import httpx

from omni_retail.ingestion.api.client import ApiClient, ApiClientConfig
from omni_retail.ingestion.api.fx import fetch_pages

LOGICAL_DATE = date(2026, 9, 10)
RATES = [{"currency": f"C{i:02d}", "rate": 1.0 + i / 100} for i in range(25)]


def envelope(page: int, page_size: int) -> dict[str, Any]:
    start = (page - 1) * page_size
    return {
        "base": "EUR",
        "date": LOGICAL_DATE.isoformat(),
        "page": page,
        "page_size": page_size,
        "total_count": len(RATES),
        "rates": RATES[start : start + page_size],
    }


def test_fetch_pages_walks_all_offset_pages() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = request.url.params
        return httpx.Response(200, json=envelope(int(params["page"]), int(params["page_size"])))

    client = ApiClient(
        ApiClientConfig(min_request_interval_seconds=0.0),
        transport=httpx.MockTransport(handler),
        rng=random.Random(1),
    )
    pages = fetch_pages(client, LOGICAL_DATE, page_size=10)

    assert [page.page_number for page in pages] == [1, 2, 3]
    assert [page.row_count for page in pages] == [10, 10, 5]
    assert [int(request.url.params["page"]) for request in requests] == [1, 2, 3]
    # logical date and base currency are part of every request
    assert all(request.url.params["date"] == LOGICAL_DATE.isoformat() for request in requests)
    assert all(request.url.params["base"] == "EUR" for request in requests)
    # the raw wire payload is preserved unchanged
    assert json.loads(pages[-1].body)["rates"] == RATES[20:]


def test_fetch_pages_stops_on_single_page() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = request.url.params
        return httpx.Response(200, json=envelope(int(params["page"]), int(params["page_size"])))

    client = ApiClient(
        ApiClientConfig(min_request_interval_seconds=0.0),
        transport=httpx.MockTransport(handler),
        rng=random.Random(1),
    )
    pages = fetch_pages(client, LOGICAL_DATE)  # default page_size 100 > 25 rates

    assert len(pages) == 1
    assert pages[0].row_count == 25
    assert len(requests) == 1
