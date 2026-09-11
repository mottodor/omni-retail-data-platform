"""Delivery status source: cursor pagination over ``/api/v1/deliveries``.

The extraction window is derived from the logical date only: updates in the
24 hours ending at midnight UTC of ``logical_date``. No wall-clock ``now()``
is involved, so backfills replay exactly the same window.
"""

from datetime import UTC, date, datetime, timedelta

from omni_retail.ingestion.api.client import ApiClient, ApiContractError
from omni_retail.ingestion.api.pipeline import RawPage, expect_list

SOURCE_NAME = "deliveries"
SCHEMA_VERSION = "1.0"
PATH = "/api/v1/deliveries"
DEFAULT_PAGE_SIZE = 100
LOOKBACK_DAYS = 1


def updated_since_for(logical_date: date) -> datetime:
    """Inclusive-lower extraction window: ``[logical_date - 1d, logical_date)`` UTC."""
    return datetime.combine(logical_date - timedelta(days=LOOKBACK_DAYS), datetime.min.time(), UTC)


def fetch_pages(
    client: ApiClient, logical_date: date, page_size: int | None = None
) -> tuple[RawPage, ...]:
    """Fetch every raw delivery page for the logical date window."""
    effective_page_size = page_size or DEFAULT_PAGE_SIZE
    updated_since = updated_since_for(logical_date)
    pages: list[RawPage] = []
    cursor: str | None = None
    page_number = 1
    while True:
        params: dict[str, str | int] = {
            "updated_since": updated_since.isoformat(),
            "limit": effective_page_size,
        }
        if cursor is not None:
            params["cursor"] = cursor
        response = client.get(PATH, params)
        payload = response.json()
        deliveries = expect_list(payload, "deliveries", response)
        pages.append(
            RawPage(page_number=page_number, body=response.content, row_count=len(deliveries))
        )
        next_cursor = payload.get("next_cursor") if isinstance(payload, dict) else None
        if next_cursor is None:
            return tuple(pages)
        if not isinstance(next_cursor, str):
            raise ApiContractError(
                f"GET {response.request.url}: envelope field 'next_cursor' must be a "
                f"string or null, got {type(next_cursor).__name__}"
            )
        if not deliveries:
            raise ApiContractError(
                f"GET {response.request.url}: page {page_number} is empty but the "
                "envelope advertises a next cursor"
            )
        cursor = next_cursor
        page_number += 1
