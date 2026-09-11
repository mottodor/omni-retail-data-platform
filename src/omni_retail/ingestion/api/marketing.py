"""Marketing campaigns source: offset pagination over ``/api/v1/marketing/campaigns``."""

from datetime import date

from omni_retail.ingestion.api.client import ApiClient
from omni_retail.ingestion.api.pipeline import RawPage, expect_int, expect_list

SOURCE_NAME = "marketing-campaigns"
SCHEMA_VERSION = "1.0"
PATH = "/api/v1/marketing/campaigns"
DEFAULT_PAGE_SIZE = 50


def fetch_pages(
    client: ApiClient, logical_date: date, page_size: int | None = None
) -> tuple[RawPage, ...]:
    """Fetch every raw campaign page for the logical date snapshot."""
    effective_page_size = page_size or DEFAULT_PAGE_SIZE
    pages: list[RawPage] = []
    page_number = 1
    while True:
        response = client.get(
            PATH,
            {
                "date": logical_date.isoformat(),
                "page": page_number,
                "page_size": effective_page_size,
            },
        )
        payload = response.json()
        campaigns = expect_list(payload, "campaigns", response)
        total_count = expect_int(payload, "total_count", response)
        pages.append(
            RawPage(page_number=page_number, body=response.content, row_count=len(campaigns))
        )
        if not campaigns or page_number * effective_page_size >= total_count:
            return tuple(pages)
        page_number += 1
