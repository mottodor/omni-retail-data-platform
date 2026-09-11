"""FX rates source: offset pagination over ``/api/v1/fx-rates``."""

from datetime import date

from omni_retail.ingestion.api.client import ApiClient
from omni_retail.ingestion.api.pipeline import RawPage, expect_int, expect_list

SOURCE_NAME = "fx-rates"
SCHEMA_VERSION = "1.0"
PATH = "/api/v1/fx-rates"
DEFAULT_PAGE_SIZE = 100
BASE_CURRENCY = "EUR"


def fetch_pages(
    client: ApiClient, logical_date: date, page_size: int | None = None
) -> tuple[RawPage, ...]:
    """Fetch every raw page of FX quotes for the logical date (base EUR)."""
    effective_page_size = page_size or DEFAULT_PAGE_SIZE
    pages: list[RawPage] = []
    page_number = 1
    while True:
        response = client.get(
            PATH,
            {
                "base": BASE_CURRENCY,
                "date": logical_date.isoformat(),
                "page": page_number,
                "page_size": effective_page_size,
            },
        )
        payload = response.json()
        rates = expect_list(payload, "rates", response)
        total_count = expect_int(payload, "total_count", response)
        pages.append(RawPage(page_number=page_number, body=response.content, row_count=len(rates)))
        if not rates or page_number * effective_page_size >= total_count:
            return tuple(pages)
        page_number += 1
