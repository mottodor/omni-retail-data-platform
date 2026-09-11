"""FastAPI application for the mock external API.

The app is a thin HTTP shell over the pure-stdlib ``data`` and ``faults``
modules: routes validate query parameters (422 on contract violations —
classified as non-retryable by the ingestion clients), apply deterministic
fault injection, and return paginated envelopes.

Pagination contracts (Phase 3 design spec §8):
- offset: ``page`` (1-based) + ``page_size`` with ``total_count`` in the envelope;
- cursor: ``cursor`` (opaque) + ``limit`` with ``next_cursor`` (null on the last page).
"""

import asyncio
import base64
import os
from collections.abc import Mapping
from datetime import UTC, date, datetime
from typing import Annotated, Any, Literal

from fastapi import FastAPI, HTTPException, Query

from mock_api import data, faults

DEFAULT_SEED = 42
DEFAULT_TIMEOUT_FAULT_SECONDS = 30.0
DEFAULT_MAX_PAGE_SIZE = 500


def create_app(
    seed: int = DEFAULT_SEED,
    timeout_fault_seconds: float = DEFAULT_TIMEOUT_FAULT_SECONDS,
) -> FastAPI:
    """Build the mock API application with deterministic data for ``seed``."""
    already_faulted: set[str] = set()
    app = FastAPI(title="OmniRetail Mock API", version="1.0.0")

    async def maybe_inject_fault(
        path: str, params: Mapping[str, Any], fault: str | None, fault_rate: float
    ) -> None:
        if not faults.should_fault(seed, path, params, fault, fault_rate, already_faulted):
            return
        if fault == "429":
            # Retry-After: 0 keeps client backoff fast in local scenarios.
            raise HTTPException(
                status_code=429,
                detail="rate limited (injected)",
                headers={"Retry-After": "0"},
            )
        if fault == "500":
            raise HTTPException(status_code=500, detail="internal error (injected)")
        if fault == "timeout":
            # Hold the connection past any sane client read timeout; the
            # client aborts, retries, and the retry succeeds (once per fingerprint).
            await asyncio.sleep(timeout_fault_seconds)
            return
        raise HTTPException(status_code=400, detail=f"unknown fault mode: {fault}")

    def validate_fault_params(fault: str | None) -> None:
        if fault is not None and fault not in faults.FAULT_MODES:
            raise HTTPException(
                status_code=400,
                detail=f"fault must be one of {sorted(faults.FAULT_MODES)}",
            )

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/v1/fx-rates")
    async def fx_rates(
        value_date: Annotated[date, Query(alias="date")],
        base: Annotated[str, Query(pattern="^[A-Z]{3}$")] = "EUR",
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[int, Query(ge=1, le=DEFAULT_MAX_PAGE_SIZE)] = 100,
        fault: str | None = None,
        fault_rate: Annotated[float, Query(ge=0, le=1)] = 0.0,
    ) -> dict[str, Any]:
        validate_fault_params(fault)
        await maybe_inject_fault(
            "/api/v1/fx-rates",
            {"base": base, "date": value_date, "page": page, "page_size": page_size},
            fault,
            fault_rate,
        )
        if base not in data.known_currency_codes():
            raise HTTPException(status_code=400, detail=f"unknown base currency: {base}")
        items = data.fx_rates(seed, value_date, base)
        start = (page - 1) * page_size
        return {
            "base": base,
            "date": value_date.isoformat(),
            "page": page,
            "page_size": page_size,
            "total_count": len(items),
            "rates": items[start : start + page_size],
        }

    @app.get("/api/v1/marketing/campaigns")
    async def marketing_campaigns(
        value_date: Annotated[date | None, Query(alias="date")] = None,
        status: Literal["active", "paused", "completed", "scheduled"] | None = None,
        page: Annotated[int, Query(ge=1)] = 1,
        page_size: Annotated[int, Query(ge=1, le=DEFAULT_MAX_PAGE_SIZE)] = 50,
        fault: str | None = None,
        fault_rate: Annotated[float, Query(ge=0, le=1)] = 0.0,
    ) -> dict[str, Any]:
        validate_fault_params(fault)
        snapshot_date = value_date or data.ANCHOR_DATE
        await maybe_inject_fault(
            "/api/v1/marketing/campaigns",
            {"date": snapshot_date, "status": status, "page": page, "page_size": page_size},
            fault,
            fault_rate,
        )
        items = data.campaigns(seed, snapshot_date)
        if status is not None:
            items = [item for item in items if item["status"] == status]
        start = (page - 1) * page_size
        return {
            "date": snapshot_date.isoformat(),
            "status": status,
            "page": page,
            "page_size": page_size,
            "total_count": len(items),
            "campaigns": items[start : start + page_size],
        }

    @app.get("/api/v1/deliveries")
    async def deliveries(
        updated_since: Annotated[datetime, Query()],
        cursor: Annotated[str | None, Query()] = None,
        limit: Annotated[int, Query(ge=1, le=DEFAULT_MAX_PAGE_SIZE)] = 100,
        fault: str | None = None,
        fault_rate: Annotated[float, Query(ge=0, le=1)] = 0.0,
    ) -> dict[str, Any]:
        validate_fault_params(fault)
        await maybe_inject_fault(
            "/api/v1/deliveries",
            {"updated_since": updated_since, "cursor": cursor, "limit": limit},
            fault,
            fault_rate,
        )
        if updated_since.tzinfo is None:
            updated_since = updated_since.replace(tzinfo=UTC)
        offset = _decode_cursor(cursor)
        all_items = data.deliveries(seed)
        items = [item for item in all_items if item["updated_at"] >= updated_since]
        page_items = items[offset : offset + limit]
        next_offset = offset + len(page_items)
        has_more = bool(page_items) and next_offset < len(items)
        return {
            "updated_since": updated_since.isoformat(),
            "limit": limit,
            "next_cursor": _encode_cursor(next_offset) if has_more else None,
            "deliveries": page_items,
        }

    return app


def _encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(str(offset).encode("utf-8")).decode("ascii")


def _decode_cursor(cursor: str | None) -> int:
    if cursor is None:
        return 0
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        offset = int(base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as error:
        raise HTTPException(status_code=400, detail=f"invalid cursor: {cursor!r}") from error
    if offset < 0:
        raise HTTPException(status_code=400, detail=f"invalid cursor offset: {offset}")
    return offset


app = create_app(
    seed=int(os.environ.get("MOCK_API_SEED", str(DEFAULT_SEED))),
    timeout_fault_seconds=float(
        os.environ.get("MOCK_API_TIMEOUT_FAULT_SECONDS", str(DEFAULT_TIMEOUT_FAULT_SECONDS))
    ),
)
