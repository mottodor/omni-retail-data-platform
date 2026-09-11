"""Unit tests for the API ingestion CLI (run/backfill)."""

import json
import random
import sys
from datetime import date
from typing import Any

import httpx
import pytest

from fakes.storage import FakeStorage
from omni_retail.ingestion.api.cli import build_parser, main, run_backfill, spec_by_name
from omni_retail.ingestion.api.client import ApiClient, ApiClientConfig, NonRetryableApiError
from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE, manifest_key

RATES = [{"currency": f"C{i:02d}", "rate": 1.0} for i in range(12)]


def fx_handler(request: httpx.Request) -> httpx.Response:
    params = request.url.params
    page, page_size = int(params["page"]), int(params["page_size"])
    start = (page - 1) * page_size
    return httpx.Response(
        200,
        json={
            "base": "EUR",
            "date": params["date"],
            "page": page,
            "page_size": page_size,
            "total_count": len(RATES),
            "rates": RATES[start : start + page_size],
        },
    )


def make_client(handler: Any = None) -> ApiClient:
    return ApiClient(
        ApiClientConfig(min_request_interval_seconds=0.0),
        transport=httpx.MockTransport(handler or fx_handler),
        rng=random.Random(7),
    )


def parse(argv: list[str]) -> Any:
    return build_parser().parse_args(argv)


def test_spec_by_name_rejects_unknown_sources() -> None:
    with pytest.raises(ValueError, match="unknown api source"):
        spec_by_name("weather")


def test_run_one_persists_raw_pages_and_manifest() -> None:
    storage = FakeStorage()
    args = parse(["run", "--source", "fx-rates", "--date", "2026-09-10"])

    from omni_retail.ingestion.api.cli import run_one

    exit_code = run_one(spec_by_name(args.source), make_client(), storage, args)

    assert exit_code == 0
    stored_pages = storage.list_object_keys(BUCKET_ARCHIVE, "api/fx-rates/20260910/")
    assert stored_pages == ("api/fx-rates/20260910/page_0001.json",)
    manifest = json.loads(
        storage.get_object(BUCKET_ARCHIVE, manifest_key("fx-rates", "fx-rates-20260910"))
    )
    assert manifest["source_kind"] == "api"
    assert manifest["row_count"] == len(RATES)


def test_backfill_processes_each_logical_date_once_in_order() -> None:
    storage = FakeStorage()
    args = parse(["backfill", "--source", "fx-rates", "--from", "2026-09-01", "--to", "2026-09-03"])

    exit_code = run_backfill(spec_by_name(args.source), make_client(), storage, args)

    assert exit_code == 0
    for day in ("20260901", "20260902", "20260903"):
        assert storage.object_exists(BUCKET_ARCHIVE, f"api/fx-rates/{day}/page_0001.json")
        assert storage.object_exists(BUCKET_ARCHIVE, manifest_key("fx-rates", f"fx-rates-{day}"))


def test_backfill_rejects_inverted_date_ranges() -> None:
    args = parse(["backfill", "--source", "fx-rates", "--from", "2026-09-03", "--to", "2026-09-01"])
    with pytest.raises(ValueError, match="must not be after"):
        run_backfill(spec_by_name(args.source), make_client(), FakeStorage(), args)


def test_backfill_fails_fast_and_keeps_already_persisted_dates() -> None:
    storage = FakeStorage()
    state = {"failing_date": "2026-09-02"}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["date"] == state["failing_date"]:
            return httpx.Response(404, json={"detail": "not found"})
        return fx_handler(request)

    args = parse(["backfill", "--source", "fx-rates", "--from", "2026-09-01", "--to", "2026-09-03"])

    with pytest.raises(NonRetryableApiError):
        run_backfill(spec_by_name(args.source), make_client(handler), storage, args)

    # the first date was persisted; the failing and later dates were not
    assert storage.object_exists(BUCKET_ARCHIVE, "api/fx-rates/20260901/page_0001.json")
    assert not storage.object_exists(BUCKET_ARCHIVE, "api/fx-rates/20260902/page_0001.json")
    assert not storage.object_exists(BUCKET_ARCHIVE, "api/fx-rates/20260903/page_0001.json")


def test_main_returns_error_exit_code_for_unknown_source(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("ERROR"):
        assert main(["run", "--source", "nope", "--date", "2026-09-10"]) == 1
    assert "unknown api source" in caplog.text


def test_main_requires_date_for_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["prog", "run", "--source", "fx-rates"])
    with pytest.raises(SystemExit):
        build_parser().parse_args()


def test_default_date_is_required_not_wall_clock() -> None:
    args = parse(["run", "--source", "fx-rates", "--date", "2026-09-10"])
    assert args.date == date(2026, 9, 10)
