"""Unit tests for the deterministic mock API data generators (pure stdlib)."""

from datetime import UTC, date, datetime

import pytest
from mock_api import data

SEED = 42
DAY_ONE = date(2026, 9, 10)
DAY_TWO = date(2026, 9, 11)


def test_fx_rates_are_deterministic_for_seed_and_date() -> None:
    first = data.fx_rates(SEED, DAY_ONE)
    second = data.fx_rates(SEED, DAY_ONE)
    assert first == second


def test_fx_rates_differ_between_dates() -> None:
    first = data.fx_rates(SEED, DAY_ONE)
    second = data.fx_rates(SEED, DAY_TWO)
    assert first != second


def test_fx_rates_quote_every_currency_except_the_base() -> None:
    euro_quotes = data.fx_rates(SEED, DAY_ONE, base="EUR")
    dollar_quotes = data.fx_rates(SEED, DAY_ONE, base="USD")

    assert len(euro_quotes) == len(data.CURRENCIES)
    assert {item["currency"] for item in dollar_quotes} == (
        {item["currency"] for item in euro_quotes} - {"USD"} | {"EUR"}
    )
    assert all(item["rate"] > 0 for item in euro_quotes)


def test_fx_rates_base_conversion_is_consistent() -> None:
    euro = {item["currency"]: item["rate"] for item in data.fx_rates(SEED, DAY_ONE, "EUR")}
    dollar = {item["currency"]: item["rate"] for item in data.fx_rates(SEED, DAY_ONE, "USD")}
    # 1 EUR = euro[USD] USD, so X-per-USD == X-per-EUR / USD-per-EUR
    assert dollar["JPY"] == pytest.approx(euro["JPY"] / euro["USD"], rel=1e-6)


def test_fx_rates_unknown_base_raises() -> None:
    with pytest.raises(KeyError):
        data.fx_rates(SEED, DAY_ONE, base="XXX")


def test_campaigns_are_deterministic_and_date_dependent() -> None:
    first = data.campaigns(SEED, DAY_ONE)
    assert first == data.campaigns(SEED, DAY_ONE)
    assert first != data.campaigns(SEED, DAY_TWO)

    by_id = {item["campaign_id"]: item for item in first}
    again = {item["campaign_id"]: item for item in data.campaigns(SEED, DAY_TWO)}
    # identity fields stay stable across dates; metrics evolve
    for campaign_id, campaign in by_id.items():
        assert again[campaign_id]["name"] == campaign["name"]
        assert again[campaign_id]["status"] == campaign["status"]


def test_campaigns_shape_and_domains() -> None:
    campaigns = data.campaigns(SEED, DAY_ONE)

    assert len(campaigns) == 180
    for campaign in campaigns:
        assert campaign["status"] in data.CAMPAIGN_STATUSES
        assert campaign["channel"] in data.CAMPAIGN_CHANNELS
        assert campaign["impressions"] >= campaign["clicks"]
        assert campaign["start_date"] < campaign["end_date"]


def test_campaigns_metrics_respect_snapshot_date() -> None:
    # campaigns starting after the snapshot date cannot have any activity
    campaigns = data.campaigns(SEED, date(2025, 1, 1))
    assert all(item["impressions"] == 0 for item in campaigns)


def test_deliveries_are_deterministic_sorted_and_complete() -> None:
    first = data.deliveries(SEED)
    assert first == data.deliveries(SEED)
    assert len(first) == 400

    keys = [(item["updated_at"], item["delivery_id"]) for item in first]
    assert keys == sorted(keys)
    ids = [item["delivery_id"] for item in first]
    assert len(set(ids)) == len(ids)

    for item in first:
        assert item["status"] in data.DELIVERY_STATUSES
        assert item["carrier"] in data.CARRIERS
        assert item["updated_at"] >= data.DELIVERY_HORIZON_START.replace(tzinfo=UTC)
        if item["status"] in {"delivered", "returned"}:
            assert item["delivered_at"] is not None
        else:
            assert item["delivered_at"] is None


def test_deliveries_updated_at_is_the_latest_known_timestamp() -> None:
    for item in data.deliveries(SEED):
        latest = item["delivered_at"] or item["shipped_at"]
        assert item["updated_at"] >= latest
        assert isinstance(item["updated_at"], datetime)
