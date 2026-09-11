"""Deterministic data generation for the mock API (pure stdlib).

Every payload is derived from ``random.Random`` instances seeded with strings
built from (seed, date, key). String seeding is stable across processes and
platforms, so the same request always yields the same payload — which is what
makes backfills reproducible (Phase 3 design spec §8).

No wall-clock access: dates are either request parameters or fixed anchors.
"""

import random
from datetime import UTC, date, datetime, timedelta
from typing import Any

# Midpoint of the delivery snapshot horizon; marketing campaigns anchor here
# when the caller does not pass an explicit date.
ANCHOR_DATE = date(2026, 6, 15)
DELIVERY_HORIZON_START = datetime(2026, 7, 1, 0, 0, 0, tzinfo=UTC)
DELIVERY_HORIZON_END = datetime(2026, 9, 30, 23, 59, 59, tzinfo=UTC)

# Quoted currencies with static EUR reference rates (31 codes besides EUR).
CURRENCIES: tuple[tuple[str, float], ...] = (
    ("USD", 1.09),
    ("GBP", 0.85),
    ("JPY", 163.2),
    ("CHF", 0.94),
    ("AUD", 1.64),
    ("CAD", 1.49),
    ("CNY", 7.82),
    ("SEK", 11.35),
    ("NOK", 11.7),
    ("DKK", 7.46),
    ("PLN", 4.28),
    ("CZK", 25.1),
    ("HUF", 395.5),
    ("RON", 4.97),
    ("BGN", 1.956),
    ("TRY", 38.1),
    ("INR", 91.5),
    ("BRL", 6.05),
    ("MXN", 19.8),
    ("ZAR", 20.1),
    ("KRW", 1480.5),
    ("SGD", 1.45),
    ("NZD", 1.78),
    ("HKD", 8.42),
    ("AED", 4.0),
    ("ILS", 4.02),
    ("IDR", 17250.0),
    ("MYR", 5.1),
    ("PHP", 62.3),
    ("THB", 39.4),
    ("VND", 27500.0),
)

CAMPAIGN_STATUSES: tuple[str, ...] = ("active", "paused", "completed", "scheduled")
CAMPAIGN_CHANNELS: tuple[str, ...] = ("search", "display", "social", "email", "video")
DELIVERY_STATUSES: tuple[str, ...] = ("delivered", "in_transit", "delayed", "returned")
CARRIERS: tuple[str, ...] = ("DHL", "UPS", "FedEx", "DPD", "GLS", "PostNL")

_ADJECTIVES = (
    "Summer",
    "Mega",
    "Flash",
    "Prime",
    "Holiday",
    "Black",
    "Spring",
    "Cyber",
    "Weekend",
    "Loyalty",
)
_NOUNS = ("Sale", "Blitz", "Boost", "Drop", "Fest", "Campaign", "Deal", "Marathon")


def known_currency_codes() -> frozenset[str]:
    """All currency codes the FX endpoint can quote (including EUR)."""
    return frozenset({"EUR", *(code for code, _ in CURRENCIES)})


def fx_rates(seed: int, as_of: date, base: str = "EUR") -> list[dict[str, Any]]:
    """Deterministic FX quotes for ``as_of``: 1 unit of ``base`` in each currency.

    The daily jitter is bounded to +/- 2% around the static reference rate.
    """
    rng = random.Random(f"fx:{seed}:{as_of.isoformat()}")
    eur_rates: dict[str, float] = {
        code: round(reference * (1 + rng.uniform(-0.02, 0.02)), 6) for code, reference in CURRENCIES
    }
    if base == "EUR":
        return [{"currency": code, "rate": rate} for code, rate in eur_rates.items()]
    base_rate = eur_rates[base]
    quotes: list[dict[str, Any]] = []
    for code, rate in eur_rates.items():
        if code == base:
            continue
        quotes.append({"currency": code, "rate": round(rate / base_rate, 6)})
    quotes.append({"currency": "EUR", "rate": round(1 / base_rate, 6)})
    return quotes


def campaigns(seed: int, as_of: date) -> list[dict[str, Any]]:
    """Deterministic marketing campaigns with metrics evolving by ``as_of``."""
    rng = random.Random(f"campaigns:{seed}")
    result: list[dict[str, Any]] = []
    for index in range(180):
        campaign_id = f"CMP-{index + 1:04d}"
        status = rng.choices(CAMPAIGN_STATUSES, weights=[35, 15, 35, 15], k=1)[0]
        channel = rng.choice(CAMPAIGN_CHANNELS)
        start_date = date(2026, 1, 1) + timedelta(days=rng.randint(0, 180))
        duration_days = rng.randint(30, 120)
        end_date = start_date + timedelta(days=duration_days)
        budget_eur = round(rng.uniform(5_000, 250_000), 2)
        name = f"{rng.choice(_ADJECTIVES)} {rng.choice(_NOUNS)}"

        # Metrics depend on (seed, campaign_id, as_of) so that snapshots for
        # different logical dates differ while staying reproducible.
        metrics_rng = random.Random(f"campaign-metrics:{seed}:{campaign_id}:{as_of.isoformat()}")
        days_active = min(max((as_of - start_date).days, 0), duration_days)
        daily_impressions = metrics_rng.uniform(500, 20_000)
        impressions = int(daily_impressions * days_active)
        ctr = metrics_rng.uniform(0.005, 0.06)
        clicks = int(impressions * ctr)
        progress = days_active / duration_days if duration_days else 0
        spend_eur = round(budget_eur * progress * metrics_rng.uniform(0.85, 1.05), 2)

        result.append(
            {
                "campaign_id": campaign_id,
                "name": name,
                "channel": channel,
                "status": status,
                "start_date": start_date,
                "end_date": end_date,
                "budget_eur": budget_eur,
                "spend_eur": spend_eur,
                "impressions": impressions,
                "clicks": clicks,
            }
        )
    return result


def deliveries(seed: int) -> list[dict[str, Any]]:
    """Deterministic delivery records sorted by (updated_at, delivery_id).

    The snapshot horizon is fixed ([2026-07-01, 2026-09-30] UTC) so that
    ``updated_since`` windows are stable across server restarts.
    """
    rng = random.Random(f"deliveries:{seed}")
    horizon_seconds = (DELIVERY_HORIZON_END - DELIVERY_HORIZON_START).total_seconds()
    result: list[dict[str, Any]] = []
    for index in range(400):
        shipped_at = DELIVERY_HORIZON_START + timedelta(
            seconds=int(rng.uniform(0, horizon_seconds * 0.75))
        )
        status = rng.choices(DELIVERY_STATUSES, weights=[60, 20, 15, 5], k=1)[0]
        delivered_at: datetime | None = None
        if status in {"delivered", "returned"}:
            delivered_at = shipped_at + timedelta(days=rng.uniform(1, 10))
        updated_at = delivered_at or (shipped_at + timedelta(days=rng.uniform(0, 5)))
        result.append(
            {
                "delivery_id": f"DLV-{index + 1:06d}",
                "order_id": f"ORD-{rng.randint(100000, 999999)}",
                "carrier": rng.choice(CARRIERS),
                "status": status,
                "shipped_at": shipped_at,
                "delivered_at": delivered_at,
                "updated_at": updated_at,
            }
        )
    result.sort(key=lambda item: (item["updated_at"], item["delivery_id"]))
    return result
