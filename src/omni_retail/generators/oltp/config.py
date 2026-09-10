"""Configuration objects for the OLTP data generator."""

from dataclasses import dataclass
from datetime import UTC, datetime

DEFAULT_ANCHOR = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)


@dataclass(frozen=True)
class InitialLoadConfig:
    """Parameters of the deterministic initial OLTP load.

    The default anchor is a fixed timestamp so that the same seed always
    reproduces the same dataset regardless of wall-clock time.
    """

    seed: int = 42
    customers: int = 10_000
    products: int = 5_000
    orders: int = 100_000
    history_days: int = 365
    anchor: datetime = DEFAULT_ANCHOR

    def validate(self) -> None:
        if self.customers < 1:
            raise ValueError("customers must be >= 1")
        if self.products < 1:
            raise ValueError("products must be >= 1")
        if self.orders < 1:
            raise ValueError("orders must be >= 1")
        if self.history_days < 1:
            raise ValueError("history_days must be >= 1")
        if self.anchor.tzinfo is None:
            raise ValueError("anchor must be timezone-aware")


@dataclass(frozen=True)
class MutationConfig:
    """Parameters of one mutation batch against the OLTP source."""

    seed: int = 123
    events: int = 200
    order_update_weight: int = 40
    order_insert_weight: int = 35
    order_delete_weight: int = 10
    customer_update_weight: int = 10
    product_update_weight: int = 5

    def validate(self) -> None:
        if self.events < 1:
            raise ValueError("events must be >= 1")
        weights = (
            self.order_update_weight,
            self.order_insert_weight,
            self.order_delete_weight,
            self.customer_update_weight,
            self.product_update_weight,
        )
        if any(weight < 0 for weight in weights) or sum(weights) == 0:
            raise ValueError("mutation mix weights must be non-negative with a positive sum")
