"""Deterministic initial-load generation for the OmniRetail OLTP source.

All randomness flows through a single ``random.Random(seed)`` instance and a
fixed default anchor timestamp, so the same config always produces the same
dataset.
"""

import random
from bisect import bisect_right
from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from itertools import accumulate

from omni_retail.generators.oltp.config import InitialLoadConfig
from omni_retail.generators.oltp.model import (
    CategoryRow,
    CustomerRow,
    InitialLoad,
    OrderItemRow,
    OrderRow,
    PaymentRow,
    ProductRow,
    ShipmentRow,
)

CATEGORY_TAXONOMY: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Electronics", ("Smartphones", "Laptops", "Audio", "Accessories")),
    ("Home & Kitchen", ("Cookware", "Small Appliances", "Furniture", "Decor")),
    ("Fashion", ("Clothing", "Footwear", "Bags", "Watches")),
    ("Sports & Outdoors", ("Fitness", "Camping", "Cycling", "Winter Sports")),
    ("Beauty & Health", ("Skincare", "Haircare", "Wellness", "Fragrances")),
)

FIRST_NAMES: tuple[str, ...] = (
    "Alexander",
    "Anna",
    "Dmitry",
    "Elena",
    "Ivan",
    "Olga",
    "Sergey",
    "Maria",
    "Alexey",
    "Natalia",
    "Mikhail",
    "Tatiana",
    "Andrey",
    "Ekaterina",
    "Nikolay",
    "Svetlana",
    "Vladimir",
    "Irina",
    "Artem",
    "Ksenia",
    "Maxim",
    "Polina",
    "Roman",
    "Yulia",
    "Denis",
    "Anastasia",
    "Kirill",
    "Vera",
    "Pavel",
    "Lydia",
)

LAST_NAMES: tuple[str, ...] = (
    "Ivanov",
    "Petrov",
    "Smirnov",
    "Kuznetsov",
    "Popov",
    "Vasiliev",
    "Sokolov",
    "Michailov",
    "Fedorov",
    "Morozov",
    "Volkov",
    "Alekseev",
    "Lebedev",
    "Semenov",
    "Egorov",
    "Pavlov",
    "Kozlov",
    "Stepanov",
    "Nikolaev",
    "Orlov",
    "Andreev",
    "Makarov",
    "Nikitin",
    "Zakharov",
    "Zaytsev",
    "Solovyov",
    "Borisov",
    "Yakovlev",
    "Grigoriev",
    "Romanov",
)

BRANDS: tuple[str, ...] = (
    "Nordwind",
    "Aurelia",
    "Vertex",
    "Lumen",
    "Craftline",
    "Terrano",
    "Bluepeak",
    "Ferrolite",
    "Meridian",
    "Solstice",
    "Aeropix",
    "Halcyon",
)

PRODUCT_ADJECTIVES: tuple[str, ...] = (
    "Compact",
    "Premium",
    "Classic",
    "Ergonomic",
    "Wireless",
    "Portable",
    "Deluxe",
    "Eco",
    "Ultra",
    "Slim",
    "Rugged",
    "Smart",
)

PRODUCT_NOUNS: tuple[str, ...] = (
    "Bundle",
    "Kit",
    "Pro",
    "Edition",
    "Set",
    "Series",
    "Model",
    "Pack",
    "Collection",
    "Line",
)

REGIONS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("Central", ("Moscow", "Tula", "Voronezh", "Yaroslavl")),
    ("Northwest", ("Saint Petersburg", "Kaliningrad", "Murmansk")),
    ("Volga", ("Kazan", "Nizhny Novgorod", "Samara", "Ufa")),
    ("Ural", ("Yekaterinburg", "Chelyabinsk", "Perm")),
    ("Siberia", ("Novosibirsk", "Omsk", "Krasnoyarsk", "Irkutsk")),
    ("South", ("Rostov-on-Don", "Krasnodar", "Sochi")),
)

EMAIL_DOMAINS: tuple[str, ...] = ("example.com", "example.org", "mail.example", "shop.example")

PAYMENT_METHODS: tuple[str, ...] = ("card", "paypal", "bank_transfer", "cash_on_delivery")
PAYMENT_METHOD_WEIGHTS: tuple[float, ...] = (0.60, 0.25, 0.10, 0.05)

CARRIERS: tuple[str, ...] = ("express_air", "express_ground", "postal", "courier")
CARRIER_WEIGHTS: tuple[float, ...] = (0.20, 0.35, 0.30, 0.15)

CURRENCIES: tuple[str, ...] = ("USD", "EUR", "GBP")
CURRENCY_WEIGHTS: tuple[float, ...] = (0.85, 0.10, 0.05)

SHIPPING_OPTIONS: tuple[Decimal, ...] = (
    Decimal("0.00"),
    Decimal("5.99"),
    Decimal("9.99"),
    Decimal("14.99"),
)
SHIPPING_WEIGHTS: tuple[float, ...] = (0.30, 0.30, 0.25, 0.15)

ITEM_COUNTS: tuple[int, ...] = (1, 2, 3, 4, 5, 6)
ITEM_COUNT_WEIGHTS: tuple[float, ...] = (0.22, 0.35, 0.22, 0.12, 0.06, 0.03)

QUANTITIES: tuple[int, ...] = (1, 2, 3)
QUANTITY_WEIGHTS: tuple[float, ...] = (0.65, 0.27, 0.08)

CENTS = Decimal("0.01")


class _WeightedPicker:
    """Deterministic weighted choice over a stable population."""

    def __init__(
        self, rng: random.Random, population: tuple[object, ...], weights: tuple[float, ...]
    ) -> None:
        self._rng = rng
        self._population = population
        self._cumulative = tuple(accumulate(weights))
        self._total = self._cumulative[-1]

    def pick(self) -> object:
        target = self._rng.random() * self._total
        return self._population[bisect_right(self._cumulative, target)]


def _round_seconds(timestamp: datetime) -> datetime:
    return timestamp.replace(microsecond=0)


def _clamp(timestamp: datetime, ceiling: datetime) -> datetime:
    return min(timestamp, ceiling)


def product_popularity_weights(count: int) -> tuple[float, ...]:
    return tuple(1.0 / ((rank + 1) ** 0.7) for rank in range(count))


def generate_initial(config: InitialLoadConfig) -> InitialLoad:
    """Build the complete deterministic initial dataset in memory."""
    config.validate()
    rng = random.Random(config.seed)

    categories = _generate_categories(rng, config.anchor)
    products = _generate_products(rng, config.products, categories, config.anchor)
    customers = _generate_customers(rng, config.customers, config.anchor)
    orders, order_items, payments, shipments = _generate_orders(rng, config, customers, products)

    return InitialLoad(
        categories=tuple(categories),
        products=tuple(products),
        customers=tuple(customers),
        orders=tuple(orders),
        order_items=tuple(order_items),
        payments=tuple(payments),
        shipments=tuple(shipments),
        anchor=config.anchor,
    )


def _generate_categories(rng: random.Random, anchor: datetime) -> list[CategoryRow]:
    rows: list[CategoryRow] = []
    next_id = 1
    for parent_name, children in CATEGORY_TAXONOMY:
        parent_created = _round_seconds(anchor - timedelta(days=rng.uniform(880, 900)))
        parent = CategoryRow(
            category_id=next_id,
            name=parent_name,
            parent_category_id=None,
            created_at=parent_created,
            updated_at=parent_created,
        )
        rows.append(parent)
        next_id += 1
        for child_name in children:
            child_created = _round_seconds(parent.created_at + timedelta(days=rng.uniform(5, 40)))
            rows.append(
                CategoryRow(
                    category_id=next_id,
                    name=child_name,
                    parent_category_id=parent.category_id,
                    created_at=child_created,
                    updated_at=child_created,
                )
            )
            next_id += 1
    return rows


def _generate_products(
    rng: random.Random, count: int, categories: list[CategoryRow], anchor: datetime
) -> list[ProductRow]:
    leaves = [c for c in categories if c.parent_category_id is not None]
    roots = [c for c in categories if c.parent_category_id is None]
    category_picker = _WeightedPicker(
        rng, tuple(leaves + roots), tuple([1.0] * len(leaves) + [0.2] * len(roots))
    )

    rows: list[ProductRow] = []
    for index in range(1, count + 1):
        category = category_picker.pick()
        assert isinstance(category, CategoryRow)
        created_at = _round_seconds(anchor - timedelta(days=rng.uniform(30, 730)))
        unit_price = Decimal(str(round(rng.uniform(5, 1500), 2))).quantize(CENTS)
        unit_cost = (unit_price * Decimal(str(round(rng.uniform(0.55, 0.85), 4)))).quantize(CENTS)
        rows.append(
            ProductRow(
                product_id=index,
                sku=f"SKU-{category.category_id:02d}-{index:05d}",
                name=f"{rng.choice(PRODUCT_ADJECTIVES)} {rng.choice(PRODUCT_NOUNS)}",
                category_id=category.category_id,
                brand=rng.choice(BRANDS),
                unit_price=unit_price,
                unit_cost=min(unit_cost, unit_price),
                is_active=rng.random() < 0.95,
                created_at=created_at,
                updated_at=created_at,
            )
        )
    return rows


def _generate_customers(rng: random.Random, count: int, anchor: datetime) -> list[CustomerRow]:
    status_picker = _WeightedPicker(rng, ("active", "inactive", "churned"), (0.85, 0.10, 0.05))
    segment_picker = _WeightedPicker(rng, ("standard", "premium", "vip"), (0.70, 0.25, 0.05))

    rows: list[CustomerRow] = []
    for index in range(1, count + 1):
        registered_at = _round_seconds(anchor - timedelta(days=rng.uniform(1, 1095)))
        status = status_picker.pick()
        assert isinstance(status, str)
        updated_at = registered_at
        if status != "active":
            updated_at = _clamp(
                _round_seconds(registered_at + timedelta(days=rng.uniform(30, 200))),
                anchor,
            )
        region, cities = rng.choice(REGIONS)
        first_name = rng.choice(FIRST_NAMES)
        last_name = rng.choice(LAST_NAMES)
        segment = segment_picker.pick()
        assert isinstance(segment, str)
        rows.append(
            CustomerRow(
                customer_id=index,
                email=f"{first_name.lower()}.{last_name.lower()}{index}@{rng.choice(EMAIL_DOMAINS)}",
                first_name=first_name,
                last_name=last_name,
                region=region,
                city=rng.choice(cities),
                status=status,
                segment=segment,
                registered_at=registered_at,
                created_at=registered_at,
                updated_at=updated_at,
            )
        )
    return rows


def _status_by_age(rng: random.Random, age_days: float) -> str:
    if age_days >= 30:
        picker = _WeightedPicker(
            rng,
            ("delivered", "refunded", "cancelled", "paid"),
            (0.88, 0.05, 0.05, 0.02),
        )
    elif age_days >= 7:
        picker = _WeightedPicker(
            rng,
            ("delivered", "shipped", "cancelled", "refunded", "paid"),
            (0.70, 0.15, 0.08, 0.04, 0.03),
        )
    elif age_days >= 2:
        picker = _WeightedPicker(
            rng,
            ("delivered", "shipped", "paid", "pending", "cancelled"),
            (0.30, 0.30, 0.20, 0.15, 0.05),
        )
    else:
        picker = _WeightedPicker(
            rng,
            ("pending", "paid", "shipped", "delivered", "cancelled"),
            (0.50, 0.30, 0.15, 0.03, 0.02),
        )
    status = picker.pick()
    assert isinstance(status, str)
    return status


def build_pending_order(
    rng: random.Random,
    order_id: int,
    first_item_id: int,
    payment_id: int,
    customer_id: int,
    product_ids: tuple[int, ...],
    product_prices: tuple[Decimal, ...],
    product_weights: tuple[float, ...],
    created_at: datetime,
) -> tuple[OrderRow, tuple[OrderItemRow, ...], PaymentRow]:
    """Build one new ``pending`` order with items and a pending payment.

    Shared by the initial load (which then ages the order to its final
    status) and the mutation planner (which inserts live pending orders).
    """
    assert len(product_ids) == len(product_prices) == len(product_weights)

    item_count_picker = _WeightedPicker(rng, ITEM_COUNTS, ITEM_COUNT_WEIGHTS)
    quantity_picker = _WeightedPicker(rng, QUANTITIES, QUANTITY_WEIGHTS)
    product_picker = _WeightedPicker(rng, product_ids, product_weights)
    shipping_picker = _WeightedPicker(rng, SHIPPING_OPTIONS, SHIPPING_WEIGHTS)
    currency_picker = _WeightedPicker(rng, CURRENCIES, CURRENCY_WEIGHTS)
    method_picker = _WeightedPicker(rng, PAYMENT_METHODS, PAYMENT_METHOD_WEIGHTS)

    item_count = item_count_picker.pick()
    assert isinstance(item_count, int)
    item_count = min(item_count, len(product_ids))

    price_by_product = dict(zip(product_ids, product_prices, strict=True))
    chosen: dict[int, int] = {}
    while len(chosen) < item_count:
        product_id = product_picker.pick()
        assert isinstance(product_id, int)
        if product_id in chosen:
            continue
        quantity = quantity_picker.pick()
        assert isinstance(quantity, int)
        chosen[product_id] = quantity

    shipping_cost = shipping_picker.pick()
    assert isinstance(shipping_cost, Decimal)
    currency = currency_picker.pick()
    assert isinstance(currency, str)
    method = method_picker.pick()
    assert isinstance(method, str)

    items: list[OrderItemRow] = []
    items_sum = Decimal("0")
    item_id = first_item_id
    for product_id, quantity in chosen.items():
        unit_price = price_by_product[product_id]
        line_total = (unit_price * quantity).quantize(CENTS)
        items_sum += line_total
        items.append(
            OrderItemRow(
                order_item_id=item_id,
                order_id=order_id,
                product_id=product_id,
                quantity=quantity,
                unit_price=unit_price,
                line_total=line_total,
                created_at=created_at,
            )
        )
        item_id += 1

    order_total = (items_sum + shipping_cost).quantize(CENTS)
    payment_created_at = _round_seconds(created_at + timedelta(seconds=rng.randint(30, 600)))

    order = OrderRow(
        order_id=order_id,
        customer_id=customer_id,
        status="pending",
        currency=currency,
        shipping_cost=shipping_cost,
        order_total=order_total,
        created_at=created_at,
        updated_at=payment_created_at,
    )
    payment = PaymentRow(
        payment_id=payment_id,
        order_id=order_id,
        method=method,
        status="pending",
        amount=order_total,
        transaction_id=f"TXN-{order_id:08d}",
        created_at=payment_created_at,
        updated_at=payment_created_at,
    )
    return order, tuple(items), payment


def _age_order(
    rng: random.Random,
    order: OrderRow,
    payment: PaymentRow,
    shipment_id: int,
    anchor: datetime,
) -> tuple[OrderRow, PaymentRow, ShipmentRow | None]:
    """Advance a pending order to a status consistent with its age."""
    target_status = _status_by_age(rng, (anchor - order.created_at).total_seconds() / 86400)

    if target_status == "pending":
        return order, payment, None

    if target_status == "cancelled":
        if rng.random() < 0.6:
            cancelled_at = _clamp(
                _round_seconds(payment.created_at + timedelta(seconds=rng.randint(60, 1200))),
                anchor,
            )
            payment = replace(payment, status="cancelled", updated_at=cancelled_at)
        else:
            captured_at = _clamp(
                _round_seconds(payment.created_at + timedelta(seconds=rng.randint(0, 3600))),
                anchor,
            )
            refunded_at = _clamp(
                _round_seconds(captured_at + timedelta(hours=rng.uniform(1, 72))),
                anchor,
            )
            payment = replace(payment, status="refunded", updated_at=max(captured_at, refunded_at))
        return replace(order, status="cancelled", updated_at=payment.updated_at), payment, None

    captured_at = _clamp(
        _round_seconds(payment.created_at + timedelta(seconds=rng.randint(0, 3600))),
        anchor,
    )
    payment = replace(payment, status="captured", updated_at=captured_at)
    order = replace(order, status=target_status, updated_at=captured_at)

    if target_status == "paid":
        return order, payment, None

    if target_status == "refunded" and rng.random() < 0.5:
        refunded_at = _clamp(
            _round_seconds(captured_at + timedelta(hours=rng.uniform(1, 168))),
            anchor,
        )
        payment = replace(payment, status="refunded", updated_at=refunded_at)
        return replace(order, status="refunded", updated_at=refunded_at), payment, None

    shipment_created = _clamp(
        _round_seconds(payment.created_at + timedelta(hours=rng.uniform(1, 24))),
        anchor,
    )
    shipped_at = _clamp(
        _round_seconds(shipment_created + timedelta(hours=rng.uniform(0, 12))),
        anchor,
    )
    carrier_picker = _WeightedPicker(rng, CARRIERS, CARRIER_WEIGHTS)

    if target_status == "shipped":
        carrier = carrier_picker.pick()
        assert isinstance(carrier, str)
        shipment = ShipmentRow(
            shipment_id=shipment_id,
            order_id=order.order_id,
            carrier=carrier,
            tracking_number=f"TRK-{order.order_id:08d}",
            status="in_transit",
            shipped_at=shipped_at,
            delivered_at=None,
            created_at=shipment_created,
            updated_at=shipped_at,
        )
        return replace(order, updated_at=shipped_at), payment, shipment

    delivered_at = _clamp(
        _round_seconds(shipped_at + timedelta(hours=rng.uniform(24, 168))),
        anchor,
    )
    carrier = carrier_picker.pick()
    assert isinstance(carrier, str)
    shipment = ShipmentRow(
        shipment_id=shipment_id,
        order_id=order.order_id,
        carrier=carrier,
        tracking_number=f"TRK-{order.order_id:08d}",
        status="delivered",
        shipped_at=shipped_at,
        delivered_at=delivered_at,
        created_at=shipment_created,
        updated_at=delivered_at,
    )
    order = replace(order, updated_at=delivered_at)

    if target_status == "refunded":
        refunded_at = _clamp(
            _round_seconds(delivered_at + timedelta(hours=rng.uniform(24, 240))),
            anchor,
        )
        payment = replace(payment, status="refunded", updated_at=refunded_at)
        order = replace(order, status="refunded", updated_at=refunded_at)

    return order, payment, shipment


def _generate_orders(
    rng: random.Random,
    config: InitialLoadConfig,
    customers: list[CustomerRow],
    products: list[ProductRow],
) -> tuple[list[OrderRow], list[OrderItemRow], list[PaymentRow], list[ShipmentRow]]:
    customers_by_registration = sorted(customers, key=lambda customer: customer.registered_at)
    registered_sorted = [customer.registered_at for customer in customers_by_registration]
    customer_ids = [customer.customer_id for customer in customers_by_registration]

    product_ids = tuple(product.product_id for product in products)
    product_prices = tuple(product.unit_price for product in products)
    product_weights = product_popularity_weights(len(products))

    orders: list[OrderRow] = []
    order_items: list[OrderItemRow] = []
    payments: list[PaymentRow] = []
    shipments: list[ShipmentRow] = []

    history_seconds = config.history_days * 86400
    next_item_id = 1
    next_shipment_id = 1

    for order_id in range(1, config.orders + 1):
        created_at = _round_seconds(
            config.anchor - timedelta(seconds=rng.uniform(601, history_seconds))
        )
        eligible_customers = bisect_right(registered_sorted, created_at)
        if eligible_customers == 0:
            eligible_customers = 1
        customer_id = customer_ids[rng.randrange(eligible_customers)]

        order, items, payment = build_pending_order(
            rng,
            order_id=order_id,
            first_item_id=next_item_id,
            payment_id=order_id,
            customer_id=customer_id,
            product_ids=product_ids,
            product_prices=product_prices,
            product_weights=product_weights,
            created_at=created_at,
        )
        order, payment, shipment = _age_order(rng, order, payment, next_shipment_id, config.anchor)

        orders.append(order)
        order_items.extend(items)
        payments.append(payment)
        next_item_id += len(items)
        if shipment is not None:
            shipments.append(shipment)
            next_shipment_id += 1

    return orders, order_items, payments, shipments
