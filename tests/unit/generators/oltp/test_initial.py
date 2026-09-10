"""Unit tests for the deterministic OLTP initial-load generator."""

from decimal import Decimal

from omni_retail.generators.oltp.config import InitialLoadConfig
from omni_retail.generators.oltp.initial import generate_initial


def make_config(**overrides: object) -> InitialLoadConfig:
    values: dict[str, object] = {
        "seed": 7,
        "customers": 50,
        "products": 40,
        "orders": 200,
        "history_days": 365,
    }
    values.update(overrides)
    return InitialLoadConfig(**values)  # type: ignore[arg-type]


def test_same_seed_produces_identical_load() -> None:
    first = generate_initial(make_config())
    second = generate_initial(make_config())

    assert first == second


def test_different_seed_produces_different_load() -> None:
    first = generate_initial(make_config(seed=1))
    second = generate_initial(make_config(seed=2))

    assert first != second


def test_row_counts_match_configuration() -> None:
    load = generate_initial(make_config())

    assert len(load.categories) > 0
    assert len(load.products) == 40
    assert len(load.customers) == 50
    assert len(load.orders) == 200
    assert len(load.order_items) >= 200
    assert len(load.payments) == 200
    assert len(load.shipments) > 0


def test_every_order_has_items_and_payment() -> None:
    load = generate_initial(make_config())

    orders_with_items = {item.order_id for item in load.order_items}
    orders_with_payments = {payment.order_id for payment in load.payments}

    assert orders_with_items == {order.order_id for order in load.orders}
    assert orders_with_payments == {order.order_id for order in load.orders}


def test_order_totals_equal_items_plus_shipping() -> None:
    load = generate_initial(make_config())

    items_by_order: dict[int, list[Decimal]] = {}
    for item in load.order_items:
        items_by_order.setdefault(item.order_id, []).append(item.line_total)

    for order in load.orders:
        items_sum = sum(items_by_order[order.order_id], Decimal("0"))
        expected = (items_sum + order.shipping_cost).quantize(Decimal("0.01"))
        assert order.order_total == expected


def test_line_totals_equal_quantity_times_unit_price() -> None:
    load = generate_initial(make_config())

    for item in load.order_items:
        expected = (item.unit_price * item.quantity).quantize(Decimal("0.01"))
        assert item.line_total == expected


def test_payment_status_matches_order_status() -> None:
    load = generate_initial(make_config())

    status_by_order = {order.order_id: order.status for order in load.orders}
    captured = {"paid", "shipped", "delivered"}

    for payment in load.payments:
        order_status = status_by_order[payment.order_id]
        if order_status in captured:
            assert payment.status == "captured", (order_status, payment.status)
        elif order_status == "pending":
            assert payment.status == "pending"
        elif order_status == "refunded":
            assert payment.status == "refunded"
        elif order_status == "cancelled":
            assert payment.status in {"cancelled", "refunded"}


def test_shipments_only_for_shipped_delivered_or_late_refunded_orders() -> None:
    load = generate_initial(make_config())

    status_by_order = {order.order_id: order.status for order in load.orders}

    for shipment in load.shipments:
        assert status_by_order[shipment.order_id] in {"shipped", "delivered", "refunded"}


def test_delivered_shipments_have_ordered_timestamps() -> None:
    load = generate_initial(make_config())

    created_by_order = {order.order_id: order.created_at for order in load.orders}

    for shipment in load.shipments:
        assert shipment.shipped_at is not None
        assert shipment.shipped_at >= created_by_order[shipment.order_id]
        if shipment.status == "delivered":
            assert shipment.delivered_at is not None
            assert shipment.delivered_at >= shipment.shipped_at
        else:
            assert shipment.delivered_at is None


def test_all_timestamps_not_later_than_anchor() -> None:
    load = generate_initial(make_config())

    timestamps = (
        [category.created_at for category in load.categories]
        + [p.created_at for p in load.products]
        + [c.registered_at for c in load.customers]
        + [c.created_at for c in load.customers]
        + [o.created_at for o in load.orders]
        + [o.updated_at for o in load.orders]
        + [item.created_at for item in load.order_items]
        + [payment.created_at for payment in load.payments]
        + [payment.updated_at for payment in load.payments]
        + [s.created_at for s in load.shipments]
    )

    assert all(ts <= load.anchor for ts in timestamps)


def test_natural_keys_are_unique() -> None:
    load = generate_initial(make_config())

    emails = [customer.email for customer in load.customers]
    skus = [product.sku for product in load.products]
    transactions = [payment.transaction_id for payment in load.payments]
    trackings = [shipment.tracking_number for shipment in load.shipments]
    order_item_pairs = [(item.order_id, item.product_id) for item in load.order_items]

    assert len(set(emails)) == len(emails)
    assert len(set(skus)) == len(skus)
    assert len(set(transactions)) == len(transactions)
    assert len(set(trackings)) == len(trackings)
    assert len(set(order_item_pairs)) == len(order_item_pairs)


def test_foreign_keys_reference_existing_rows() -> None:
    load = generate_initial(make_config())

    category_ids = {category.category_id for category in load.categories}
    product_ids = {product.product_id for product in load.products}
    customer_ids = {customer.customer_id for customer in load.customers}
    order_ids = {order.order_id for order in load.orders}

    for category in load.categories:
        if category.parent_category_id is not None:
            assert category.parent_category_id in category_ids
            assert category.parent_category_id != category.category_id

    for product in load.products:
        assert product.category_id in category_ids

    for order in load.orders:
        assert order.customer_id in customer_ids

    for item in load.order_items:
        assert item.order_id in order_ids
        assert item.product_id in product_ids
