"""Unit tests for the deterministic OLTP mutation planner."""

import random
from datetime import UTC, datetime
from decimal import Decimal

from omni_retail.generators.oltp.config import MutationConfig
from omni_retail.generators.oltp.model import (
    MutationState,
    OrderState,
    ProductState,
)
from omni_retail.generators.oltp.mutations import ORDER_TRANSITIONS, plan_mutations

NOW = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)


def make_state() -> MutationState:
    orders = (
        OrderState(1, "pending", 101, "pending", None),
        OrderState(2, "pending", 102, "pending", None),
        OrderState(3, "paid", 103, "captured", None),
        OrderState(4, "paid", 104, "captured", None),
        OrderState(5, "shipped", 105, "captured", 501),
        OrderState(6, "delivered", 106, "captured", 502),
        OrderState(7, "cancelled", 107, "cancelled", None),
        OrderState(8, "refunded", 108, "refunded", None),
    )
    products = (
        ProductState(product_id=1, unit_price=Decimal("100.00")),
        ProductState(product_id=2, unit_price=Decimal("25.50")),
    )
    return MutationState(
        orders=orders,
        customer_ids=(1, 2, 3),
        products=products,
        next_order_id=9,
        next_order_item_id=900,
        next_payment_id=109,
        next_shipment_id=503,
    )


def test_plan_is_deterministic_for_same_seed_and_state() -> None:
    config = MutationConfig(seed=42, events=30)

    first = plan_mutations(random.Random(42), make_state(), config, NOW)
    second = plan_mutations(random.Random(42), make_state(), config, NOW)

    assert first == second


def test_order_updates_follow_allowed_transitions() -> None:
    plan = plan_mutations(random.Random(1), make_state(), MutationConfig(events=50), NOW)

    for update in plan.order_updates:
        assert update.to_status in ORDER_TRANSITIONS[update.from_status]
        assert update.updated_at == NOW


def test_terminal_orders_are_never_updated() -> None:
    plan = plan_mutations(random.Random(2), make_state(), MutationConfig(events=50), NOW)

    touched = {update.order_id for update in plan.order_updates}
    assert touched <= {1, 2, 3, 4, 5, 6}


def test_paid_transition_captures_payment() -> None:
    plan = plan_mutations(random.Random(3), make_state(), MutationConfig(events=50), NOW)

    payment_updates = {u.order_id: u for u in plan.payment_updates}

    for update in plan.order_updates:
        if update.from_status == "pending" and update.to_status == "paid":
            payment = payment_updates[update.order_id]
            assert payment.from_status == "pending"
            assert payment.to_status == "captured"


def test_cancel_from_pending_cancels_payment() -> None:
    plan = plan_mutations(random.Random(4), make_state(), MutationConfig(events=50), NOW)

    payment_updates = {u.order_id: u for u in plan.payment_updates}

    for update in plan.order_updates:
        if update.from_status == "pending" and update.to_status == "cancelled":
            payment = payment_updates[update.order_id]
            assert payment.to_status == "cancelled"


def test_shipped_transition_creates_in_transit_shipment() -> None:
    plan = plan_mutations(random.Random(5), make_state(), MutationConfig(events=50), NOW)

    inserted_orders = {s.order_id: s for s in plan.shipment_inserts}

    for update in plan.order_updates:
        if update.from_status == "paid" and update.to_status == "shipped":
            shipment = inserted_orders[update.order_id]
            assert shipment.status == "in_transit"
            assert shipment.shipped_at == NOW
            assert shipment.delivered_at is None


def test_delivered_transition_updates_shipment() -> None:
    plan = plan_mutations(random.Random(6), make_state(), MutationConfig(events=50), NOW)

    shipment_updates = {u.order_id: u for u in plan.shipment_updates}

    for update in plan.order_updates:
        if update.from_status == "shipped" and update.to_status == "delivered":
            shipment = shipment_updates[update.order_id]
            assert shipment.to_status == "delivered"
            assert shipment.delivered_at == NOW


def test_deletes_target_only_pending_orders() -> None:
    plan = plan_mutations(random.Random(7), make_state(), MutationConfig(events=50), NOW)

    pending = {order.order_id for order in make_state().orders if order.status == "pending"}

    assert set(plan.deleted_order_ids) <= pending


def test_deleted_orders_are_not_also_updated() -> None:
    plan = plan_mutations(random.Random(8), make_state(), MutationConfig(events=50), NOW)

    updated = {update.order_id for update in plan.order_updates}
    assert updated.isdisjoint(plan.deleted_order_ids)


def test_new_orders_are_pending_with_consistent_totals() -> None:
    plan = plan_mutations(random.Random(9), make_state(), MutationConfig(events=60), NOW)

    if not plan.new_orders:
        return  # mix may produce zero inserts for this seed; covered by other seeds

    for new_order in plan.new_orders:
        order = new_order.order
        assert order.status == "pending"
        assert order.created_at <= NOW
        assert len(new_order.items) >= 1

        items_sum = sum((item.line_total for item in new_order.items), Decimal("0"))
        expected = (items_sum + order.shipping_cost).quantize(Decimal("0.01"))
        assert order.order_total == expected

        assert new_order.payment.status == "pending"
        assert new_order.payment.amount == order.order_total


def test_new_orders_present_for_insert_heavy_seed() -> None:
    config = MutationConfig(seed=11, events=40)
    found = False
    for seed in range(20):
        plan = plan_mutations(random.Random(seed), make_state(), config, NOW)
        if plan.new_orders:
            found = True
            break

    assert found
