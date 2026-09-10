"""Deterministic mutation planning for the OmniRetail OLTP source.

The planner is a pure function of ``(rng, state, config, now)``: given the
same database snapshot, seed and event time it always produces the same plan.
"""

import random
from datetime import datetime, timedelta
from decimal import Decimal

from omni_retail.generators.oltp.config import MutationConfig
from omni_retail.generators.oltp.initial import (
    CARRIERS,
    CENTS,
    build_pending_order,
    product_popularity_weights,
)
from omni_retail.generators.oltp.model import (
    CustomerUpdate,
    MutationPlan,
    MutationState,
    NewOrder,
    OrderState,
    OrderUpdate,
    PaymentUpdate,
    ProductUpdate,
    ShipmentRow,
    ShipmentUpdate,
)

ORDER_TRANSITIONS: dict[str, tuple[str, ...]] = {
    "pending": ("paid", "cancelled"),
    "paid": ("shipped", "cancelled", "refunded"),
    "shipped": ("delivered",),
    "delivered": ("refunded",),
    "cancelled": (),
    "refunded": (),
}

CUSTOMER_STATUSES: tuple[str, ...] = ("active", "inactive", "churned")

_EVENT_KINDS = (
    "order_update",
    "order_insert",
    "order_delete",
    "customer_update",
    "product_update",
)


def plan_mutations(
    rng: random.Random,
    state: MutationState,
    config: MutationConfig,
    now: datetime,
) -> MutationPlan:
    """Plan one deterministic batch of inserts, updates and deletes."""
    config.validate()

    product_ids = tuple(product.product_id for product in state.products)
    product_prices = tuple(product.unit_price for product in state.products)
    product_weights = product_popularity_weights(len(state.products))

    new_orders: list[NewOrder] = []
    order_updates: list[OrderUpdate] = []
    payment_updates: list[PaymentUpdate] = []
    shipment_inserts: list[ShipmentRow] = []
    shipment_updates: list[ShipmentUpdate] = []
    customer_updates: list[CustomerUpdate] = []
    product_updates: list[ProductUpdate] = []
    deleted_order_ids: list[int] = []

    touched_order_ids: set[int] = set()
    next_order_id = state.next_order_id
    next_order_item_id = state.next_order_item_id
    next_payment_id = state.next_payment_id
    next_shipment_id = state.next_shipment_id

    mix_weights = (
        config.order_update_weight,
        config.order_insert_weight,
        config.order_delete_weight,
        config.customer_update_weight,
        config.product_update_weight,
    )
    kinds = rng.choices(_EVENT_KINDS, weights=mix_weights, k=config.events)

    for kind in kinds:
        if kind == "order_update":
            candidates = [
                order
                for order in state.orders
                if order.order_id not in touched_order_ids and ORDER_TRANSITIONS[order.status]
            ]
            if not candidates:
                continue
            order = rng.choice(candidates)
            to_status = rng.choice(ORDER_TRANSITIONS[order.status])
            _apply_order_transition(
                rng=rng,
                order=order,
                to_status=to_status,
                now=now,
                shipment_id=next_shipment_id,
                order_updates=order_updates,
                payment_updates=payment_updates,
                shipment_inserts=shipment_inserts,
                shipment_updates=shipment_updates,
            )
            if order.status == "paid" and to_status == "shipped":
                next_shipment_id += 1
            touched_order_ids.add(order.order_id)

        elif kind == "order_insert":
            created_at = now.replace(microsecond=0) - timedelta(seconds=rng.randint(601, 9000))
            customer_id = rng.choice(state.customer_ids)
            order_row, items, payment = build_pending_order(
                rng,
                order_id=next_order_id,
                first_item_id=next_order_item_id,
                payment_id=next_payment_id,
                customer_id=customer_id,
                product_ids=product_ids,
                product_prices=product_prices,
                product_weights=product_weights,
                created_at=created_at,
            )
            new_orders.append(NewOrder(order=order_row, items=items, payment=payment))
            next_order_id += 1
            next_order_item_id += len(items)
            next_payment_id += 1

        elif kind == "order_delete":
            pending = [
                order
                for order in state.orders
                if order.order_id not in touched_order_ids and order.status == "pending"
            ]
            if not pending:
                continue
            order = rng.choice(pending)
            deleted_order_ids.append(order.order_id)
            touched_order_ids.add(order.order_id)

        elif kind == "customer_update":
            customer_id = rng.choice(state.customer_ids)
            customer_updates.append(
                CustomerUpdate(
                    customer_id=customer_id,
                    to_status=rng.choice(CUSTOMER_STATUSES),
                    updated_at=now,
                )
            )

        elif kind == "product_update":
            product = rng.choice(state.products)
            new_price = (
                product.unit_price * Decimal(str(round(rng.uniform(0.90, 1.15), 4)))
            ).quantize(CENTS)
            product_updates.append(
                ProductUpdate(
                    product_id=product.product_id,
                    unit_price=new_price,
                    updated_at=now,
                )
            )

    return MutationPlan(
        new_orders=tuple(new_orders),
        order_updates=tuple(order_updates),
        payment_updates=tuple(payment_updates),
        shipment_inserts=tuple(shipment_inserts),
        shipment_updates=tuple(shipment_updates),
        customer_updates=tuple(customer_updates),
        product_updates=tuple(product_updates),
        deleted_order_ids=tuple(deleted_order_ids),
    )


def _apply_order_transition(
    rng: random.Random,
    order: OrderState,
    to_status: str,
    now: datetime,
    shipment_id: int,
    order_updates: list[OrderUpdate],
    payment_updates: list[PaymentUpdate],
    shipment_inserts: list[ShipmentRow],
    shipment_updates: list[ShipmentUpdate],
) -> None:
    order_updates.append(
        OrderUpdate(
            order_id=order.order_id,
            from_status=order.status,
            to_status=to_status,
            updated_at=now,
        )
    )

    if order.status == "pending" and to_status == "paid":
        payment_updates.append(_payment_update(order, "pending", "captured", now))
    elif order.status == "pending" and to_status == "cancelled":
        payment_updates.append(_payment_update(order, "pending", "cancelled", now))
    elif order.status == "paid" and to_status == "shipped":
        shipment_inserts.append(
            ShipmentRow(
                shipment_id=shipment_id,
                order_id=order.order_id,
                carrier=rng.choice(CARRIERS),
                tracking_number=f"TRK-{order.order_id:08d}",
                status="in_transit",
                shipped_at=now,
                delivered_at=None,
                created_at=now,
                updated_at=now,
            )
        )
    elif order.status == "paid" and to_status in ("cancelled", "refunded"):
        payment_updates.append(_payment_update(order, "captured", "refunded", now))
    elif order.status == "shipped" and to_status == "delivered":
        if order.shipment_id is not None:
            shipment_updates.append(
                ShipmentUpdate(
                    shipment_id=order.shipment_id,
                    order_id=order.order_id,
                    to_status="delivered",
                    delivered_at=now,
                )
            )
    elif order.status == "delivered" and to_status == "refunded":
        payment_updates.append(_payment_update(order, "captured", "refunded", now))


def _payment_update(
    order: OrderState, from_status: str, to_status: str, now: datetime
) -> PaymentUpdate:
    return PaymentUpdate(
        payment_id=order.payment_id,
        order_id=order.order_id,
        from_status=from_status,
        to_status=to_status,
        updated_at=now,
    )
