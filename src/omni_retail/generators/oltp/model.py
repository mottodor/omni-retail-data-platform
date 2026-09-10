"""Row models shared by the OLTP generator and the mutation planner."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class CategoryRow:
    category_id: int
    name: str
    parent_category_id: int | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ProductRow:
    product_id: int
    sku: str
    name: str
    category_id: int
    brand: str
    unit_price: Decimal
    unit_cost: Decimal
    is_active: bool
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class CustomerRow:
    customer_id: int
    email: str
    first_name: str
    last_name: str
    region: str
    city: str
    status: str
    segment: str
    registered_at: datetime
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class OrderRow:
    order_id: int
    customer_id: int
    status: str
    currency: str
    shipping_cost: Decimal
    order_total: Decimal
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class OrderItemRow:
    order_item_id: int
    order_id: int
    product_id: int
    quantity: int
    unit_price: Decimal
    line_total: Decimal
    created_at: datetime


@dataclass(frozen=True)
class PaymentRow:
    payment_id: int
    order_id: int
    method: str
    status: str
    amount: Decimal
    transaction_id: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class ShipmentRow:
    shipment_id: int
    order_id: int
    carrier: str
    tracking_number: str
    status: str
    shipped_at: datetime | None
    delivered_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class InitialLoad:
    """Complete deterministic initial dataset for all OLTP tables."""

    categories: tuple[CategoryRow, ...]
    products: tuple[ProductRow, ...]
    customers: tuple[CustomerRow, ...]
    orders: tuple[OrderRow, ...]
    order_items: tuple[OrderItemRow, ...]
    payments: tuple[PaymentRow, ...]
    shipments: tuple[ShipmentRow, ...]
    anchor: datetime

    def row_counts(self) -> dict[str, int]:
        return {
            "categories": len(self.categories),
            "products": len(self.products),
            "customers": len(self.customers),
            "orders": len(self.orders),
            "order_items": len(self.order_items),
            "payments": len(self.payments),
            "shipments": len(self.shipments),
        }


@dataclass(frozen=True)
class NewOrder:
    """A freshly inserted order with its items and payment."""

    order: OrderRow
    items: tuple[OrderItemRow, ...]
    payment: PaymentRow


@dataclass(frozen=True)
class OrderUpdate:
    order_id: int
    from_status: str
    to_status: str
    updated_at: datetime


@dataclass(frozen=True)
class PaymentUpdate:
    payment_id: int
    order_id: int
    from_status: str
    to_status: str
    updated_at: datetime


@dataclass(frozen=True)
class ShipmentUpdate:
    shipment_id: int
    order_id: int
    to_status: str
    delivered_at: datetime


@dataclass(frozen=True)
class CustomerUpdate:
    customer_id: int
    to_status: str
    updated_at: datetime


@dataclass(frozen=True)
class ProductUpdate:
    product_id: int
    unit_price: Decimal
    updated_at: datetime


@dataclass(frozen=True)
class MutationPlan:
    """One batch of deterministic mutations ready to be applied."""

    new_orders: tuple[NewOrder, ...]
    order_updates: tuple[OrderUpdate, ...]
    payment_updates: tuple[PaymentUpdate, ...]
    shipment_inserts: tuple[ShipmentRow, ...]
    shipment_updates: tuple[ShipmentUpdate, ...]
    customer_updates: tuple[CustomerUpdate, ...]
    product_updates: tuple[ProductUpdate, ...]
    deleted_order_ids: tuple[int, ...]


@dataclass(frozen=True)
class OrderState:
    """Current mutable state of one order fetched from the database."""

    order_id: int
    status: str
    payment_id: int
    payment_status: str
    shipment_id: int | None


@dataclass(frozen=True)
class ProductState:
    product_id: int
    unit_price: Decimal


@dataclass(frozen=True)
class MutationState:
    """Snapshot of database state required for deterministic planning."""

    orders: tuple[OrderState, ...]
    customer_ids: tuple[int, ...]
    products: tuple[ProductState, ...]
    next_order_id: int
    next_order_item_id: int
    next_payment_id: int
    next_shipment_id: int
