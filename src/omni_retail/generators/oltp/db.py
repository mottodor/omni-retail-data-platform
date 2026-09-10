"""psycopg-based database access for the OLTP generator."""

import logging
import os
from pathlib import Path

import psycopg
from psycopg import Connection
from psycopg.conninfo import make_conninfo

from omni_retail.generators.oltp.model import (
    InitialLoad,
    MutationPlan,
    MutationState,
    OrderState,
    ProductState,
)

logger = logging.getLogger(__name__)

OLTP_TABLES: tuple[str, ...] = (
    "categories",
    "products",
    "customers",
    "orders",
    "order_items",
    "payments",
    "shipments",
)

SCHEMA_FILE_NAME = "01_oltp_schema.sql"


def connection_kwargs() -> dict[str, str]:
    """Collect PostgreSQL connection parameters from POSTGRES_* environment variables."""
    params: dict[str, str | None] = {
        "host": os.environ.get("POSTGRES_HOST", "localhost"),
        "port": os.environ.get("POSTGRES_PORT", "5432"),
        "user": os.environ.get("POSTGRES_USER"),
        "password": os.environ.get("POSTGRES_PASSWORD"),
        "dbname": os.environ.get("POSTGRES_DB"),
    }
    required_env = {
        "POSTGRES_USER": "user",
        "POSTGRES_PASSWORD": "password",
        "POSTGRES_DB": "dbname",
    }
    missing = [env for env, key in required_env.items() if not params[key]]
    if missing:
        raise RuntimeError(f"missing required environment variables: {', '.join(missing)}")

    return {key: value for key, value in params.items() if value is not None}


def connect() -> Connection:
    return psycopg.connect(conninfo=make_conninfo(**connection_kwargs()))


def default_schema_file() -> Path:
    """Locate the OLTP DDL file by walking up from CWD, then from the package."""
    name = Path("postgres") / "init" / SCHEMA_FILE_NAME
    for base in (Path.cwd(), *Path.cwd().parents):
        candidate = base / name
        if candidate.is_file():
            return candidate
    package_root_candidate = Path(__file__).resolve().parents[4] / name
    if package_root_candidate.is_file():
        return package_root_candidate
    raise FileNotFoundError(f"could not locate {SCHEMA_FILE_NAME}; pass --schema-file explicitly")


def apply_schema(conn: Connection, schema_file: Path) -> None:
    """Apply the idempotent DDL script in a single transaction."""
    script = schema_file.read_text()
    with conn.transaction():
        conn.execute(script)
    logger.info("schema applied from %s", schema_file)


def count_rows(conn: Connection, table: str) -> int:
    if table not in OLTP_TABLES:
        raise ValueError(f"unknown OLTP table: {table}")
    row = conn.execute(f"select count(*) from {table}").fetchone()
    return 0 if row is None else int(row[0])


def oltp_row_counts(conn: Connection) -> dict[str, int]:
    return {table: count_rows(conn, table) for table in OLTP_TABLES}


def truncate_oltp_data(conn: Connection) -> None:
    """Delete all OLTP data (destructive; used by the explicit truncate flag)."""
    with conn.transaction():
        conn.execute(
            "truncate table shipments, payments, order_items, orders, "
            "products, customers, categories restart identity cascade"
        )
    logger.warning("all OLTP data truncated (restart identity, cascade)")


def insert_initial_load(conn: Connection, load: InitialLoad) -> dict[str, int]:
    """Insert the full initial dataset in one transaction."""
    with conn.transaction():
        conn.cursor().executemany(
            "insert into categories "
            "(category_id, name, parent_category_id, created_at, updated_at) "
            "values (%s, %s, %s, %s, %s)",
            [
                (r.category_id, r.name, r.parent_category_id, r.created_at, r.updated_at)
                for r in load.categories
            ],
        )
        conn.cursor().executemany(
            "insert into products "
            "(product_id, sku, name, category_id, brand, unit_price, "
            "unit_cost, is_active, created_at, updated_at) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    r.product_id,
                    r.sku,
                    r.name,
                    r.category_id,
                    r.brand,
                    r.unit_price,
                    r.unit_cost,
                    r.is_active,
                    r.created_at,
                    r.updated_at,
                )
                for r in load.products
            ],
        )
        conn.cursor().executemany(
            "insert into customers (customer_id, email, first_name, last_name, region, city, "
            "status, segment, registered_at, created_at, updated_at) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    r.customer_id,
                    r.email,
                    r.first_name,
                    r.last_name,
                    r.region,
                    r.city,
                    r.status,
                    r.segment,
                    r.registered_at,
                    r.created_at,
                    r.updated_at,
                )
                for r in load.customers
            ],
        )
        conn.cursor().executemany(
            "insert into orders (order_id, customer_id, status, currency, shipping_cost, "
            "order_total, created_at, updated_at) values (%s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    r.order_id,
                    r.customer_id,
                    r.status,
                    r.currency,
                    r.shipping_cost,
                    r.order_total,
                    r.created_at,
                    r.updated_at,
                )
                for r in load.orders
            ],
        )
        conn.cursor().executemany(
            "insert into order_items (order_item_id, order_id, product_id, quantity, "
            "unit_price, line_total, created_at) values (%s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    r.order_item_id,
                    r.order_id,
                    r.product_id,
                    r.quantity,
                    r.unit_price,
                    r.line_total,
                    r.created_at,
                )
                for r in load.order_items
            ],
        )
        conn.cursor().executemany(
            "insert into payments (payment_id, order_id, method, status, amount, "
            "transaction_id, created_at, updated_at) values (%s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    r.payment_id,
                    r.order_id,
                    r.method,
                    r.status,
                    r.amount,
                    r.transaction_id,
                    r.created_at,
                    r.updated_at,
                )
                for r in load.payments
            ],
        )
        conn.cursor().executemany(
            "insert into shipments (shipment_id, order_id, carrier, tracking_number, status, "
            "shipped_at, delivered_at, created_at, updated_at) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    r.shipment_id,
                    r.order_id,
                    r.carrier,
                    r.tracking_number,
                    r.status,
                    r.shipped_at,
                    r.delivered_at,
                    r.created_at,
                    r.updated_at,
                )
                for r in load.shipments
            ],
        )
    return load.row_counts()


def fetch_mutation_state(conn: Connection) -> MutationState:
    """Snapshot the mutable OLTP state needed for deterministic planning."""
    orders = [
        OrderState(
            order_id=row[0],
            status=row[1],
            payment_id=row[2],
            payment_status=row[3],
            shipment_id=row[4],
        )
        for row in conn.execute(
            "select o.order_id, o.status, p.payment_id, p.status, s.shipment_id "
            "from orders o "
            "join payments p on p.order_id = o.order_id "
            "left join shipments s on s.order_id = o.order_id "
            "where o.status in ('pending', 'paid', 'shipped', 'delivered') "
            "order by o.order_id"
        ).fetchall()
    ]

    customer_ids = tuple(
        row[0] for row in conn.execute("select customer_id from customers order by customer_id")
    )
    products = tuple(
        ProductState(product_id=row[0], unit_price=row[1])
        for row in conn.execute("select product_id, unit_price from products order by product_id")
    )
    if not customer_ids or not products:
        raise RuntimeError("OLTP source is empty; run the initial load first")

    return MutationState(
        orders=tuple(orders),
        customer_ids=customer_ids,
        products=products,
        next_order_id=_next_id(conn, "orders", "order_id"),
        next_order_item_id=_next_id(conn, "order_items", "order_item_id"),
        next_payment_id=_next_id(conn, "payments", "payment_id"),
        next_shipment_id=_next_id(conn, "shipments", "shipment_id"),
    )


def _next_id(conn: Connection, table: str, column: str) -> int:
    row = conn.execute(f"select coalesce(max({column}), 0) + 1 from {table}").fetchone()
    return int(row[0]) if row is not None else 1


def apply_mutations(conn: Connection, plan: MutationPlan) -> dict[str, int]:
    """Apply a mutation plan in one transaction with guarded updates."""
    with conn.transaction():
        for new_order in plan.new_orders:
            order = new_order.order
            conn.execute(
                "insert into orders (order_id, customer_id, status, currency, shipping_cost, "
                "order_total, created_at, updated_at) "
                "values (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    order.order_id,
                    order.customer_id,
                    order.status,
                    order.currency,
                    order.shipping_cost,
                    order.order_total,
                    order.created_at,
                    order.updated_at,
                ),
            )
            conn.cursor().executemany(
                "insert into order_items (order_item_id, order_id, product_id, quantity, "
                "unit_price, line_total, created_at) values (%s, %s, %s, %s, %s, %s, %s)",
                [
                    (
                        item.order_item_id,
                        item.order_id,
                        item.product_id,
                        item.quantity,
                        item.unit_price,
                        item.line_total,
                        item.created_at,
                    )
                    for item in new_order.items
                ],
            )
            payment = new_order.payment
            conn.execute(
                "insert into payments (payment_id, order_id, method, status, amount, "
                "transaction_id, created_at, updated_at) "
                "values (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    payment.payment_id,
                    payment.order_id,
                    payment.method,
                    payment.status,
                    payment.amount,
                    payment.transaction_id,
                    payment.created_at,
                    payment.updated_at,
                ),
            )

        conn.cursor().executemany(
            "update orders set status = %s, updated_at = %s where order_id = %s and status = %s",
            [(u.to_status, u.updated_at, u.order_id, u.from_status) for u in plan.order_updates],
        )
        conn.cursor().executemany(
            "update payments set status = %s, updated_at = %s "
            "where payment_id = %s and status = %s",
            [
                (u.to_status, u.updated_at, u.payment_id, u.from_status)
                for u in plan.payment_updates
            ],
        )
        conn.cursor().executemany(
            "insert into shipments (shipment_id, order_id, carrier, tracking_number, status, "
            "shipped_at, delivered_at, created_at, updated_at) "
            "values (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (
                    s.shipment_id,
                    s.order_id,
                    s.carrier,
                    s.tracking_number,
                    s.status,
                    s.shipped_at,
                    s.delivered_at,
                    s.created_at,
                    s.updated_at,
                )
                for s in plan.shipment_inserts
            ],
        )
        conn.cursor().executemany(
            "update shipments set status = %s, delivered_at = %s, updated_at = %s "
            "where shipment_id = %s",
            [
                (u.to_status, u.delivered_at, u.delivered_at, u.shipment_id)
                for u in plan.shipment_updates
            ],
        )
        conn.cursor().executemany(
            "update customers set status = %s, updated_at = %s "
            "where customer_id = %s and status is distinct from %s",
            [
                (u.to_status, u.updated_at, u.customer_id, u.to_status)
                for u in plan.customer_updates
            ],
        )
        conn.cursor().executemany(
            "update products set unit_price = %s, updated_at = %s where product_id = %s",
            [(u.unit_price, u.updated_at, u.product_id) for u in plan.product_updates],
        )
        conn.cursor().executemany(
            "delete from orders where order_id = %s and status = 'pending'",
            [(order_id,) for order_id in plan.deleted_order_ids],
        )

    return {
        "orders_inserted": len(plan.new_orders),
        "orders_updated": len(plan.order_updates),
        "orders_deleted": len(plan.deleted_order_ids),
        "payments_updated": len(plan.payment_updates),
        "shipments_inserted": len(plan.shipment_inserts),
        "shipments_updated": len(plan.shipment_updates),
        "customers_updated": len(plan.customer_updates),
        "products_updated": len(plan.product_updates),
    }
