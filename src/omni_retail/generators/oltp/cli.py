"""Command-line interface for the OLTP data generator."""

import argparse
import logging
import random
import sys
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from omni_retail.generators.oltp import db
from omni_retail.generators.oltp.config import InitialLoadConfig, MutationConfig
from omni_retail.generators.oltp.initial import generate_initial
from omni_retail.generators.oltp.mutations import plan_mutations

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m omni_retail.generators.oltp",
        description="Deterministic generator for the OmniRetail PostgreSQL OLTP source.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    schema_parser = subparsers.add_parser("schema", help="apply the idempotent OLTP DDL")
    schema_parser.add_argument(
        "--schema-file",
        type=Path,
        default=None,
        help="path to the DDL script (auto-detected from the repository by default)",
    )

    initial_parser = subparsers.add_parser("initial", help="generate and load the initial dataset")
    initial_parser.add_argument("--seed", type=int, default=42)
    initial_parser.add_argument("--customers", type=int, default=10_000)
    initial_parser.add_argument("--products", type=int, default=5_000)
    initial_parser.add_argument("--orders", type=int, default=100_000)
    initial_parser.add_argument("--history-days", type=int, default=365)
    initial_parser.add_argument(
        "--truncate-oltp-data",
        action="store_true",
        help="DESTRUCTIVE: delete all existing OLTP rows before loading",
    )
    initial_parser.add_argument("--schema-file", type=Path, default=None)

    mutate_parser = subparsers.add_parser(
        "mutate", help="apply one batch of inserts, updates and deletes"
    )
    mutate_parser.add_argument("--seed", type=int, default=123)
    mutate_parser.add_argument("--events", type=int, default=200)

    return parser


def run_schema(args: argparse.Namespace) -> None:
    schema_file = args.schema_file or db.default_schema_file()
    with db.connect() as conn:
        db.apply_schema(conn, schema_file)
        logger.info("current OLTP row counts: %s", db.oltp_row_counts(conn))


def run_initial(args: argparse.Namespace) -> None:
    config = InitialLoadConfig(
        seed=args.seed,
        customers=args.customers,
        products=args.products,
        orders=args.orders,
        history_days=args.history_days,
    )
    config.validate()

    logger.info(
        "generating initial load: seed=%s customers=%s products=%s orders=%s history_days=%s",
        config.seed,
        config.customers,
        config.products,
        config.orders,
        config.history_days,
    )
    load = generate_initial(config)

    with db.connect() as conn:
        schema_file = args.schema_file or db.default_schema_file()
        db.apply_schema(conn, schema_file)

        existing = db.oltp_row_counts(conn)
        if any(count > 0 for count in existing.values()):
            if not args.truncate_oltp_data:
                raise RuntimeError(
                    f"OLTP tables are not empty ({existing}); "
                    "pass --truncate-oltp-data to replace the data"
                )
            db.truncate_oltp_data(conn)

        counts = db.insert_initial_load(conn, load)
        logger.info("initial load inserted: %s", counts)


def run_mutate(args: argparse.Namespace) -> None:
    config = MutationConfig(seed=args.seed, events=args.events)
    now = datetime.now(UTC)

    with db.connect() as conn:
        state = db.fetch_mutation_state(conn)
        logger.info(
            "mutation state: %d mutable orders, %d customers, %d products, seed=%s, events=%s",
            len(state.orders),
            len(state.customer_ids),
            len(state.products),
            config.seed,
            config.events,
        )
        plan = plan_mutations(random.Random(config.seed), state, config, now)
        summary = db.apply_mutations(conn, plan)
        logger.info("mutation batch applied: %s", summary)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = build_parser().parse_args(argv)

    handlers = {
        "schema": run_schema,
        "initial": run_initial,
        "mutate": run_mutate,
    }
    try:
        handlers[args.command](args)
    except (psycopg.OperationalError, RuntimeError, FileNotFoundError, ValueError) as error:
        logger.error("%s failed: %s", args.command, error)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
