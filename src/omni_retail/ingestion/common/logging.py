"""Structured logging context for ingestion pipelines.

Pipeline logs must expose execution context (AGENTS.md §9): batch_id,
source, dataset, row counts and similar identifiers are rendered as a
``key=value`` prefix instead of being interpolated into free-form text.
"""

import logging
import sys
from collections.abc import MutableMapping
from typing import Any


class ContextLogger(logging.LoggerAdapter[logging.Logger]):
    """LoggerAdapter rendering its context fields as a ``key=value`` prefix."""

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[str, MutableMapping[str, Any]]:
        fields = self.extra or {}
        prefix = " ".join(f"{key}={value}" for key, value in sorted(fields.items()))
        if not prefix:
            return str(msg), kwargs
        return f"[{prefix}] {msg}", kwargs


def context_logger(name: str, **fields: object) -> ContextLogger:
    """Build a logger whose every record carries the given context fields."""
    return ContextLogger(logging.getLogger(name), dict(fields))


def configure_logging(level: int = logging.INFO) -> None:
    """CLI-friendly logging setup with timestamps and logger names."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stderr,
    )
