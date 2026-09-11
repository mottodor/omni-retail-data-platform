"""Unit tests for structured logging context (batch_id, source, counts)."""

import logging

import pytest

from omni_retail.ingestion.common.logging import context_logger


def test_context_fields_appear_in_output(caplog: pytest.LogCaptureFixture) -> None:
    logger = context_logger("omni_retail.test", source="supplier-prices", batch_id="spb-abc")

    with caplog.at_level(logging.INFO, logger="omni_retail.test"):
        logger.info("archived file")

    assert "source=supplier-prices" in caplog.text
    assert "batch_id=spb-abc" in caplog.text


def test_context_prefix_comes_before_message(caplog: pytest.LogCaptureFixture) -> None:
    logger = context_logger("omni_retail.test", source="partner-products")

    with caplog.at_level(logging.INFO, logger="omni_retail.test"):
        logger.info("processed object")

    assert "source=partner-products" in caplog.text
    assert "processed object" in caplog.text


def test_count_fields_are_rendered(caplog: pytest.LogCaptureFixture) -> None:
    logger = context_logger("omni_retail.test", row_count=10, rejected_row_count=2)

    with caplog.at_level(logging.INFO, logger="omni_retail.test"):
        logger.info("done")

    assert "row_count=10" in caplog.text
    assert "rejected_row_count=2" in caplog.text


def test_no_context_renders_plain_message(caplog: pytest.LogCaptureFixture) -> None:
    logger = context_logger("omni_retail.test")

    with caplog.at_level(logging.INFO, logger="omni_retail.test"):
        logger.info("plain")

    assert "] plain" not in caplog.text
