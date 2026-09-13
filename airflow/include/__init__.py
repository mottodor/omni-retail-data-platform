"""Shared helpers imported by DAG files as ``include.*`` (mounted read-only).

This directory is on ``PYTHONPATH`` inside the Airflow image and services;
it is intentionally outside the dev-venv package because Airflow is not a
dependency of the local environment (ADR 0003).
"""
