"""Airflow Datasets wiring the lakehouse orchestration chain (Phase 5 slice 3).

Ingestion DAGs emit ``raw://`` datasets when their logical batches reach the
archive bucket; ``load_bronze`` consumes them and emits ``lakehouse://bronze``
after a watermark-driven Bronze load; ``transform_lakehouse`` consumes that
dataset to run the dbt build. URIs are logical data addresses, not S3 paths.
"""

from airflow.datasets import Dataset


def raw_dataset(source_name: str) -> Dataset:
    """Dataset updated when one raw source's batch reaches the archive bucket."""
    return Dataset(f"raw://{source_name}")


#: Emitted by every ``snapshot_*`` task of ``ingest_postgres_snapshot``.
RAW_POSTGRES_SNAPSHOT = raw_dataset("postgres-snapshot")

#: Emitted by ``load_bronze`` after a successful watermark-driven load.
BRONZE = Dataset("lakehouse://bronze")

#: Datasets consumed by ``load_bronze`` after a producer successfully
#: preserves its raw batch in the archive bucket.
BRONZE_RAW_INPUTS = [
    RAW_POSTGRES_SNAPSHOT,
    raw_dataset("fx-rates"),
    raw_dataset("marketing-campaigns"),
    raw_dataset("deliveries"),
    raw_dataset("supplier-prices"),
    raw_dataset("partner-products"),
    raw_dataset("historical-orders"),
    raw_dataset("supplier-stock"),
]
