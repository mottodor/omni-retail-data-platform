"""Shared task policy for ingestion DAGs (Phase 4 design spec §4).

Bounded task-level retries with exponential backoff complement — not replace
— the http-level retries inside ``ApiClient`` and S3 operation retries
inside the ingestion flows.
"""

from datetime import timedelta
from typing import TypedDict


class TaskPolicy(TypedDict):
    """Typed subset of Airflow operator arguments shared by local tasks."""

    retries: int
    retry_delay: timedelta
    retry_exponential_backoff: bool
    max_retry_delay: timedelta
    execution_timeout: timedelta


#: Applied to every ingestion task via DAG ``default_args``.
INGESTION_TASK_DEFAULT_ARGS: TaskPolicy = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=5),
}

#: Applied to ``load_bronze`` tasks (Phase 5 slice 3). The Bronze loader
#: already retries transient catalog-auth failures internally; task retries
#: re-run the watermark-driven load, which is idempotent per day.
LAKEHOUSE_TASK_DEFAULT_ARGS: TaskPolicy = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}

#: Applied to idempotent weekly batch-Bronze snapshot expiration. Each retry
#: replans the table, so already-expired snapshots become an explicit no-op.
SNAPSHOT_MAINTENANCE_TASK_DEFAULT_ARGS: TaskPolicy = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
}

#: Applied to CDC data-file compaction. A retry replans current files and the
#: insert-only ledger makes a completed first attempt converge to a no-op.
CDC_COMPACTION_TASK_DEFAULT_ARGS: TaskPolicy = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=60),
}

#: Applied to CDC snapshot expiration after compaction. Protected refs and
#: retained ancestors are recalculated on every bounded retry.
CDC_EXPIRATION_TASK_DEFAULT_ARGS: TaskPolicy = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
}

#: Applied to stable-boundary polling before any analytical work starts.
#: The primitive has its own five-minute default deadline; the task timeout
#: leaves a small margin for client cleanup and log flushing.
CDC_BOUNDARY_TASK_DEFAULT_ARGS: TaskPolicy = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=6),
}

#: Applied to ``transform_lakehouse`` dbt tasks. A full dbt build (models plus
#: tests) is slower than ingestion and a task retry repeats the whole build,
#: so the backoff and timeout budgets are wider than the lakehouse defaults.
TRANSFORM_TASK_DEFAULT_ARGS: TaskPolicy = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=60),
}

#: Applied to the idempotent full-snapshot Gold -> ClickHouse publication.
SERVING_TASK_DEFAULT_ARGS: TaskPolicy = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
}
