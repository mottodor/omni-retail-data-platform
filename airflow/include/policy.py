"""Shared task policy for ingestion DAGs (Phase 4 design spec §4).

Bounded task-level retries with exponential backoff complement — not replace
— the http-level retries inside ``ApiClient`` and S3 operation retries
inside the ingestion flows.
"""

from datetime import timedelta

#: Applied to every ingestion task via DAG ``default_args``.
INGESTION_TASK_DEFAULT_ARGS: dict[str, object] = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=5),
}
