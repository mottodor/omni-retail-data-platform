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

#: Applied to ``load_bronze`` tasks (Phase 5 slice 3). The Bronze loader
#: already retries transient catalog-auth failures internally; task retries
#: re-run the watermark-driven load, which is idempotent per day.
LAKEHOUSE_TASK_DEFAULT_ARGS: dict[str, object] = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=30),
}

#: Applied to ``transform_lakehouse`` tasks. A full dbt build (models plus
#: tests) is slower than ingestion and a task retry repeats the whole build,
#: so the backoff and timeout budgets are wider than the lakehouse defaults.
TRANSFORM_TASK_DEFAULT_ARGS: dict[str, object] = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=60),
}
