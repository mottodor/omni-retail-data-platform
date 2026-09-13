"""Incremental-extraction watermarks stored in the archive bucket (Phase 4 §5).

The watermark is the ``(updated_at, pk)`` pair of the last row of the last
successfully extracted window. It moves **only after** a successful upload,
so an interruption between upload and watermark save simply re-extracts the
same window and overwrites the same object — duplicates cannot appear.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime

from omni_retail.ingestion.common.paths import BUCKET_ARCHIVE, postgres_watermark_key
from omni_retail.ingestion.common.storage import ObjectNotFoundError, ObjectStorage
from omni_retail.ingestion.postgres_snapshot.tables import TableSpec


@dataclass(frozen=True)
class Watermark:
    """Keyset position of the last extracted row of one table."""

    table: str
    updated_at: datetime
    pk: int

    def __post_init__(self) -> None:
        if self.updated_at.tzinfo is None:
            raise ValueError(
                f"watermark updated_at must be timezone-aware for table {self.table!r}"
            )


def load_watermark(storage: ObjectStorage, spec: TableSpec) -> Watermark | None:
    """Read the durable watermark; ``None`` means: extract a full snapshot."""
    try:
        payload = storage.get_object(BUCKET_ARCHIVE, postgres_watermark_key(spec.name))
    except ObjectNotFoundError:
        return None
    data = json.loads(payload)
    return Watermark(
        table=data["table"],
        updated_at=datetime.fromisoformat(data["updated_at"]),
        pk=int(data["pk"]),
    )


def save_watermark(storage: ObjectStorage, watermark: Watermark) -> None:
    """Persist the watermark (idempotent overwrite of the same JSON object)."""
    payload = {
        "table": watermark.table,
        "updated_at": watermark.updated_at.astimezone(UTC).isoformat(),
        "pk": watermark.pk,
    }
    storage.put_object(
        BUCKET_ARCHIVE,
        postgres_watermark_key(watermark.table),
        json.dumps(payload, sort_keys=True).encode(),
    )
