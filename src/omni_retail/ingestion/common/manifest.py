"""Batch manifest: durable metadata about one ingested batch."""

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

SourceKind = Literal["file", "api"]
BatchStatus = Literal["completed", "rejected", "duplicate"]


@dataclass(frozen=True)
class BatchManifest:
    """Metadata recorded for every processed batch (files and API pages alike)."""

    batch_id: str
    source: str
    source_kind: SourceKind
    status: BatchStatus
    object_key: str
    checksum: str
    size_bytes: int
    ingested_at: datetime
    logical_date: date | None
    row_count: int
    rejected_row_count: int
    schema_version: str
    rejection_reasons: tuple[str, ...] = ()

    def to_json(self) -> str:
        payload = {
            "batch_id": self.batch_id,
            "source": self.source,
            "source_kind": self.source_kind,
            "status": self.status,
            "object_key": self.object_key,
            "checksum": self.checksum,
            "size_bytes": self.size_bytes,
            "ingested_at": self.ingested_at.isoformat(),
            "logical_date": self.logical_date.isoformat() if self.logical_date else None,
            "row_count": self.row_count,
            "rejected_row_count": self.rejected_row_count,
            "schema_version": self.schema_version,
            "rejection_reasons": list(self.rejection_reasons),
        }
        return json.dumps(payload, sort_keys=True)

    @classmethod
    def from_json(cls, payload: str) -> "BatchManifest":
        data = json.loads(payload)
        return cls(
            batch_id=data["batch_id"],
            source=data["source"],
            source_kind=data["source_kind"],
            status=data["status"],
            object_key=data["object_key"],
            checksum=data["checksum"],
            size_bytes=data["size_bytes"],
            ingested_at=datetime.fromisoformat(data["ingested_at"]),
            logical_date=(
                date.fromisoformat(data["logical_date"]) if data["logical_date"] else None
            ),
            row_count=data["row_count"],
            rejected_row_count=data["rejected_row_count"],
            schema_version=data["schema_version"],
            rejection_reasons=tuple(data["rejection_reasons"]),
        )
