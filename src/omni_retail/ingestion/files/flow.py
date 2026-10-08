"""File ingestion flow: incoming -> processing -> archive | rejected.

Guarantees (Phase 3 design spec §5, §7):
- raw payload is stored unchanged in the archive bucket;
- a re-run of the same content is detected via the dedup marker and skipped;
- an object stranded in ``processing`` by an interrupted run is reprocessed;
- ``incoming`` objects are deleted only after a successful archive/reject;
- structurally broken files and malformed rows are quarantined with reasons.
"""

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime

from omni_retail.ingestion.common.checksum import compute_checksum
from omni_retail.ingestion.common.logging import context_logger
from omni_retail.ingestion.common.manifest import BatchManifest
from omni_retail.ingestion.common.paths import (
    BUCKET_ARCHIVE,
    BUCKET_LANDING,
    BUCKET_REJECTED,
    archive_key,
    badrows_key,
    dedup_key,
    incoming_key,
    manifest_key,
    processing_key,
    rejected_key,
    rejection_key,
)
from omni_retail.ingestion.common.storage import ObjectNotFoundError, ObjectStorage
from omni_retail.ingestion.files.schemas import FileSourceSchema
from omni_retail.ingestion.files.validation import (
    serialize_bad_rows_csv,
    serialize_rejection_json,
    validate_payload,
)

Clock = Callable[[], datetime]

logger = logging.getLogger(__name__)


class FileFlowError(Exception):
    """Base class for file flow failures."""


class RejectedRowsError(FileFlowError):
    """Raised in strict mode when a batch produced quarantined data."""


@dataclass(frozen=True)
class BatchOutcome:
    """Result of processing one file: the durable manifest plus its filename."""

    filename: str
    manifest: BatchManifest


def process_incoming(
    storage: ObjectStorage,
    schema: FileSourceSchema,
    run_date: date,
    clock: Clock | None = None,
    fail_on_rejected: bool = False,
    limit: int | None = None,
) -> tuple[BatchOutcome, ...]:
    """Process every pending object of one source; returns one outcome per file."""
    effective_clock: Clock = clock or (lambda: datetime.now(UTC))
    log = context_logger(__name__, source=schema.name, run_date=run_date.isoformat())

    outcomes: list[BatchOutcome] = []
    outcomes.extend(_recover_stranded(storage, schema, run_date, effective_clock, log))

    incoming_prefix = f"{schema.name}/incoming/"
    keys = storage.list_object_keys(BUCKET_LANDING, incoming_prefix)
    if limit is not None:
        keys = keys[:limit]
    for key in keys:
        filename = key.removeprefix(incoming_prefix)
        outcomes.append(_process_one(storage, schema, filename, run_date, effective_clock, log))

    if fail_on_rejected and _has_quarantined_data(outcomes):
        raise RejectedRowsError(
            "batch produced rejected data (strict mode): "
            + ", ".join(
                f"{outcome.filename}:{outcome.manifest.status}"
                f"/rows={outcome.manifest.rejected_row_count}"
                for outcome in outcomes
                if outcome.manifest.status == "rejected" or outcome.manifest.rejected_row_count
            )
        )
    log.info("batch finished: files=%d", len(outcomes))
    return tuple(outcomes)


def _recover_stranded(
    storage: ObjectStorage,
    schema: FileSourceSchema,
    run_date: date,
    clock: Clock,
    log: logging.LoggerAdapter[logging.Logger],
) -> list[BatchOutcome]:
    """Reprocess objects left in ``processing`` by an interrupted run."""
    processing_prefix = f"{schema.name}/processing/"
    outcomes: list[BatchOutcome] = []
    for key in storage.list_object_keys(BUCKET_LANDING, processing_prefix):
        filename = key.removeprefix(processing_prefix)
        log.info("recovering stranded object: filename=%s", filename)
        outcomes.append(
            _process_one(storage, schema, filename, run_date, clock, log, from_processing=True)
        )
    return outcomes


def _process_one(
    storage: ObjectStorage,
    schema: FileSourceSchema,
    filename: str,
    run_date: date,
    clock: Clock,
    log: logging.LoggerAdapter[logging.Logger],
    from_processing: bool = False,
) -> BatchOutcome:
    read_key = (
        processing_key(schema.name, filename)
        if from_processing
        else incoming_key(schema.name, filename)
    )
    payload = storage.get_object(BUCKET_LANDING, read_key)
    checksum = compute_checksum(payload)
    batch_id = f"{schema.name}-{checksum[:16]}"
    log = context_logger(
        __name__,
        source=schema.name,
        filename=filename,
        batch_id=batch_id,
        checksum=checksum,
    )

    marker_key = dedup_key(schema.name, checksum)
    if storage.object_exists(BUCKET_ARCHIVE, marker_key):
        try:
            marker = json.loads(storage.get_object(BUCKET_ARCHIVE, marker_key))
            canonical = BatchManifest.from_json(
                storage.get_object(BUCKET_ARCHIVE, manifest_key(schema.name, batch_id)).decode()
            )
            marker_object_key = marker["object_key"]
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
            ObjectNotFoundError,
        ) as error:
            raise FileFlowError(
                f"invalid duplicate evidence for batch {batch_id}: {error}"
            ) from error
        if (
            marker.get("batch_id") != batch_id
            or marker.get("checksum") != checksum
            or canonical.status != "completed"
            or canonical.source != schema.name
            or canonical.source_kind != "file"
            or canonical.batch_id != batch_id
            or canonical.checksum != checksum
            or canonical.object_key != marker_object_key
            or not storage.object_exists(BUCKET_ARCHIVE, marker_object_key)
        ):
            raise FileFlowError(
                f"inconsistent duplicate evidence for batch {batch_id}; "
                "canonical completed manifest and archived object are required"
            )
        manifest = _build_manifest(
            schema=schema,
            batch_id=batch_id,
            status="duplicate",
            object_key=canonical.object_key,
            checksum=checksum,
            size_bytes=len(payload),
            ingested_at=clock(),
            run_date=run_date,
            row_count=0,
            rejected_row_count=0,
            rejection_reasons=(),
        )
        _cleanup_landing(storage, schema, filename)
        log.info("duplicate file skipped: original=%s", canonical.object_key)
        return BatchOutcome(filename=filename, manifest=manifest)

    if not from_processing:
        storage.copy_object(
            BUCKET_LANDING,
            incoming_key(schema.name, filename),
            BUCKET_LANDING,
            processing_key(schema.name, filename),
        )

    expected_extension = f".{schema.format}"
    if not filename.endswith(expected_extension):
        return _reject_file(
            storage,
            schema,
            filename,
            run_date,
            clock,
            log,
            payload=payload,
            checksum=checksum,
            rejection_reasons=(
                f"unexpected file extension: expected {expected_extension} for source "
                f"{schema.name}",
            ),
        )

    result = validate_payload(schema, payload)
    if not result.file_valid:
        return _reject_file(
            storage,
            schema,
            filename,
            run_date,
            clock,
            log,
            payload=payload,
            checksum=checksum,
            rejection_reasons=result.file_errors,
        )

    archived_key = archive_key(schema.name, filename, run_date)
    if storage.object_exists(BUCKET_ARCHIVE, archived_key):
        archived_checksum = compute_checksum(storage.get_object(BUCKET_ARCHIVE, archived_key))
        if archived_checksum != checksum:
            return _reject_file(
                storage,
                schema,
                filename,
                run_date,
                clock,
                log,
                payload=payload,
                checksum=checksum,
                rejection_reasons=(
                    "archive path collision: existing immutable object has a different "
                    f"checksum at {archived_key}",
                ),
            )
    storage.put_object(BUCKET_ARCHIVE, archived_key, payload)
    if result.bad_rows:
        storage.put_object(
            BUCKET_REJECTED,
            badrows_key(schema.name, filename, run_date),
            serialize_bad_rows_csv(schema, result.bad_rows),
        )

    manifest = _build_manifest(
        schema=schema,
        batch_id=batch_id,
        status="completed",
        object_key=archived_key,
        checksum=checksum,
        size_bytes=len(payload),
        ingested_at=clock(),
        run_date=run_date,
        row_count=result.row_count,
        rejected_row_count=result.rejected_row_count,
        rejection_reasons=tuple(f"row {bad.row_number}: {bad.reason}" for bad in result.bad_rows),
    )
    storage.put_object(
        BUCKET_ARCHIVE, manifest_key(schema.name, batch_id), manifest.to_json().encode()
    )
    storage.put_object(
        BUCKET_ARCHIVE,
        marker_key,
        json.dumps(
            {
                "batch_id": batch_id,
                "checksum": checksum,
                "object_key": archived_key,
            },
            sort_keys=True,
        ).encode(),
    )
    _cleanup_landing(storage, schema, filename)
    log.info(
        "file archived: row_count=%d rejected_row_count=%d",
        result.row_count,
        result.rejected_row_count,
    )
    return BatchOutcome(filename=filename, manifest=manifest)


def _reject_file(
    storage: ObjectStorage,
    schema: FileSourceSchema,
    filename: str,
    run_date: date,
    clock: Clock,
    log: logging.LoggerAdapter[logging.Logger],
    payload: bytes,
    checksum: str,
    rejection_reasons: tuple[str, ...],
) -> BatchOutcome:
    batch_id = f"{schema.name}-{checksum[:16]}"
    quarantine_key = rejected_key(schema.name, filename, run_date)
    storage.put_object(BUCKET_REJECTED, quarantine_key, payload)
    storage.put_object(
        BUCKET_REJECTED,
        rejection_key(schema.name, filename, run_date),
        serialize_rejection_json(rejection_reasons),
    )
    manifest = _build_manifest(
        schema=schema,
        batch_id=batch_id,
        status="rejected",
        object_key=quarantine_key,
        checksum=checksum,
        size_bytes=len(payload),
        ingested_at=clock(),
        run_date=run_date,
        row_count=0,
        rejected_row_count=0,
        rejection_reasons=rejection_reasons,
    )
    storage.put_object(
        BUCKET_ARCHIVE, manifest_key(schema.name, batch_id), manifest.to_json().encode()
    )
    _cleanup_landing(storage, schema, filename)
    log.warning("file rejected: reasons=%s", list(rejection_reasons))
    return BatchOutcome(filename=filename, manifest=manifest)


def _cleanup_landing(storage: ObjectStorage, schema: FileSourceSchema, filename: str) -> None:
    storage.delete_object(BUCKET_LANDING, processing_key(schema.name, filename))
    storage.delete_object(BUCKET_LANDING, incoming_key(schema.name, filename))


def _build_manifest(
    *,
    schema: FileSourceSchema,
    batch_id: str,
    status: str,
    object_key: str,
    checksum: str,
    size_bytes: int,
    ingested_at: datetime,
    run_date: date,
    row_count: int,
    rejected_row_count: int,
    rejection_reasons: tuple[str, ...],
) -> BatchManifest:
    return BatchManifest(
        batch_id=batch_id,
        source=schema.name,
        source_kind="file",
        status=status,  # type: ignore[arg-type]
        object_key=object_key,
        checksum=checksum,
        size_bytes=size_bytes,
        ingested_at=ingested_at,
        logical_date=run_date,
        row_count=row_count,
        rejected_row_count=rejected_row_count,
        schema_version=schema.schema_version,
        rejection_reasons=rejection_reasons,
    )


def _has_quarantined_data(outcomes: list[BatchOutcome]) -> bool:
    return any(
        outcome.manifest.status == "rejected" or outcome.manifest.rejected_row_count > 0
        for outcome in outcomes
    )
