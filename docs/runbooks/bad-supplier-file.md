# Runbook: bad supplier file

**Scenario:** a supplier delivery was quarantined by the file ingestion flow
(`supplier-prices`, `partner-products`, `historical-orders`, or `supplier-stock`).
This runbook locates the reason, classifies the failure, and drives the fix.

## 1. Detect

Symptoms:

- pipeline logs contain `file rejected: reasons=[...]` (whole file) or
  `file archived: row_count=... rejected_row_count=N>0` (bad rows);
- `make ingest-files ARGS="--source <source> --date <date> --fail-on-rejected"`
  exited non-zero in strict mode;
- manifests with `status != "completed"` (see step 2).

## 2. Locate and read the evidence

Every batch writes a manifest:

```bash
# list manifests of the source (batch_id = <source>-<sha256[:16]>)
mc ls --recursive local/archive/_manifests/supplier-prices/

# read one manifest: status, row counts, rejection reasons
mc cat local/archive/_manifests/supplier-prices/<batch_id>.json
```

The `rejection_reasons` field distinguishes the failure class:

| Reason text | Class |
|---|---|
| `unexpected file extension` | transport/misdelivery |
| `malformed utf-8` / `malformed json` / `malformed parquet` / `malformed xlsx` | corrupted file |
| `missing required column: X` / `unexpected column: X` | schema drift |
| `row N: <field>: <detail>` | row-level data errors |

Raw evidence is always preserved:

- whole-file quarantine: `rejected/<source>/<yyyy>/<mm>/<dd>/<filename>` plus
  `<filename>.rejection.json` (machine-readable reasons);
- row-level quarantine: `<filename>.badrows.<ext>` — original values plus a
  `_rejection_reason` column;
- the incoming object was deleted from `landing` only after the durable
  quarantine copy existed.

## 3. Decide and act

### 3.1 Corrupted file (truncated/garbled payload)

1. Confirm with the supplier that the transfer failed; the checksum in the
   manifest identifies the exact content.
2. Ask for a re-delivery of the same logical file. Re-uploading identical
   corrupted content would be skipped as a `duplicate` — the corrected file
   has a different checksum and processes normally.
3. No platform-side cleanup needed: quarantined objects stay as audit trail.

### 3.2 Schema drift (missing/unexpected column)

1. Check `docs/data-contracts.md` for the current contract and the change
   process — additive producer changes are intentionally rejected until the
   contract is versioned.
2. If the change is agreed:
   - update the contract and `schema_version`;
   - update `omni_retail.ingestion.files.schemas` (and the generator, if used
     for tests);
   - add validator tests for the new shape;
   - land via PR; the drifted file is then re-delivered (step 3.1 re-delivery).
3. If the change is not agreed, reject it upstream; keep the quarantine as
   evidence.

### 3.3 Bad rows (file structure valid)

1. Review `rejected/.../<file>.badrows.<ext>`; every row carries the reason.
2. Classify: transient producer bug (ask for a corrected file) vs. legitimate
   edge values the contract should define (e.g. `quantity = 0` is valid,
   `quantity = -5` is not).
3. The valid rows are already archived; after the corrected file arrives, it
   is processed as a new batch (different checksum). If the producer can only
   re-send the identical file, the duplicates stay quarantined — reprocessing
   is NOT done by deleting the dedup marker for the same content, because the
   valid rows would be archived twice.

## 4. Reprocess after a fix

```bash
# re-deliver the corrected file to the drop zone (or via the vendor generator)
make seed-supplier-files ARGS="--source supplier-prices --rows 500 --seed 11"

# process the source for the logical date
make ingest-files ARGS="--source supplier-prices --date 2026-09-11"
```

Interrupted runs need no manual action: objects stranded in
`landing/<source>/processing/` are reprocessed automatically on the next run.

## 5. Alerting / prevention

- In scheduled Airflow operation, non-`completed` manifests are surfaced by
  the task exit state and logs. No external paging integration is delivered.
- Repeated schema drift from one supplier is a contract governance issue —
  escalate to the contract owner rather than repeatedly hotfixing validators.
