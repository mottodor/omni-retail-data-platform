# Agent guide — Ingestion and source systems

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

Read this guide before working on: batch extraction, REST API clients, file/S3
ingestion, the synthetic data generator, the PostgreSQL source schema, or
pipeline logging.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md sections 9, 14–17, 46. Section numbers are
preserved so existing references of the form "AGENTS §N" keep resolving.

---

## 9. Logging requirements

Pipeline logs should expose useful execution context.

Where applicable include:

- `run_id`;
- `batch_id`;
- dataset name;
- source name;
- logical date/data interval;
- object/file name;
- row count;
- rejected row count;
- checksum;
- partition;
- Kafka topic/partition/offset where relevant.

Avoid logging full raw records when they may contain sensitive or large data.

Prefer summaries and identifiers.

---

## 14. Data ingestion rules

### 14.1 Raw data preservation

For external sources, preserve the raw representation before transformation whenever practical.

REST API:

```text
API response -> landing/raw -> normalized Bronze
```

Files:

```text
landing -> processing -> archive
                       -> rejected
```

Do not transform away the original input before durable raw storage unless explicitly justified.

### 14.2 Ingestion metadata

Include metadata where applicable:

- source;
- ingestion timestamp;
- batch ID;
- source file/object;
- checksum;
- schema version;
- extraction interval;
- source cursor/watermark.

### 14.3 Incremental extraction

Do not use unbounded production-like:

```sql
SELECT *
FROM source_table
```

for every scheduled run.

Use a documented strategy such as:

- timestamp watermark;
- monotonic ID;
- snapshot;
- CDC.

Initial bootstrap extraction may use full snapshots when appropriate.

### 14.4 Idempotency

A retry or replay of the same logical batch must not create duplicate business state.

Idempotency strategy must be explicit.

Possible mechanisms:

- deterministic object path;
- batch manifest;
- checksum;
- unique ingestion key;
- MERGE semantics;
- deduplication key;
- replace partition;
- transaction boundaries.

---

## 15. REST API ingestion

API clients must consider:

- pagination;
- request timeout;
- retry;
- exponential backoff;
- rate limiting;
- HTTP 429;
- HTTP 5xx;
- malformed responses;
- schema validation;
- partial responses;
- checkpoint/watermark;
- backfill.

Do not implement infinite retry loops.

Retries must have:

- maximum attempts or timeout;
- retryable/non-retryable classification;
- logging.

Raw responses should be persisted when required by the ingestion design.

Mock APIs are acceptable and encouraged for deterministic development and fault injection.

---

## 16. File/S3 ingestion

Support production-like file handling.

Expected object flow:

```text
landing
   ->
processing
   ->
archive

invalid
   ->
rejected
```

Important concerns:

- duplicate file detection;
- checksum validation;
- schema validation;
- malformed rows;
- partially uploaded objects;
- deterministic archive paths;
- backfill;
- reprocessing.

Do not silently skip malformed files.

Quarantined files require an explicit reason.

---

## 17. PostgreSQL rules

PostgreSQL serves as an OLTP source and may also host component metadata databases where appropriate.

OLTP schema should use:

- primary keys;
- foreign keys where realistic;
- constraints;
- meaningful data types;
- `created_at`;
- `updated_at`;
- realistic states/statuses.

The source generator must support:

- deterministic seed;
- initial load;
- inserts;
- updates;
- deletes.

Database migrations should be versioned.

Do not manually mutate production-like schema outside migrations unless the task is explicitly a schema-evolution exercise.

---

## 46. Generated data

The synthetic data generators and deterministic source simulators are part of
the delivered capstone. Preserve plausible data for:

- customers;
- products;
- categories;
- orders;
- order items;
- payments;
- shipments;
- supplier files;
- mock API sources.

Clickstream generation is outside this repository's final scope. Use
deterministic seeds where test reproducibility matters, and preserve the OLTP
generator's initial-load and mutation modes.

Avoid creating random values that violate database constraints unless testing invalid data explicitly.
