"""PostgreSQL snapshot extraction into the archive bucket (Phase 4 design spec §5).

Watermark-based incremental snapshots of the OLTP source tables, serialized
as Parquet with explicit pyarrow schemas. Reuses the Phase 3 storage, path
and manifest primitives.
"""
