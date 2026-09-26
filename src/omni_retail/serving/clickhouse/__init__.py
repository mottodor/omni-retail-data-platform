"""ClickHouse serving publication (Phase 6, ADR 0004).

Publisher module mirroring the Bronze loader shape: env-driven typed
config, a declarative mart registry (``specs``), a thin client boundary
(``client``), the publication cycle itself (``publisher``), and a CLI.
"""
