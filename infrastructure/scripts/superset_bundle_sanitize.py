"""Sanitize Superset import/export v1 ZIP bundles before committing them.

The Superset export API stamps bundles with a ``..._YYYYMMDDTHHMMSS`` root
folder (binary-diff churn between exports) and includes a
``databases/*.yaml`` entry for every referenced database — with the full
``sqlalchemy_uri``, credentials included (AGENTS.md §10 forbids committing
those). This script rewrites exported bundles in place:

- drops every ``databases/`` YAML entry (connections are always re-created
  from ``.env`` by ``infrastructure/scripts/superset_init.py`` at import
  time — the init script injects the env-built database config into the
  in-memory contents, keyed by the fixed connection UUIDs);
- rewrites ``metadata.yaml`` to the canonical assets form — ``type:
  assets`` (required by ``ImportAssetsCommand``; exporters stamp
  ``Dashboard``/``Dataset``/``Database`` per object) without the export
  timestamp;
- renames the export root folder to the ZIP stem;
- rewrites the archive deterministically (fixed entry timestamps, sorted
  entries) so identical content produces identical bytes.

Part of the BI-as-code loop (ADR 0005): export → sanitize → commit.
Guarded by ``tests/unit/serving/superset/test_superset_assets.py``.
"""

from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

#: Fixed ZIP timestamp (the format's epoch) for deterministic archives.
_FIXED_ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)

#: Canonical bundle metadata: the assets importer's required type, no churn.
_CANONICAL_METADATA = b"version: 1.0.0\ntype: assets\n"

#: Default bundle directory (``superset/assets/`` of this repository).
DEFAULT_ASSETS_DIR = Path("superset") / "assets"


def is_database_entry(name: str) -> bool:
    """True for entries inside the bundle's ``databases/`` directory."""
    return "databases" in Path(name).parts


def sanitize_bundle(path: Path) -> tuple[int, int]:
    """Rewrite one bundle in place; return (changed_entries, kept_entries)."""
    with zipfile.ZipFile(path) as archive:
        current = [(info.filename, archive.read(info.filename)) for info in archive.infolist()]

    root = f"{path.stem}/"
    kept: list[tuple[str, bytes]] = []
    changed = 0
    for name, payload in current:
        _, separator, rest = name.partition("/")
        # top-level strays and directory-marker entries are dropped
        if not separator or not rest:
            changed += 1
            continue
        if is_database_entry(rest):
            changed += 1
            continue
        if rest == "metadata.yaml" and payload != _CANONICAL_METADATA:
            payload = _CANONICAL_METADATA
            changed += 1
        kept.append((f"{root}{rest}", payload))
    kept.sort(key=lambda item: item[0])

    if changed == 0 and current == kept:
        print(f"sanitize {path.name}: already clean, rewrite skipped")
        return 0, len(kept)

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, payload in kept:
            info = zipfile.ZipInfo(name, date_time=_FIXED_ZIP_TIMESTAMP)
            info.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(info, payload)
    tmp_path.replace(path)
    print(f"sanitize {path.name}: changed={changed} kept={len(kept)} root={root!r}")
    return changed, len(kept)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "bundles",
        nargs="*",
        type=Path,
        default=None,
        help=f"Bundle ZIPs (default: every *.zip under {DEFAULT_ASSETS_DIR}/)",
    )
    args = parser.parse_args(argv)
    bundles = args.bundles or sorted(DEFAULT_ASSETS_DIR.glob("*.zip"))
    if not bundles:
        print("sanitize: no bundles found")
        return 0
    for bundle in bundles:
        sanitize_bundle(bundle)
    return 0


if __name__ == "__main__":
    sys.exit(main())
