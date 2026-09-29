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
- normalizes dashboard/chart metadata that Superset 5 otherwise cannot
  hydrate after an import (layout root, native time filter, categorical
  ECharts axes, and the Histogram v2 column field);
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
from typing import Any

import yaml

#: Fixed ZIP timestamp (the format's epoch) for deterministic archives.
_FIXED_ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)

#: Canonical bundle metadata: the assets importer's required type, no churn.
_CANONICAL_METADATA = b"version: 1.0.0\ntype: assets\n"

#: Default bundle directory (``superset/assets/`` of this repository).
DEFAULT_ASSETS_DIR = Path("superset") / "assets"


def is_database_entry(name: str) -> bool:
    """True for entries inside the bundle's ``databases/`` directory."""
    return "databases" in Path(name).parts


def _yaml_payload(data: dict[str, Any]) -> bytes:
    return yaml.safe_dump(data, sort_keys=False, allow_unicode=True).encode()


def _dashboard_chart_ids(position: dict[str, Any]) -> list[int]:
    return sorted(
        component["meta"]["chartId"]
        for component in position.values()
        if isinstance(component, dict)
        and component.get("type") == "CHART"
        and isinstance(component.get("meta", {}).get("chartId"), int)
    )


def _normalize_dashboard(payload: bytes) -> bytes:
    dashboard = yaml.safe_load(payload)
    if not isinstance(dashboard, dict) or not isinstance(dashboard.get("position"), dict):
        raise ValueError("dashboard asset must carry a position mapping")

    position = dashboard["position"]
    root = position.get("ROOT_ID")
    if not isinstance(root, dict):
        raise ValueError("dashboard position must carry ROOT_ID")
    root_children = root.get("children")
    if not isinstance(root_children, list) or not root_children:
        if "GRID_ID" not in position:
            raise ValueError("dashboard ROOT_ID has no children and GRID_ID is missing")
        root["children"] = ["GRID_ID"]

    chart_ids = _dashboard_chart_ids(position)
    metadata = dashboard.get("metadata", {})
    if not isinstance(metadata, dict):
        raise ValueError("dashboard metadata must be a mapping")
    for native_filter in metadata.get("native_filter_configuration", []):
        if native_filter.get("filterType") == "filter_time_range":
            native_filter["filterType"] = "filter_time"
        scope = native_filter.get("scope", {})
        if scope.get("rootPath") == ["ROOT_ID"] and not scope.get("excluded"):
            native_filter["chartsInScope"] = chart_ids

    normalized = _yaml_payload(dashboard)
    return payload if yaml.safe_load(normalized) == yaml.safe_load(payload) else normalized


def _normalize_chart(payload: bytes) -> bytes:
    chart = yaml.safe_load(payload)
    if not isinstance(chart, dict) or not isinstance(chart.get("params"), dict):
        raise ValueError("chart asset must carry a params mapping")

    params = chart["params"]
    viz_type = chart.get("viz_type")
    groupby = params.get("groupby")
    if (
        isinstance(viz_type, str)
        and viz_type.startswith("echarts_timeseries_")
        and not params.get("x_axis")
        and isinstance(groupby, list)
        and len(groupby) == 1
    ):
        params["x_axis"] = groupby[0]
        params["groupby"] = []

    all_columns = params.get("all_columns")
    if (
        viz_type == "histogram_v2"
        and not params.get("column")
        and isinstance(all_columns, list)
        and len(all_columns) == 1
    ):
        params["column"] = all_columns[0]
        del params["all_columns"]

    normalized = _yaml_payload(chart)
    return payload if yaml.safe_load(normalized) == yaml.safe_load(payload) else normalized


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
        elif rest.startswith("dashboards/") and rest.endswith((".yaml", ".yml")):
            normalized = _normalize_dashboard(payload)
            if normalized != payload:
                payload = normalized
                changed += 1
        elif rest.startswith("charts/") and rest.endswith((".yaml", ".yml")):
            normalized = _normalize_chart(payload)
            if normalized != payload:
                payload = normalized
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
