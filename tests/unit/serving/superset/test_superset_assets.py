"""Unit tests for the committed Superset BI asset bundles and their sanitizer.

The BI-as-code loop (ADR 0005) commits import/export v1 ZIP bundles under
``superset/assets/``. The Superset export API includes database connection
YAMLs with full credentials — these tests pin the invariant that committed
bundles are sanitized (no ``databases/`` entries, no ``sqlalchemy_uri``) and
that ``superset_bundle_sanitize.py`` produces deterministic, importable
archives.
"""

import importlib.util
import sys
import uuid
import zipfile
from pathlib import Path
from types import ModuleType

import yaml

REPO_ROOT = Path(__file__).resolve().parents[4]
ASSETS_DIR = REPO_ROOT / "superset" / "assets"
SANITIZER_PATH = REPO_ROOT / "infrastructure" / "scripts" / "superset_bundle_sanitize.py"
INIT_SCRIPT_PATH = REPO_ROOT / "infrastructure" / "scripts" / "superset_init.py"


def _load_module(name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault(name, module)
    spec.loader.exec_module(module)
    return module


sanitizer = _load_module("superset_bundle_sanitize", SANITIZER_PATH)
superset_init = _load_module("superset_init_for_assets", INIT_SCRIPT_PATH)


def _make_export_zip(path: Path) -> None:
    """A miniature raw export: timestamped root, secret database YAML."""
    root = "dashboard_export_20260927T120000"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(f"{root}/", b"")
        archive.writestr(
            f"{root}/metadata.yaml",
            b"version: 1.0.0\ntype: Dashboard\ntimestamp: '2026-09-27T12:00:00+00:00'\n",
        )
        archive.writestr(
            f"{root}/databases/ClickHouse_analytics.yaml",
            b"database_name: ClickHouse analytics\n"
            b"sqlalchemy_uri: clickhousedb://superset_reader:secret@clickhouse:8123/analytics\n"
            b"uuid: 7c1a6c2e-9b3d-4f5a-8e42-1b0d5c9a3f70\nversion: 1.0.0\n",
        )
        archive.writestr(
            f"{root}/datasets/ClickHouse_analytics/mart_daily_sales.yaml",
            b"table_name: mart_daily_sales\nuuid: 11095e62-b038-4c5a-b176-eda27191ad4f\n",
        )
        archive.writestr(
            f"{root}/dashboards/Sales.yaml",
            b"dashboard_title: Sales\n"
            b"uuid: c6573898-eca4-44f9-a7e7-180f01217be7\n"
            b"position:\n"
            b"  ROOT_ID: {id: ROOT_ID, type: ROOT}\n"
            b"  GRID_ID: {id: GRID_ID, type: GRID, isRoot: true, children: []}\n"
            b"  CHART-1:\n"
            b"    id: CHART-1\n"
            b"    type: CHART\n"
            b"    meta: {chartId: 7, uuid: 62b2db2a-a8c4-4049-821c-9223f4f5f848}\n"
            b"metadata:\n"
            b"  native_filter_configuration:\n"
            b"  - filterType: filter_time_range\n"
            b"    scope: {rootPath: [ROOT_ID], excluded: []}\n"
            b"    chartsInScope: [99]\n",
        )


def _entry_names(archive_path: Path) -> list[str]:
    with zipfile.ZipFile(archive_path) as archive:
        return archive.namelist()


def _read_entry(archive_path: Path, name: str) -> bytes:
    with zipfile.ZipFile(archive_path) as archive:
        return archive.read(name)


class TestSanitizeBundle:
    def test_strips_databases_and_normalizes_metadata(self, tmp_path: Path) -> None:
        bundle = tmp_path / "sales_dashboard.zip"
        _make_export_zip(bundle)

        changed, kept = sanitizer.sanitize_bundle(bundle)

        assert changed == 4  # database + metadata + dashboard + dropped dir marker
        assert kept == 3
        names = _entry_names(bundle)
        assert all("databases" not in name for name in names), names
        assert all(name.startswith("sales_dashboard/") for name in names), names
        assert (
            _read_entry(bundle, "sales_dashboard/metadata.yaml")
            == b"version: 1.0.0\ntype: assets\n"
        )
        assert b"sqlalchemy_uri" not in _read_entry(
            bundle, "sales_dashboard/datasets/ClickHouse_analytics/mart_daily_sales.yaml"
        ) + _read_entry(bundle, "sales_dashboard/dashboards/Sales.yaml")
        dashboard = yaml.safe_load(_read_entry(bundle, "sales_dashboard/dashboards/Sales.yaml"))
        assert dashboard["position"]["ROOT_ID"]["children"] == ["GRID_ID"]
        native_filter = dashboard["metadata"]["native_filter_configuration"][0]
        assert native_filter["filterType"] == "filter_time"
        assert native_filter["chartsInScope"] == [7]

    def test_normalizes_superset_5_chart_control_fields(self) -> None:
        categorical = yaml.safe_dump(
            {
                "viz_type": "echarts_timeseries_bar",
                "params": {
                    "viz_type": "echarts_timeseries_bar",
                    "metrics": ["revenue"],
                    "groupby": ["category_name"],
                },
            },
            sort_keys=False,
        ).encode()
        histogram = yaml.safe_dump(
            {
                "viz_type": "histogram_v2",
                "params": {
                    "viz_type": "histogram_v2",
                    "all_columns": ["avg_order_value_eur"],
                },
            },
            sort_keys=False,
        ).encode()

        categorical_result = yaml.safe_load(sanitizer._normalize_chart(categorical))
        histogram_result = yaml.safe_load(sanitizer._normalize_chart(histogram))

        assert categorical_result["params"]["x_axis"] == "category_name"
        assert categorical_result["params"]["groupby"] == []
        assert histogram_result["params"]["column"] == "avg_order_value_eur"
        assert "all_columns" not in histogram_result["params"]

    def test_rewrite_is_deterministic_and_idempotent(self, tmp_path: Path) -> None:
        bundle = tmp_path / "sales_dashboard.zip"
        _make_export_zip(bundle)

        sanitizer.sanitize_bundle(bundle)
        first_bytes = bundle.read_bytes()

        result = sanitizer.sanitize_bundle(bundle)
        assert result == (0, 3), "second run must detect an already-clean bundle"
        assert bundle.read_bytes() == first_bytes, "sanitize must be byte-stable"

        # same content in a differently-named raw export converges to the
        # same bytes except for the root folder (which follows the stem)
        other = tmp_path / "other_bundle.zip"
        _make_export_zip(other)
        sanitizer.sanitize_bundle(other)
        with zipfile.ZipFile(other) as archive:
            other_entries = sorted(name.split("/", 1)[1] for name in archive.namelist())
        with zipfile.ZipFile(bundle) as archive:
            bundle_entries = sorted(name.split("/", 1)[1] for name in archive.namelist())
        assert other_entries == bundle_entries

    def test_zip_entries_carry_fixed_timestamp(self, tmp_path: Path) -> None:
        bundle = tmp_path / "sales_dashboard.zip"
        _make_export_zip(bundle)
        sanitizer.sanitize_bundle(bundle)
        with zipfile.ZipFile(bundle) as archive:
            for info in archive.infolist():
                assert info.date_time == (1980, 1, 1, 0, 0, 0)


class TestCommittedBundles:
    def test_committed_bundles_exist(self) -> None:
        bundles = sorted(ASSETS_DIR.glob("*.zip"))
        assert len(bundles) >= 2, "slice 2 commits the datasets + sales dashboard bundles"
        assert any("dashboard" in bundle.name for bundle in bundles)
        assert any("dataset" in bundle.name for bundle in bundles)

    def test_committed_bundles_carry_no_credentials(self) -> None:
        for bundle in sorted(ASSETS_DIR.glob("*.zip")):
            with zipfile.ZipFile(bundle) as archive:
                for info in archive.infolist():
                    assert "databases" not in Path(info.filename).parts, (
                        f"{bundle.name}:{info.filename} carries a database config"
                    )
                    payload = archive.read(info.filename)
                    assert b"sqlalchemy_uri" not in payload, (
                        f"{bundle.name}:{info.filename} carries a SQLAlchemy URI"
                    )
                    for proxy_env in (b"http_proxy", b"HTTPS_PROXY"):
                        assert proxy_env not in payload

    def test_committed_bundles_are_canonical_assets(self) -> None:
        for bundle in sorted(ASSETS_DIR.glob("*.zip")):
            with zipfile.ZipFile(bundle) as archive:
                names = archive.namelist()
                assert all(name.startswith(f"{bundle.stem}/") for name in names), (
                    f"{bundle.name}: entries must live under the stable stem root"
                )
                for info in archive.infolist():
                    assert info.date_time == (1980, 1, 1, 0, 0, 0), (
                        f"{bundle.name}:{info.filename} is not deterministically stamped"
                    )
                assert archive.read(f"{bundle.stem}/metadata.yaml") == (
                    b"version: 1.0.0\ntype: assets\n"
                )
            rest = [name.split("/", 1)[1] for name in names]
            assert any(name.startswith(("dashboards/", "datasets/")) for name in rest), (
                f"{bundle.name}: no dashboard/dataset content"
            )

    def test_committed_dashboard_layouts_and_controls_are_hydratable(self) -> None:
        for bundle in sorted(ASSETS_DIR.glob("*_dashboard.zip")):
            with zipfile.ZipFile(bundle) as archive:
                for name in archive.namelist():
                    rest = name.split("/", 1)[1]
                    if rest.startswith("dashboards/"):
                        dashboard = yaml.safe_load(archive.read(name))
                        position = dashboard["position"]
                        assert position["ROOT_ID"]["children"] == ["GRID_ID"], name
                        chart_ids = sanitizer._dashboard_chart_ids(position)
                        for native_filter in dashboard.get("metadata", {}).get(
                            "native_filter_configuration", []
                        ):
                            assert native_filter["filterType"] != "filter_time_range", name
                            scope = native_filter.get("scope", {})
                            if scope.get("rootPath") == ["ROOT_ID"] and not scope.get("excluded"):
                                assert native_filter["chartsInScope"] == chart_ids, name
                    elif rest.startswith("charts/"):
                        chart = yaml.safe_load(archive.read(name))
                        params = chart["params"]
                        if str(chart["viz_type"]).startswith("echarts_timeseries_"):
                            assert params.get("x_axis"), name
                        if chart["viz_type"] == "histogram_v2":
                            assert params.get("column"), name

    def test_customer_histogram_uses_a_numeric_calculated_column(self) -> None:
        """ClickHouse DECIMAL metadata is not accepted by Histogram v2 controls."""
        bundle = ASSETS_DIR / "customer_dashboard.zip"
        with zipfile.ZipFile(bundle) as archive:
            chart = yaml.safe_load(
                archive.read("customer_dashboard/charts/Avg_order_value_distribution_1.yaml")
            )
            dataset = yaml.safe_load(
                archive.read(
                    "customer_dashboard/datasets/ClickHouse_analytics/mart_customer_ltv.yaml"
                )
            )

        assert chart["params"]["column"] == "avg_order_value_eur_numeric"
        assert chart["params"]["adhoc_filters"] == [
            {
                "clause": "WHERE",
                "comparator": None,
                "expressionType": "SIMPLE",
                "filterOptionName": None,
                "isExtra": False,
                "operator": "IS NOT NULL",
                "sqlExpression": None,
                "subject": "avg_order_value_eur_numeric",
            }
        ]
        calculated_column = next(
            column
            for column in dataset["columns"]
            if column["column_name"] == "avg_order_value_eur_numeric"
        )
        assert calculated_column["type"] == "FLOAT"
        assert calculated_column["expression"] == "CAST(avg_order_value_eur AS Nullable(Float64))"


class TestConnectionIdentity:
    def test_fixed_connection_uuids_are_valid_and_distinct(self) -> None:
        values = [
            superset_init.CLICKHOUSE_CONNECTION_UUID,
            superset_init.TRINO_CONNECTION_UUID,
        ]
        for value in values:
            assert str(uuid.UUID(value)) == value, f"{value} is not a canonical UUID"
        assert len(set(values)) == len(values)

    def test_injected_database_config_uses_env_uri_and_fixed_uuid(self) -> None:
        config = superset_init._injected_clickhouse_database_config(
            "clickhousedb://user:secret@clickhouse:8123/analytics"
        )
        assert config["uuid"] == superset_init.CLICKHOUSE_CONNECTION_UUID
        assert config["database_name"] == superset_init.CLICKHOUSE_CONNECTION_NAME
        assert config["sqlalchemy_uri"] == "clickhousedb://user:secret@clickhouse:8123/analytics"
        assert config["allow_dml"] is False
        assert config["impersonate_user"] is False
