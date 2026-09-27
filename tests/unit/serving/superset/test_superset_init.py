"""Unit tests for the Superset bootstrap decision logic.

The script is imported from ``infrastructure/scripts`` via importlib (it is
not part of the ``omni_retail`` package); only the pure functions are tested
here — the Superset API calls run in the one-shot ``superset-init`` container
and are covered by ``tests/integration/test_superset_bootstrap.py``.
"""

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

SCRIPT_PATH = (
    Path(__file__).resolve().parents[4] / "infrastructure" / "scripts" / "superset_init.py"
)


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("superset_init", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("superset_init", module)
    spec.loader.exec_module(module)
    return module


superset_init = _load_module()


class TestConnectionUris:
    def test_clickhouse_uri_uses_engine_and_reader_account(self) -> None:
        uri = superset_init.build_clickhouse_uri(
            user="superset_reader",
            password="reader-secret",
            host="clickhouse",
            port=8123,
            database="analytics",
        )
        assert uri == "clickhousedb://superset_reader:reader-secret@clickhouse:8123/analytics"

    def test_clickhouse_uri_quotes_credentials(self) -> None:
        uri = superset_init.build_clickhouse_uri(
            user="reader",
            password="p@ss/w:rd",
            host="clickhouse",
            port=8123,
            database="analytics",
        )
        assert uri.startswith("clickhousedb://reader:p%40ss%2Fw%3Ard@clickhouse:8123/analytics")

    def test_trino_uri_has_no_password_component(self) -> None:
        uri = superset_init.build_trino_uri(
            user="omni_superset", host="trino", port=8080, catalog="iceberg"
        )
        assert uri == "trino://omni_superset@trino:8080/iceberg"


class TestUserUpsertDecision:
    def test_missing_user_is_created(self) -> None:
        assert superset_init.decide_user_action(exists=False, password_matches=False) == "create"

    def test_existing_user_with_same_password_is_skipped(self) -> None:
        assert superset_init.decide_user_action(exists=True, password_matches=True) == "skip"

    def test_existing_user_with_new_password_is_reset(self) -> None:
        assert superset_init.decide_user_action(exists=True, password_matches=False) == (
            "reset_password"
        )


class TestDatabaseUpsertDecision:
    def test_missing_connection_is_created(self) -> None:
        assert superset_init.decide_database_action(exists=False, uri_matches=False) == "create"

    def test_existing_connection_with_same_uri_is_skipped(self) -> None:
        assert superset_init.decide_database_action(exists=True, uri_matches=True) == "skip"

    def test_existing_connection_with_new_uri_is_updated(self) -> None:
        assert superset_init.decide_database_action(exists=True, uri_matches=False) == "update"


class TestFindAssetBundles:
    def test_missing_directory_is_empty(self, tmp_path: Path) -> None:
        assert superset_init.find_asset_bundles(tmp_path / "assets") == []

    def test_empty_directory_is_empty(self, tmp_path: Path) -> None:
        assets = tmp_path / "assets"
        assets.mkdir()
        assert superset_init.find_asset_bundles(assets) == []

    def test_only_zip_bundles_are_returned_sorted(self, tmp_path: Path) -> None:
        assets = tmp_path / "assets"
        assets.mkdir()
        (assets / "sales_dashboard.zip").touch()
        (assets / "executive_dashboard.zip").touch()
        (assets / "README.md").write_text("not a bundle", encoding="utf-8")
        (assets / "nested").mkdir()
        assert superset_init.find_asset_bundles(assets) == [
            assets / "executive_dashboard.zip",
            assets / "sales_dashboard.zip",
        ]
