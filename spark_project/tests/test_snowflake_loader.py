from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

from export.snowflake_loader import (
    copy_into,
    discover_exports,
    ensure_file_format,
    ensure_schema,
    ensure_table,
    load_table,
    main,
    stage_path,
    truncate_table,
    upload_parquet,
)


def _touch_parquet(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "part-00000.snappy.parquet").write_text("fake parquet content")
    (directory / "_SUCCESS").write_text("")  # cree par Spark, doit etre ignore


def test_discover_exports_finds_schema_table_pairs_with_parquet(tmp_path: Path) -> None:
    _touch_parquet(tmp_path / "intermediate" / "monthly_sales_by_product")
    _touch_parquet(tmp_path / "training" / "product_month_features")
    (tmp_path / "intermediate" / "empty_dir").mkdir(parents=True)  # pas de .parquet -> ignore

    found = discover_exports(str(tmp_path))

    pairs = {(schema, table) for schema, table, _ in found}
    assert pairs == {
        ("intermediate", "monthly_sales_by_product"),
        ("training", "product_month_features"),
    }


def test_discover_exports_returns_empty_list_if_dir_missing(tmp_path: Path) -> None:
    assert discover_exports(str(tmp_path / "does-not-exist")) == []


def test_stage_path_format() -> None:
    assert stage_path("intermediate", "monthly_sales_by_product") == (
        "@~/intermediate/monthly_sales_by_product"
    )


def test_ensure_file_format_executes_create_file_format() -> None:
    conn = MagicMock()
    ensure_file_format(conn)

    sql = conn.cursor().execute.call_args[0][0]
    assert "CREATE FILE FORMAT IF NOT EXISTS" in sql
    assert "TYPE = PARQUET" in sql
    # Qualifie avec PUBLIC : la connexion ne fixe pas de schema courant, un
    # nom non qualifie ferait echouer avec "no current schema" (bug reel deja
    # rencontre).
    assert "PUBLIC.PARQUET_FORMAT" in sql


def test_ensure_schema_executes_create_schema() -> None:
    conn = MagicMock()
    ensure_schema(conn, "intermediate")

    sql = conn.cursor().execute.call_args[0][0]
    assert "CREATE SCHEMA IF NOT EXISTS intermediate" in sql


def test_upload_parquet_puts_glob_with_overwrite(tmp_path: Path) -> None:
    conn = MagicMock()
    upload_parquet(conn, tmp_path, "intermediate", "monthly_sales_by_product")

    sql = conn.cursor().execute.call_args[0][0]
    assert sql.startswith("PUT file://")
    assert "*.parquet" in sql
    assert "@~/intermediate/monthly_sales_by_product" in sql
    assert "OVERWRITE = TRUE" in sql


def test_ensure_table_uses_infer_schema_from_stage() -> None:
    conn = MagicMock()
    ensure_table(conn, "intermediate", "monthly_sales_by_product")

    sql = conn.cursor().execute.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS intermediate.monthly_sales_by_product" in sql
    assert "INFER_SCHEMA" in sql
    assert "@~/intermediate/monthly_sales_by_product" in sql


def test_truncate_table_executes_truncate_if_exists() -> None:
    conn = MagicMock()
    truncate_table(conn, "intermediate", "monthly_sales_by_product")

    sql = conn.cursor().execute.call_args[0][0]
    assert sql == "TRUNCATE TABLE IF EXISTS intermediate.monthly_sales_by_product"


def test_copy_into_uses_match_by_column_name() -> None:
    conn = MagicMock()
    conn.cursor.return_value.description = [("file",), ("status",), ("rows_loaded",)]
    conn.cursor.return_value.fetchall.return_value = [("f.parquet", "LOADED", 42)]

    result = copy_into(conn, "intermediate", "monthly_sales_by_product")

    sql = conn.cursor().execute.call_args[0][0]
    assert "COPY INTO intermediate.monthly_sales_by_product" in sql
    assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in sql
    assert result == [{"file": "f.parquet", "status": "LOADED", "rows_loaded": 42}]


def test_copy_into_surfaces_zero_rows_loaded() -> None:
    """C'est exactement ce que l'ancien code laissait passer inapercu : un
    COPY INTO qui "reussit" (aucune exception) mais ne charge aucune ligne
    (colonnes qui ne matchent pas, table existante au schema incompatible
    issu d'un essai precedent, ...)."""
    conn = MagicMock()
    conn.cursor.return_value.description = [("file",), ("rows_loaded",)]
    conn.cursor.return_value.fetchall.return_value = [("f.parquet", 0)]

    result = copy_into(conn, "intermediate", "monthly_sales_by_product")

    assert result[0]["rows_loaded"] == 0


def test_load_table_runs_steps_in_order(tmp_path: Path) -> None:
    conn = MagicMock()
    conn.cursor.return_value.description = [("file",), ("rows_loaded",)]
    conn.cursor.return_value.fetchall.return_value = [("f.parquet", 10)]
    executed_sql = []
    conn.cursor.return_value.execute.side_effect = lambda sql: executed_sql.append(sql)

    results = load_table(conn, "intermediate", "monthly_sales_by_product", tmp_path)

    assert len(executed_sql) == 5
    assert "CREATE SCHEMA" in executed_sql[0]
    assert executed_sql[1].startswith("PUT file://")
    assert "CREATE TABLE" in executed_sql[2]
    assert "TRUNCATE TABLE" in executed_sql[3]
    assert "COPY INTO" in executed_sql[4]
    assert results == [{"file": "f.parquet", "rows_loaded": 10}]


def test_connect_passes_settings_to_snowflake_connector(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_connector_module = ModuleType("snowflake.connector")
    fake_connect = MagicMock()
    fake_connector_module.connect = fake_connect  # type: ignore[attr-defined]
    fake_snowflake_module = ModuleType("snowflake")
    fake_snowflake_module.connector = fake_connector_module  # type: ignore[attr-defined]

    monkeypatch.setitem(sys.modules, "snowflake", fake_snowflake_module)
    monkeypatch.setitem(sys.modules, "snowflake.connector", fake_connector_module)
    # settings est un dataclass frozen : on ne peut pas modifier un seul
    # attribut, on remplace tout le nom `settings` dans le module par un
    # objet equivalent (SimpleNamespace suffit, seuls les attributs lus par
    # connect() sont necessaires).
    fake_settings = SimpleNamespace(
        snowflake_account="myaccount",
        snowflake_user="user",
        snowflake_password="pwd",
        snowflake_warehouse="wh",
        snowflake_database="db",
        snowflake_role="",
    )
    monkeypatch.setattr("export.snowflake_loader.settings", fake_settings)

    from export.snowflake_loader import connect

    connect()

    fake_connect.assert_called_once()
    kwargs = fake_connect.call_args.kwargs
    assert kwargs["account"] == "myaccount"
    assert kwargs["role"] is None  # role vide -> None, pas une chaine vide


def test_main_skips_gracefully_without_snowflake_account(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Contrairement aux identifiants Kaggle (requis par tout le pipeline),
    Snowflake est une destination optionnelle : main() ne doit pas planter
    (ni tenter de se connecter) si SNOWFLAKE_ACCOUNT est vide, la valeur par
    defaut hors formation locale."""
    fake_settings = SimpleNamespace(snowflake_account="", parquet_export_dir="/unused")
    monkeypatch.setattr("export.snowflake_loader.settings", fake_settings)
    monkeypatch.setattr("sys.argv", ["snowflake_loader.py"])

    main()  # ne doit lever aucune exception

    assert "SNOWFLAKE_ACCOUNT" in capsys.readouterr().out
