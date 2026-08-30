from __future__ import annotations

import argparse
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
for _candidate in (_SCRIPT_DIR, _SCRIPT_DIR.parent):
    _candidate_str = str(_candidate)
    if _candidate_str in sys.path:
        sys.path.remove(_candidate_str)
    sys.path.insert(0, _candidate_str)

from settings import settings  # noqa: E402

FILE_FORMAT_NAME = "PUBLIC.PARQUET_FORMAT"


def discover_exports(export_dir: str) -> list[tuple[str, str, Path]]:
    root = Path(export_dir)
    if not root.is_dir():
        return []

    found = []
    for schema_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        for table_dir in sorted(p for p in schema_dir.iterdir() if p.is_dir()):
            if any(table_dir.glob("*.parquet")):
                found.append((schema_dir.name, table_dir.name, table_dir))
    return found


def connect():
    import snowflake.connector

    return snowflake.connector.connect(
        account=settings.snowflake_account,
        user=settings.snowflake_user,
        password=settings.snowflake_password,
        warehouse=settings.snowflake_warehouse,
        database=settings.snowflake_database,
        role=settings.snowflake_role or None,
    )


def stage_path(schema: str, table: str) -> str:
    return f"@~/{schema}/{table}"


def ensure_file_format(conn) -> None:
    conn.cursor().execute(f"CREATE FILE FORMAT IF NOT EXISTS {FILE_FORMAT_NAME} TYPE = PARQUET")


def ensure_schema(conn, schema: str) -> None:
    conn.cursor().execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")


def upload_parquet(conn, local_dir: Path, schema: str, table: str) -> None:
    local_glob = (local_dir / "*.parquet").as_posix()
    conn.cursor().execute(
        f"PUT file://{local_glob} {stage_path(schema, table)} "
        "OVERWRITE = TRUE AUTO_COMPRESS = FALSE"
    )


def ensure_table(conn, schema: str, table: str) -> None:
    conn.cursor().execute(
        f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table}
        USING TEMPLATE (
            SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
            FROM TABLE(
                INFER_SCHEMA(
                    LOCATION => '{stage_path(schema, table)}',
                    FILE_FORMAT => '{FILE_FORMAT_NAME}'
                )
            )
        )
        """
    )


def truncate_table(conn, schema: str, table: str) -> None:
    """Vide la table avant COPY INTO, qui n'ajoute que des lignes : sans ca,
    un re-run accumulerait des doublons."""
    conn.cursor().execute(f"TRUNCATE TABLE IF EXISTS {schema}.{table}")


def copy_into(conn, schema: str, table: str) -> list[dict]:
    """Renvoie le resultat de COPY INTO (fichier, lignes chargees/parsees,
    erreurs...), sinon un chargement a 0 ligne passe inapercu."""
    cursor = conn.cursor()
    cursor.execute(
        f"""
        COPY INTO {schema}.{table}
        FROM {stage_path(schema, table)}
        FILE_FORMAT = (FORMAT_NAME = {FILE_FORMAT_NAME})
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        PURGE = TRUE
        """
    )
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def load_table(conn, schema: str, table: str, local_dir: Path) -> list[dict]:
    ensure_schema(conn, schema)
    upload_parquet(conn, local_dir, schema, table)
    ensure_table(conn, schema, table)
    truncate_table(conn, schema, table)
    return copy_into(conn, schema, table)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-dir", default=None, help="Defaut : settings.parquet_export_dir.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    export_dir = args.export_dir or settings.parquet_export_dir

    if not settings.snowflake_account:
        print("SNOWFLAKE_ACCOUNT non renseigne (.env) : chargement Snowflake ignore.")
        return

    exports = discover_exports(export_dir)
    if not exports:
        print(f"Aucun export Parquet trouve dans {export_dir}.")
        return

    conn = connect()
    try:
        ensure_file_format(conn)
        for schema, table, local_dir in exports:
            print(f"Chargement de {local_dir} -> Snowflake {schema}.{table}...")
            results = load_table(conn, schema, table, local_dir)
            for result in results:
                print(f"  {result}")
            print(f"{schema}.{table} charge.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
