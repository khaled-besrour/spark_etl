"""Exporte en Parquet les tables intermediate/training alimentees par
jobs/retail_sales_intermediate.py et jobs/retail_sales_training_table.py.

Job Spark distinct : les jobs sous jobs/ ne font que de l'injection en base,
celui-ci relit ces tables pour produire les fichiers consommes par
export/snowflake_loader.py.

Usage :
    spark-submit /opt/export/parquet_exporter.py --export-dir /opt/spark-data/exports
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

_SCRIPT_DIR = Path(__file__).resolve().parent
for _candidate in (_SCRIPT_DIR, _SCRIPT_DIR.parent):
    _candidate_str = str(_candidate)
    if _candidate_str in sys.path:
        sys.path.remove(_candidate_str)
    sys.path.insert(0, _candidate_str)

from settings import settings  # noqa: E402


def build_spark_session(app_name: str = "parquet_exporter") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # v2 : chaque tache renomme son fichier elle-meme, au lieu de v1 qui
        # delegue au driver. Le driver (conteneur Airflow) et les executors
        # (conteneurs spark-worker) sont sur des volumes Docker differents --
        # v1 echoue avec "Failed to rename ...".
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2").getOrCreate()
    )


def tables_to_export() -> list[str]:
    """Tables intermediate/training a exporter. Pas raw/staging : trop
    volumineuses, sans interet en Parquet pour un notebook/modele."""
    return [
        settings.intermediate_monthly_by_product_table,
        settings.intermediate_daily_by_product_table,
        settings.intermediate_monthly_by_store_table,
        settings.intermediate_day_name_by_product_table,
        settings.training_product_month_table,
    ]


def read_table(spark: SparkSession, table: str) -> DataFrame:
    """Lecture simple, pas besoin de partitionnement : tables d'agregats,
    petites, et la plupart n'ont pas de colonne `date`."""
    return (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", table)
        .option("user", settings.raw_db_user)
        .option("password", settings.raw_db_password)
        .option("driver", "org.postgresql.Driver")
        .option("fetchsize", "10000")
        .load()
    )


def export_parquet(df: DataFrame, export_dir: str, table: str) -> str:
    """Export sous {export_dir}/{schema}/{name}. overwrite : recalcule a
    chaque run."""
    schema, name = table.split(".", 1)
    path = f"{export_dir}/{schema}/{name}"
    df.write.mode("overwrite").parquet(path)
    return path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-dir", default=None, help="Defaut : settings.parquet_export_dir.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    export_dir = args.export_dir or settings.parquet_export_dir

    spark = build_spark_session()
    try:
        for table in tables_to_export():
            df = read_table(spark, table)
            path = export_parquet(df, export_dir, table)
            print(f"{table} exporte vers {path}.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
