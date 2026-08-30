from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_SCRIPT_DIR = Path(__file__).resolve().parent
for _candidate in (_SCRIPT_DIR, _SCRIPT_DIR.parent):
    _candidate_str = str(_candidate)
    if _candidate_str in sys.path:
        sys.path.remove(_candidate_str)
    sys.path.insert(0, _candidate_str)

from retail_sales_stats import read_raw  # noqa: E402
from settings import settings  # noqa: E402


def _agg_columns() -> list:
    """Agregats communs aux quatre vues. Fonction, pas constante : F.sum/
    F.avg ont besoin d'une SparkSession active."""
    return [
        F.sum("sales").alias("total_sales"),
        F.sum("revenue").alias("total_revenue"),
        F.avg("price").alias("avg_price"),
    ]


def build_spark_session(app_name: str = "retail_sales_intermediate") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def monthly_sales_by_product(df: DataFrame) -> DataFrame:
    return df.groupBy("product_id", "year", "month").agg(*_agg_columns())


def daily_sales_by_product(df: DataFrame) -> DataFrame:
    return df.groupBy("product_id", "date").agg(*_agg_columns())


def monthly_sales_by_store(df: DataFrame) -> DataFrame:
    return df.groupBy("store_id", "year", "month").agg(*_agg_columns())


def sales_by_day_name_by_product(df: DataFrame) -> DataFrame:
    return df.groupBy("product_id", "year", "day_name").agg(*_agg_columns())


def load(df: DataFrame, table: str, num_partitions: int = 8) -> None:
    (
        df.repartition(num_partitions)
        .write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", table)
        .option("user", settings.raw_db_user)
        .option("password", settings.raw_db_password)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-table", default=None, help="Defaut : settings.staging_table.")
    parser.add_argument("--num-partitions", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    source_table = args.source_table or settings.staging_table

    outputs = {
        "monthly_sales_by_product": settings.intermediate_monthly_by_product_table,
        "daily_sales_by_product": settings.intermediate_daily_by_product_table,
        "monthly_sales_by_store": settings.intermediate_monthly_by_store_table,
        "sales_by_day_name_by_product": settings.intermediate_day_name_by_product_table,
    }

    spark = build_spark_session()
    try:
        staging = read_raw(spark, source_table, args.num_partitions).cache()

        dataframes = {
            "monthly_sales_by_product": monthly_sales_by_product(staging),
            "daily_sales_by_product": daily_sales_by_product(staging),
            "monthly_sales_by_store": monthly_sales_by_store(staging),
            "sales_by_day_name_by_product": sales_by_day_name_by_product(staging),
        }

        for name, df in dataframes.items():
            table = outputs[name]
            load(df, table, args.num_partitions)
            print(f"Agregats ecrits dans {table}.")

        staging.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
