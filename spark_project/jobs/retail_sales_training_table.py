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


def build_features(df: DataFrame) -> DataFrame:
    return df.groupBy("product_id", "month").agg(
        F.countDistinct("store_id").alias("num_stores"),
        F.sum("stock").alias("total_stock"),
        F.avg("price").alias("avg_price"),
        F.sum("sales").alias("total_sales"),
    )


def build_spark_session(app_name: str = "retail_sales_training_table") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


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
    parser.add_argument(
        "--training-table", default=None, help="Defaut : settings.training_product_month_table."
    )
    parser.add_argument("--num-partitions", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    source_table = args.source_table or settings.staging_table
    training_table = args.training_table or settings.training_product_month_table

    spark = build_spark_session()
    try:
        staging = read_raw(spark, source_table, args.num_partitions)
        features = build_features(staging)
        load(features, training_table, args.num_partitions)
        print(f"Table d'entrainement ecrite dans {training_table}.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
