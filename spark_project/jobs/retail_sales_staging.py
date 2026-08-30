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

from retail_sales_stats import PERTINENT_COLUMNS, read_raw  # noqa: E402
from settings import settings  # noqa: E402

# Colonnes utilisees pour le hash global_object_key.
HASH_COLUMNS = ["product_id", "store_id", "date", "price", "revenue"]

# Colonnes business traitees comme manquantes quand elles valent 0.
ZERO_INVALID_COLUMNS = ["price", "stock"]


def build_spark_session(app_name: str = "retail_sales_staging") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def add_global_object_key(df: DataFrame) -> DataFrame:
    """Hash stable de l'identite d'une ligne."""
    key_parts = [F.coalesce(F.col(c).cast("string"), F.lit("~NULL~")) for c in HASH_COLUMNS]
    return df.withColumn("global_object_key", F.sha2(F.concat_ws("|", *key_parts), 256))


def add_reject_reason(df: DataFrame) -> DataFrame:
    missing_labels = [F.when(F.col(c).isNull(), F.lit(c)) for c in PERTINENT_COLUMNS]
    zero_labels = [F.when(F.col(c) == 0, F.lit(f"{c}=0")) for c in ZERO_INVALID_COLUMNS]
    reasons = F.concat_ws(", ", *missing_labels, *zero_labels)
    return df.withColumn("reject_reason", F.when(reasons == "", None).otherwise(reasons))


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """`rejected_at` n'est ajoute qu'aux lignes rejetees."""
    valid = df.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = df.filter(F.col("reject_reason").isNotNull()).withColumn(
        "rejected_at", F.current_timestamp()
    )
    return valid, rejected


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
    parser.add_argument("--source-table", default=None, help="Defaut : settings.raw_table.")
    parser.add_argument("--num-partitions", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    source_table = args.source_table or settings.raw_table

    spark = build_spark_session()
    try:
        raw = read_raw(spark, source_table, args.num_partitions)
        with_key = add_global_object_key(raw)
        with_reason = add_reject_reason(with_key)
        valid, rejected = split_valid_rejected(with_reason)
        valid = valid.cache()
        rejected = rejected.cache()

        load(valid, settings.staging_table, args.num_partitions)
        load(rejected, settings.staging_rejected_table, args.num_partitions)

        print(f"{valid.count()} lignes valides -> {settings.staging_table}")
        print(f"{rejected.count()} lignes rejetees -> {settings.staging_rejected_table}")

        valid.unpersist()
        rejected.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
