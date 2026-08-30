from __future__ import annotations

import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

_SCRIPT_DIR = Path(__file__).resolve().parent
for _candidate in (_SCRIPT_DIR, _SCRIPT_DIR.parent):
    _candidate_str = str(_candidate)
    if _candidate_str in sys.path:
        sys.path.remove(_candidate_str)
    sys.path.insert(0, _candidate_str)

from settings import settings  # noqa: E402

SALES_SCHEMA = T.StructType(
    [
        T.StructField("product_id", T.StringType(), nullable=True),
        T.StructField("store_id", T.StringType(), nullable=True),
        T.StructField("date", T.StringType(), nullable=True),
        T.StructField("sales", T.DoubleType(), nullable=True),
        T.StructField("revenue", T.DoubleType(), nullable=True),
        T.StructField("stock", T.DoubleType(), nullable=True),
        T.StructField("price", T.DoubleType(), nullable=True),
        T.StructField("promo_type_1", T.StringType(), nullable=True),
        T.StructField("promo_bin_1", T.StringType(), nullable=True),
        T.StructField("promo_type_2", T.StringType(), nullable=True),
        T.StructField("promo_bin_2", T.StringType(), nullable=True),
        T.StructField("promo_discount_2", T.StringType(), nullable=True),
        T.StructField("promo_discount_type_2", T.StringType(), nullable=True),
    ]
)

STORE_CITIES_SCHEMA = T.StructType(
    [
        T.StructField("store_id", T.StringType(), nullable=True),
        T.StructField("storetype_id", T.StringType(), nullable=True),
        T.StructField("store_size", T.StringType(), nullable=True),
        T.StructField("city_id", T.StringType(), nullable=True),
    ]
)


def build_spark_session(app_name: str = "retail_sales_elt") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def extract_sales(spark: SparkSession, input_dir: str) -> DataFrame:
    """Lecture de sales.csv avec un schema explicite (pas d'inferSchema)."""
    path = str(Path(input_dir) / "sales.csv")
    return spark.read.option("header", "true").schema(SALES_SCHEMA).csv(path)


def extract_store_cities(spark: SparkSession, input_dir: str) -> DataFrame:
    path = str(Path(input_dir) / "store_cities.csv")
    return spark.read.option("header", "true").schema(STORE_CITIES_SCHEMA).csv(path)


def transform(sales: DataFrame, store_cities: DataFrame) -> DataFrame:
    """Chargement "raw" : pas de filtrage ici, c'est le role du staging.
    Joint city_id depuis store_cities, derive year/month/week/day_name.
    try_to_date (pas to_date) renvoie null sur une date malformee au lieu
    de planter (Spark 4 est en mode ANSI par defaut)."""
    with_city = sales.join(store_cities.select("store_id", "city_id"), on="store_id", how="left")
    with_date = with_city.withColumn("date", F.try_to_date(F.col("date")))

    return (
        with_date.withColumn("year", F.year(F.col("date")))
        .withColumn("month", F.month(F.col("date")))
        .withColumn("week", F.weekofyear(F.col("date")))
        .withColumn("day_name", F.date_format(F.col("date"), "EEEE"))
        .select(
            "product_id",
            "store_id",
            "date",
            "sales",
            "revenue",
            "stock",
            "price",
            "month",
            "week",
            "day_name",
            "year",
            "city_id",
        )
        .withColumn("ingested_at", F.current_timestamp())
    )


def load(
    df: DataFrame,
    jdbc_url: str,
    table: str,
    user: str,
    password: str,
    mode: str = "append",
    num_partitions: int = 8,
) -> None:
    """Repartition avant l'ecriture pour paralleliser les connexions JDBC."""
    (
        df.repartition(num_partitions)
        .write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", required=True, help="Dossier contenant sales.csv et store_cities.csv."
    )
    parser.add_argument("--mode", default="append", choices=["append", "overwrite"])
    parser.add_argument("--num-partitions", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    spark = build_spark_session()
    try:
        sales = extract_sales(spark, args.input)
        store_cities = extract_store_cities(spark, args.input)
        clean = transform(sales, store_cities).cache()
        load(
            clean,
            jdbc_url=settings.jdbc_url,
            table=settings.raw_table,
            user=settings.raw_db_user,
            password=settings.raw_db_password,
            mode=args.mode,
            num_partitions=args.num_partitions,
        )
        print(f"{clean.count()} lignes chargees dans {settings.raw_table}.")
        clean.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
