from __future__ import annotations

import argparse
import datetime as dt
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

from settings import settings  # noqa: E402

# Colonnes requises pour qu'une ligne soit exploitable en aval. Reutilise par
# retail_sales_staging.py pour rejeter les lignes incompletes.
PERTINENT_COLUMNS = [
    "product_id",
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
]


def build_spark_session(app_name: str = "retail_sales_stats") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def _date_bounds(spark: SparkSession, table: str) -> tuple[dt.date | None, dt.date | None]:
    """MIN/MAX de `date`, pour decouper la lecture en plages."""
    bounds_table = f"(SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM {table}) bounds"
    row = (
        spark.read.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", bounds_table)
        .option("user", settings.raw_db_user)
        .option("password", settings.raw_db_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .first()
    )
    return (row["min_date"], row["max_date"]) if row else (None, None)


def date_predicates(min_date: dt.date, max_date: dt.date, num_partitions: int) -> list[str]:
    span_days = max((max_date - min_date).days, 1)
    step = max(span_days // num_partitions, 1)

    predicates = ["date IS NULL"]
    start = min_date
    for i in range(num_partitions):
        is_last = i == num_partitions - 1
        end = max_date + dt.timedelta(days=1) if is_last else start + dt.timedelta(days=step)
        predicates.append(f"date >= DATE '{start}' AND date < DATE '{end}'")
        start = end
    return predicates


def read_raw(spark: SparkSession, table: str, num_partitions: int = 8) -> DataFrame:
    min_date, max_date = _date_bounds(spark, table)
    properties = {
        "user": settings.raw_db_user,
        "password": settings.raw_db_password,
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }

    if min_date is None or max_date is None:
        # Table vide : rien a partitionner.
        return spark.read.jdbc(url=settings.jdbc_url, table=table, properties=properties)

    predicates = date_predicates(min_date, max_date, num_partitions)
    return spark.read.jdbc(
        url=settings.jdbc_url, table=table, predicates=predicates, properties=properties
    )


def compute_stats(spark: SparkSession, df: DataFrame) -> DataFrame:
    total_rows = df.count()
    complete_rows = df.na.drop(how="any").count()
    pertinent_rows = df.na.drop(how="any", subset=PERTINENT_COLUMNS).count()

    null_counts = df.select(
        [F.count(F.when(F.col(c).isNull(), c)).alias(f"null_{c}") for c in df.columns]
    ).first()

    row = {
        "total_rows": total_rows,
        "complete_rows": complete_rows,
        "pertinent_rows": pertinent_rows,
        **null_counts.asDict(),
    }
    return spark.createDataFrame([row]).withColumn("computed_at", F.current_timestamp())


def load_stats(df: DataFrame, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", settings.jdbc_url)
        .option("dbtable", table)
        .option("user", settings.raw_db_user)
        .option("password", settings.raw_db_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-table", default=None, help="Defaut : settings.raw_table.")
    parser.add_argument("--stats-table", default=None, help="Defaut : settings.stats_table.")
    parser.add_argument("--num-partitions", type=int, default=8)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    source_table = args.source_table or settings.raw_table
    stats_table = args.stats_table or settings.stats_table

    spark = build_spark_session()
    try:
        raw = read_raw(spark, source_table, args.num_partitions)
        stats = compute_stats(spark, raw)
        stats.show(truncate=False, vertical=True)
        load_stats(stats, stats_table)
        print(f"Metriques ecrites dans {stats_table}.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
