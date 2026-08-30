from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def build_spark_session(app_name: str = "example_word_count") -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def count_words(spark: SparkSession, input_path: str) -> DataFrame:
    lines = spark.read.text(input_path)

    words = lines.select(
        F.explode(F.split(F.lower(F.col("value")), r"\s+")).alias("word")
    ).filter(F.col("word") != "")

    return words.groupBy("word").count().orderBy(F.col("count").desc())


def main(input_path: str) -> None:
    spark = build_spark_session()
    try:
        result = count_words(spark, input_path)
        result.show(20, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "data/sample.txt"
    main(path)
