from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from jobs.retail_sales_stats import compute_stats, date_predicates

SCHEMA = T.StructType(
    [
        T.StructField("product_id", T.StringType(), nullable=True),
        T.StructField("store_id", T.StringType(), nullable=True),
        T.StructField("date", T.DateType(), nullable=True),
        T.StructField("sales", T.IntegerType(), nullable=True),
        T.StructField("revenue", T.DoubleType(), nullable=True),
        T.StructField("stock", T.IntegerType(), nullable=True),
        T.StructField("price", T.DoubleType(), nullable=True),
        T.StructField("month", T.IntegerType(), nullable=True),
        T.StructField("week", T.IntegerType(), nullable=True),
        T.StructField("day_name", T.StringType(), nullable=True),
        T.StructField("year", T.IntegerType(), nullable=True),
        T.StructField("city_id", T.StringType(), nullable=True),
    ]
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]").appName("test_retail_sales_stats").getOrCreate()
    )
    yield session
    session.stop()


def _sample_df(spark: SparkSession):
    rows = [
        # complete : toutes colonnes renseignees
        ("P001", "S01", dt.date(2024, 1, 15), 10, 99.9, 50, 9.99, 1, 3, "Monday", 2024, "C01"),
        # sales manquant (pertinent) -> ni complete ni pertinente
        ("P002", "S01", dt.date(2024, 1, 16), None, 24.95, 12, 4.99, 1, 3, "Tuesday", 2024, "C01"),
        # store_id manquant (non pertinent) -> pertinente mais pas complete
        ("P003", None, dt.date(2024, 1, 17), 3, 10.0, 5, 3.33, 1, 3, "Wednesday", 2024, "C01"),
    ]
    return spark.createDataFrame(rows, schema=SCHEMA)


def test_compute_stats_counts_total_complete_and_pertinent(spark: SparkSession) -> None:
    stats = compute_stats(spark, _sample_df(spark)).first()

    assert stats["total_rows"] == 3
    assert stats["complete_rows"] == 1
    assert stats["pertinent_rows"] == 2


def test_compute_stats_null_counts_per_column(spark: SparkSession) -> None:
    stats = compute_stats(spark, _sample_df(spark)).first()

    assert stats["null_sales"] == 1
    assert stats["null_store_id"] == 1
    assert stats["null_product_id"] == 0


def test_date_predicates_includes_a_null_catching_partition() -> None:
    predicates = date_predicates(dt.date(2024, 1, 1), dt.date(2024, 1, 31), num_partitions=4)

    assert predicates[0] == "date IS NULL"
    assert len(predicates) == 5  # 4 plages de dates + 1 pour les nulls


def test_date_predicates_range_predicates_are_well_formed(spark: SparkSession) -> None:
    """Verifie que les plages ne se chevauchent pas et couvrent bien
    [min_date, max_date] : construit un DataFrame avec une ligne par jour de
    janvier et verifie qu'appliquer chaque predicat (sauf celui des nulls)
    et sommer les comptes redonne le total, sans doublon ni trou."""
    schema = T.StructType([T.StructField("date", T.DateType(), nullable=True)])
    rows = [(dt.date(2024, 1, d),) for d in range(1, 32)]
    df = spark.createDataFrame(rows, schema=schema)

    predicates = date_predicates(dt.date(2024, 1, 1), dt.date(2024, 1, 31), num_partitions=4)
    range_predicates = predicates[1:]  # exclut "date IS NULL"

    counts = [df.where(p).count() for p in range_predicates]
    assert sum(counts) == 31
    assert all(c > 0 for c in counts)
