from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from jobs.retail_sales_training_table import build_features

SCHEMA = T.StructType(
    [
        T.StructField("product_id", T.StringType(), nullable=True),
        T.StructField("store_id", T.StringType(), nullable=True),
        T.StructField("sales", T.IntegerType(), nullable=True),
        T.StructField("stock", T.IntegerType(), nullable=True),
        T.StructField("price", T.DoubleType(), nullable=True),
        T.StructField("month", T.IntegerType(), nullable=True),
        T.StructField("year", T.IntegerType(), nullable=True),
    ]
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_retail_sales_training_table")
        .getOrCreate()
    )
    yield session
    session.stop()


def _sample_df(spark: SparkSession):
    rows = [
        # P001 vendu dans 2 magasins distincts (S01, S02) en janvier 2024
        ("P001", "S01", 10, 50, 9.0, 1, 2024),
        ("P001", "S01", 5, 20, 11.0, 1, 2024),  # meme produit/magasin, 2e ligne
        ("P001", "S02", 3, 30, 10.0, 1, 2024),
        # P001, meme mois (janvier) mais annee differente : doit s'agreger avec
        # les lignes 2024 ci-dessus, plus de grain par annee.
        ("P001", "S01", 2, 15, 8.0, 1, 2023),
        # P002 : autre produit, meme mois, un seul magasin
        ("P002", "S01", 7, 40, 3.0, 1, 2024),
    ]
    return spark.createDataFrame(rows, schema=SCHEMA)


def test_build_features_counts_distinct_stores(spark: SparkSession) -> None:
    result = build_features(_sample_df(spark))

    row = result.filter(result.product_id == "P001").first()
    assert row["num_stores"] == 2  # S01 et S02, pas 3 (le nombre de lignes)


def test_build_features_sums_sales_and_stock(spark: SparkSession) -> None:
    result = build_features(_sample_df(spark))

    row = result.filter(result.product_id == "P001").first()
    assert row["total_sales"] == 20  # 10 + 5 + 3 + 2
    assert row["total_stock"] == 115  # 50 + 20 + 30 + 15


def test_build_features_averages_price(spark: SparkSession) -> None:
    result = build_features(_sample_df(spark))

    row = result.filter(result.product_id == "P001").first()
    assert row["avg_price"] == pytest.approx((9.0 + 11.0 + 10.0 + 8.0) / 4)


def test_build_features_grain_is_product_month(spark: SparkSession) -> None:
    """month seul (pas year) : deux lignes P001 de janvier, une en 2024 et
    une en 2023, doivent tomber dans le meme groupe."""
    result = build_features(_sample_df(spark))

    assert result.count() == 2  # un groupe par produit (P001 et P002)
    assert set(result.columns) == {
        "product_id",
        "month",
        "num_stores",
        "total_stock",
        "avg_price",
        "total_sales",
    }
