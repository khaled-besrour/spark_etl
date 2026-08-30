from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from jobs.retail_sales_intermediate import (
    daily_sales_by_product,
    monthly_sales_by_product,
    monthly_sales_by_store,
    sales_by_day_name_by_product,
)

SCHEMA = T.StructType(
    [
        T.StructField("product_id", T.StringType(), nullable=True),
        T.StructField("store_id", T.StringType(), nullable=True),
        T.StructField("date", T.DateType(), nullable=True),
        T.StructField("sales", T.IntegerType(), nullable=True),
        T.StructField("revenue", T.DoubleType(), nullable=True),
        T.StructField("price", T.DoubleType(), nullable=True),
        T.StructField("month", T.IntegerType(), nullable=True),
        T.StructField("year", T.IntegerType(), nullable=True),
        T.StructField("day_name", T.StringType(), nullable=True),
    ]
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_retail_sales_intermediate")
        .getOrCreate()
    )
    yield session
    session.stop()


def _sample_df(spark: SparkSession):
    rows = [
        # P001 / S01 : deux lundis de janvier (1er et 8), meme produit+magasin
        ("P001", "S01", dt.date(2024, 1, 1), 10, 100.0, 10.0, 1, 2024, "Monday"),
        ("P001", "S01", dt.date(2024, 1, 8), 4, 40.0, 10.0, 1, 2024, "Monday"),
        # P001 / S01 : un mardi, meme produit+magasin
        ("P001", "S01", dt.date(2024, 1, 2), 5, 50.0, 10.0, 1, 2024, "Tuesday"),
        # P001 / S02 : meme produit, autre magasin, un mercredi
        ("P001", "S02", dt.date(2024, 1, 3), 3, 30.0, 10.0, 1, 2024, "Wednesday"),
        # P002 / S01 : autre produit, meme magasin/mois, un lundi
        ("P002", "S01", dt.date(2024, 1, 1), 7, 21.0, 3.0, 1, 2024, "Monday"),
        # P001 / S01 : meme jour de semaine (Monday) mais annee differente ->
        # ne doit pas se retrouver agrege avec les lundis de 2024.
        ("P001", "S01", dt.date(2023, 1, 2), 100, 1000.0, 10.0, 1, 2023, "Monday"),
    ]
    return spark.createDataFrame(rows, schema=SCHEMA)


def test_monthly_sales_by_product_aggregates_across_stores(spark: SparkSession) -> None:
    result = monthly_sales_by_product(_sample_df(spark))

    row = result.filter(
        (result.product_id == "P001") & (result.year == 2024) & (result.month == 1)
    ).first()
    # P001 : 10 + 4 + 5 (S01) + 3 (S02) = 22, tous magasins confondus
    assert row["total_sales"] == 22
    assert row["total_revenue"] == 220.0


def test_daily_sales_by_product_aggregates_across_stores(spark: SparkSession) -> None:
    result = daily_sales_by_product(_sample_df(spark))

    row = result.filter(
        (result.product_id == "P001") & (result.date == dt.date(2024, 1, 1))
    ).first()
    assert row["total_sales"] == 10  # une seule ligne (S01) ce jour-la pour P001

    # P001 apparait sur 5 dates distinctes dans l'echantillon (1, 2, 3, 8 janvier
    # 2024, et 2 janvier 2023) -> 5 groupes (product_id, date).
    assert result.filter(result.product_id == "P001").count() == 5


def test_monthly_sales_by_store_aggregates_across_products(spark: SparkSession) -> None:
    result = monthly_sales_by_store(_sample_df(spark))

    row = result.filter(
        (result.store_id == "S01") & (result.year == 2024) & (result.month == 1)
    ).first()
    # S01 : 10 + 4 + 5 (P001) + 7 (P002) = 26, tous produits confondus
    assert row["total_sales"] == 26
    assert row["total_revenue"] == 211.0


def test_sales_by_day_name_by_product_aggregates_across_dates_and_stores(
    spark: SparkSession,
) -> None:
    result = sales_by_day_name_by_product(_sample_df(spark))

    monday_2024 = result.filter(
        (result.product_id == "P001") & (result.year == 2024) & (result.day_name == "Monday")
    ).first()
    # P001 / 2024 / Monday : 10 (1er janvier, S01) + 4 (8 janvier, S01) = 14
    assert monday_2024["total_sales"] == 14
    assert monday_2024["total_revenue"] == 140.0

    # Meme jour de semaine, annee differente (2023) : groupe separe, pas agrege
    # avec les lundis 2024 -- c'est le but d'inclure `year` dans le groupBy.
    monday_2023 = result.filter(
        (result.product_id == "P001") & (result.year == 2023) & (result.day_name == "Monday")
    ).first()
    assert monday_2023["total_sales"] == 100

    tuesday = result.filter(
        (result.product_id == "P001") & (result.year == 2024) & (result.day_name == "Tuesday")
    ).first()
    assert tuesday["total_sales"] == 5

    # P001 doit avoir un groupe par (annee, jour de semaine) distinct present
    # dans l'echantillon : 2024/Monday, 2024/Tuesday, 2024/Wednesday, 2023/Monday.
    assert result.filter(result.product_id == "P001").count() == 4
