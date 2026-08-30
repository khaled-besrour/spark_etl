from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import SparkSession

from jobs.retail_sales_elt import SALES_SCHEMA, STORE_CITIES_SCHEMA, transform


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("test_retail_sales_elt").getOrCreate()
    yield session
    session.stop()


def _sales_df(spark: SparkSession, rows=None):
    rows = rows or [
        # product_id, store_id, date, sales, revenue, stock, price, promo_type_1,
        # promo_bin_1, promo_type_2, promo_bin_2, promo_discount_2, promo_discount_type_2
        ("P001", "S01", "2024-01-15", 10.0, 99.9, 50.0, 9.99, None, None, None, None, None, None),
        ("P002", "S02", "2024-01-16", 5.0, 24.95, 12.0, 4.99, None, None, None, None, None, None),
    ]
    return spark.createDataFrame(rows, schema=SALES_SCHEMA)


def _store_cities_df(spark: SparkSession):
    rows = [
        ("S01", "type_a", "120", "C01"),
    ]
    return spark.createDataFrame(rows, schema=STORE_CITIES_SCHEMA)


def test_transform_joins_city_id_from_store_cities(spark: SparkSession) -> None:
    result = transform(_sales_df(spark), _store_cities_df(spark))

    row = result.filter(result.product_id == "P001").first()
    assert row["city_id"] == "C01"


def test_transform_left_join_keeps_unmatched_store(spark: SparkSession) -> None:
    """S02 n'existe pas dans store_cities : la ligne doit etre gardee (raw ne
    filtre rien), city_id simplement null."""
    result = transform(_sales_df(spark), _store_cities_df(spark))

    row = result.filter(result.product_id == "P002").first()
    assert row is not None
    assert row["city_id"] is None


def test_transform_derives_date_parts(spark: SparkSession) -> None:
    result = transform(_sales_df(spark), _store_cities_df(spark))

    row = result.filter(result.product_id == "P001").first()
    assert row["date"] == dt.date(2024, 1, 15)
    assert row["year"] == 2024
    assert row["month"] == 1
    assert row["day_name"] == "Monday"


def test_transform_malformed_date_returns_null_instead_of_raising(spark: SparkSession) -> None:
    rows = [
        ("P003", "S01", "19", 1.0, 1.0, 1.0, 1.0, None, None, None, None, None, None),
    ]
    result = transform(_sales_df(spark, rows), _store_cities_df(spark))

    row = result.first()
    assert row["date"] is None
    assert row["year"] is None


def test_transform_adds_ingestion_metadata_and_drops_unused_columns(spark: SparkSession) -> None:
    result = transform(_sales_df(spark), _store_cities_df(spark))

    assert "ingested_at" in result.columns
    assert "promo_type_1" not in result.columns
    assert "storetype_id" not in result.columns
