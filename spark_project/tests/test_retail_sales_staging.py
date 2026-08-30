from __future__ import annotations

import datetime as dt

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from jobs.retail_sales_staging import add_global_object_key, add_reject_reason, split_valid_rejected

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
        SparkSession.builder.master("local[1]").appName("test_retail_sales_staging").getOrCreate()
    )
    yield session
    session.stop()


def _sample_df(spark: SparkSession):
    rows = [
        # valide
        ("P001", "S01", dt.date(2024, 1, 15), 10, 99.9, 50, 9.99, 1, 3, "Monday", 2024, "C01"),
        # sales manquant (pertinent) -> rejetee
        ("P002", "S01", dt.date(2024, 1, 16), None, 24.95, 12, 4.99, 1, 3, "Tuesday", 2024, "C01"),
        # store_id manquant (non pertinent) -> valide quand meme
        ("P003", None, dt.date(2024, 1, 17), 3, 10.0, 5, 3.33, 1, 3, "Wednesday", 2024, "C01"),
    ]
    return spark.createDataFrame(rows, schema=SCHEMA)


def test_global_object_key_is_deterministic_and_distinct(spark: SparkSession) -> None:
    result = add_global_object_key(_sample_df(spark))

    keys = [row["global_object_key"] for row in result.collect()]
    assert len(set(keys)) == 3
    assert all(len(k) == 64 for k in keys)  # sha2-256 en hexadecimal


def test_global_object_key_is_null_safe(spark: SparkSession) -> None:
    """Deux lignes qui ne different que par une valeur nulle dans la cle de
    hashage ne doivent pas produire le meme hash."""
    rows = [
        ("P001", "S01", dt.date(2024, 1, 15), 10, 99.9, 50, 9.99, 1, 3, "Monday", 2024, "C01"),
        ("P001", "S01", dt.date(2024, 1, 15), None, 99.9, 50, 9.99, 1, 3, "Monday", 2024, "C01"),
    ]
    df = spark.createDataFrame(rows, schema=SCHEMA)
    keys = [row["global_object_key"] for row in add_global_object_key(df).collect()]

    assert keys[0] != keys[1]


def test_reject_reason_flags_missing_pertinent_columns(spark: SparkSession) -> None:
    result = add_reject_reason(_sample_df(spark))

    reasons = {row["product_id"]: row["reject_reason"] for row in result.collect()}
    assert reasons["P001"] is None
    assert reasons["P002"] == "sales"
    assert reasons["P003"] is None  # store_id n'est pas une colonne pertinente


def test_reject_reason_flags_zero_business_values(spark: SparkSession) -> None:
    rows = [
        # stock a 0 : rejetee, meme si toutes les colonnes pertinentes sont renseignees
        ("P010", "S01", dt.date(2024, 1, 15), 10, 99.9, 0, 9.99, 1, 3, "Monday", 2024, "C01"),
        # sales ET revenue a 0 : les deux motifs apparaissent
        ("P011", "S01", dt.date(2024, 1, 15), 0, 0.0, 5, 9.99, 1, 3, "Monday", 2024, "C01"),
    ]
    df = spark.createDataFrame(rows, schema=SCHEMA)
    result = add_reject_reason(df)

    reasons = {row["product_id"]: row["reject_reason"] for row in result.collect()}
    assert reasons["P010"] == "stock=0"
    assert reasons["P011"] == "sales=0, revenue=0"


def test_split_valid_rejected(spark: SparkSession) -> None:
    with_reason = add_reject_reason(_sample_df(spark))
    valid, rejected = split_valid_rejected(with_reason)

    assert valid.count() == 2
    assert rejected.count() == 1
    assert "reject_reason" not in valid.columns


def test_split_valid_rejected_stamps_rejected_at_only_on_rejected(spark: SparkSession) -> None:
    """rejected_at ne doit apparaitre que sur les lignes rejetees : l'ajouter
    avant le split donnerait a tort un horodatage de rejet aux lignes valides."""
    with_reason = add_reject_reason(_sample_df(spark))
    valid, rejected = split_valid_rejected(with_reason)

    assert "rejected_at" not in valid.columns
    assert "rejected_at" in rejected.columns
    assert rejected.first()["rejected_at"] is not None
