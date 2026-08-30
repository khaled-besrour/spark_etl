from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession

from jobs.example_word_count import count_words

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "sample.txt")


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = (
        SparkSession.builder.master("local[1]")
        .appName("test_example_word_count")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_count_words_returns_expected_columns(spark: SparkSession) -> None:
    result = count_words(spark, DATA_PATH)
    assert result.columns == ["word", "count"]


def test_count_words_finds_known_word(spark: SparkSession) -> None:
    result = count_words(spark, DATA_PATH)
    rows = {row["word"]: row["count"] for row in result.collect()}
    assert rows.get("airflow") == 2
    assert rows.get("spark") == 2
