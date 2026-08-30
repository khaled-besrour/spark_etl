from __future__ import annotations

import pytest
from pyspark.sql import Row, SparkSession

from export.parquet_exporter import export_parquet, tables_to_export


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("test_parquet_exporter").getOrCreate()
    yield session
    session.stop()


def test_tables_to_export_lists_intermediate_and_training_tables() -> None:
    tables = tables_to_export()

    assert tables == [
        "intermediate.monthly_sales_by_product",
        "intermediate.daily_sales_by_product",
        "intermediate.monthly_sales_by_store",
        "intermediate.sales_by_day_name_by_product",
        "training.product_month_features",
    ]


def test_export_parquet_writes_under_schema_and_table_name(spark: SparkSession, tmp_path) -> None:
    df = spark.createDataFrame([Row(product_id="P001", total_sales=10)])

    path = export_parquet(df, str(tmp_path), "intermediate.monthly_sales_by_product")

    assert path == str(tmp_path) + "/intermediate/monthly_sales_by_product"
    reloaded = spark.read.parquet(path)
    assert reloaded.count() == df.count()
    assert set(reloaded.columns) == set(df.columns)
