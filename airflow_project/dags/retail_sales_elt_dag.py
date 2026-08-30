from __future__ import annotations

import datetime as dt
import subprocess
import sys

from airflow.decorators import dag, task

from spark_helpers import build_spark_submit_task

KAGGLE_DATASET = "berkayalan/retail-sales-data"
RAW_DATA_DIR = "/opt/spark-data/raw/retail_sales"

default_args = {
    "owner": "formation",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=2),
}


@dag(
    dag_id="retail_sales_elt_dag",
    description="ELT : dataset Kaggle Retail Sales Data -> table raw Postgres (via Spark)",
    default_args=default_args,
    schedule="@weekly",
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    tags=["formation", "elt", "kaggle", "spark"],
)
def retail_sales_elt_dag():
    @task
    def download_retail_sales() -> str:
        result = subprocess.run(
            [
                sys.executable,
                "/opt/ingestion/kaggle_download.py",
                "--dataset",
                KAGGLE_DATASET,
                "--dest",
                RAW_DATA_DIR,
            ],
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        print(result.stderr)
        result.check_returncode()
        return RAW_DATA_DIR

    submit_retail_sales_elt = build_spark_submit_task(
        task_id="RAW_injection",
        application="/opt/spark-jobs/retail_sales_elt.py",
        application_args=["--input", RAW_DATA_DIR],
    )

    submit_retail_sales_stats = build_spark_submit_task(
        task_id="RAW_stats",
        application="/opt/spark-jobs/retail_sales_stats.py",
    )

    submit_retail_sales_staging = build_spark_submit_task(
        task_id="Staging_filters",
        application="/opt/spark-jobs/retail_sales_staging.py",
    )

    submit_retail_sales_intermediate = build_spark_submit_task(
        task_id="Intermediate_feature",
        application="/opt/spark-jobs/retail_sales_intermediate.py",
    )

    submit_retail_sales_training_table = build_spark_submit_task(
        task_id="Training_feature",
        application="/opt/spark-jobs/retail_sales_training_table.py",
    )

    submit_parquet_export = build_spark_submit_task(
        task_id="Parquet_export",
        application="/opt/export/parquet_exporter.py",
    )

    @task
    def load_to_snowflake() -> None:
        result = subprocess.run(
            [sys.executable, "/opt/export/snowflake_loader.py"],
            capture_output=True,
            text=True,
        )
        print(result.stdout)
        print(result.stderr)
        result.check_returncode()

    download_retail_sales() >> submit_retail_sales_elt
    submit_retail_sales_elt >> [submit_retail_sales_stats, submit_retail_sales_staging]
    submit_retail_sales_staging >> [
        submit_retail_sales_intermediate,
        submit_retail_sales_training_table,
    ]
    (
        [
            submit_retail_sales_intermediate,
            submit_retail_sales_training_table,
        ]
        >> submit_parquet_export
        >> load_to_snowflake()
    )


retail_sales_elt_dag()
