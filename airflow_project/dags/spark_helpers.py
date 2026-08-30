from __future__ import annotations

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"


def build_spark_submit_task(
    task_id: str,
    application: str,
    application_args: list[str] | None = None,
    *,
    conn_id: str = "spark_default",
    packages: str | None = POSTGRES_JDBC_PACKAGE,
    verbose: bool = True,
    conf: dict[str, str] | None = None,
    **kwargs,
) -> SparkSubmitOperator:
    merged_conf = {
        "spark.driver.host": "airflow-scheduler",
        "spark.driver.bindAddress": "0.0.0.0",
        **(conf or {}),
    }
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=conn_id,
        application=application,
        application_args=application_args,
        packages=packages,
        verbose=verbose,
        conf=merged_conf,
        **kwargs,
    )
