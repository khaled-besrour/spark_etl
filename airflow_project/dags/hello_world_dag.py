from __future__ import annotations

import datetime as dt

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from spark_helpers import build_spark_submit_task

default_args = {
    "owner": "formation",
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}


@dag(
    dag_id="hello_world_dag",
    description="DAG d'exemple pour la formation Airflow",
    default_args=default_args,
    schedule="@daily",
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    tags=["formation", "exemple"],
)
def hello_world_dag():
    @task
    def say_hello() -> str:
        message = "Hello World depuis Airflow !"
        print(message)
        return message

    print_date = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

    @task
    def say_goodbye(previous_message: str) -> None:
        print(f"Message precedent : {previous_message}")
        print("Fin du DAG hello_world_dag.")

    submit_spark_word_count = build_spark_submit_task(
        task_id="submit_spark_word_count",
        application="/opt/spark-jobs/example_word_count.py",
        application_args=["/opt/spark-data/sample.txt"],
        packages=None,
    )

    hello = say_hello()
    goodbye = say_goodbye(hello)

    hello >> print_date >> goodbye >> submit_spark_word_count


hello_world_dag()
