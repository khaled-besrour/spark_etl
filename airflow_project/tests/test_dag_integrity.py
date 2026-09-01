from __future__ import annotations

from pathlib import Path

from airflow.dag_processing.dagbag import DagBag

DAGS_FOLDER = Path(__file__).resolve().parent.parent / "dags"


def _dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_FOLDER))


def test_dagbag_has_no_import_errors() -> None:
    dagbag = _dagbag()
    assert dagbag.import_errors == {}


def test_hello_world_dag_is_loaded() -> None:
    dagbag = _dagbag()
    assert "hello_world_dag" in dagbag.dags


def test_retail_sales_elt_dag_is_loaded() -> None:
    dagbag = _dagbag()
    assert "retail_sales_elt_dag" in dagbag.dags


def test_retail_sales_elt_dag_task_count() -> None:
    dagbag = _dagbag()
    dag = dagbag.dags["retail_sales_elt_dag"]
    assert len(dag.tasks) == 8
