"""Tests generiques : verifient que les DAGs se chargent sans erreur,
qu'ils n'ont pas de cycles et qu'ils respectent quelques bonnes pratiques
minimales (tags, retries, pas d'import errors).
"""

from __future__ import annotations

import os

import pytest
from airflow.models import DagBag

DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


def test_no_import_errors(dagbag: DagBag) -> None:
    assert not dagbag.import_errors, (
        f"Erreurs d'import detectees dans les DAGs : {dagbag.import_errors}"
    )


def test_at_least_one_dag_loaded(dagbag: DagBag) -> None:
    assert len(dagbag.dags) > 0, "Aucun DAG n'a ete trouve dans le dossier dags/."


def test_hello_world_dag_exists(dagbag: DagBag) -> None:
    assert "hello_world_dag" in dagbag.dags


def test_retail_sales_elt_dag_exists(dagbag: DagBag) -> None:
    assert "retail_sales_elt_dag" in dagbag.dags


def test_dags_have_tags(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"Le DAG '{dag_id}' n'a pas de tags."


def test_dags_have_retries(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        retries = dag.default_args.get("retries")
        assert retries is not None, f"Le DAG '{dag_id}' ne definit pas de retries par defaut."
