import pytest

from airflow.models import DagBag


@pytest.mark.parametrize("dag_id", DagBag().dag_ids)
def test_dags_generic(dag_id):
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id=dag_id)
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) >= 1
