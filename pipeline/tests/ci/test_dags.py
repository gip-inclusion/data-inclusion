from airflow.models import DagBag


def test_dags_generic():
    dagbag = DagBag()
    assert dagbag.import_errors == {}
    assert len(dagbag.dag_ids) > 0, "No DAGs found"
    for dag_id in dagbag.dag_ids:
        dag = dagbag.get_dag(dag_id=dag_id)
        assert dag is not None
        assert len(dag.tasks) >= 1
