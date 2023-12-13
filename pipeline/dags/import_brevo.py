import logging

import airflow
import pendulum
from airflow.operators import empty, python

from dag_utils import date
from dag_utils.dbt import dbt_operator_factory
from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}

SOURCE_ID = "brevo"
STREAM_ID = "contacts"


def _extract_rgpd_contacts(run_id: str, stream_id: str, source_id: str, logical_date):
    import json

    from airflow.models import Variable

    from data_inclusion.scripts.tasks import brevo

    from dag_utils import constants, s3

    brevo_client = brevo.BrevoClient(token=Variable.get("BREVO_API_KEY"))
    contacts = list(brevo_client.list_contacts(constants.BREVO_ALL_CONTACTS_LIST_ID))
    s3.store_content(
        s3.source_file_path(source_id, f"{stream_id}.json", run_id, logical_date),
        json.dumps(contacts).encode(),
    )


def _load_rgpd_contacts(run_id: str, stream_id: str, source_id: str, logical_date):
    import pandas as pd

    from dags.dag_utils import date, pg, s3
    from data_inclusion.scripts.tasks import utils

    s3_path = s3.source_file_path(source_id, f"{stream_id}.json", run_id, logical_date)
    tmp_filename = s3.download_file(s3_path)
    df = utils.read_json(tmp_filename)

    df = pd.DataFrame().assign(data=df.apply(lambda row: row.to_dict(), axis="columns"))
    df = df.assign(_di_batch_id=run_id)
    df = df.assign(_di_logical_date=date.local_date_str(logical_date))

    pg.create_schema(source_id)
    pg.load_source_df(source_id, stream_id, df)


with airflow.DAG(
    dag_id="import_brevo",
    start_date=pendulum.datetime(2023, 1, 1, tz=date.TIME_ZONE),
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    concurrency=1,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    extract_rgpd_contacts = python.ExternalPythonOperator(
        task_id="extract_rgpd_contacts",
        python=str(PYTHON_BIN_PATH),
        python_callable=_extract_rgpd_contacts,
        op_kwargs={"stream_id": STREAM_ID, "source_id": SOURCE_ID},
    )

    load_rgpd_contacts = python.ExternalPythonOperator(
        task_id="load_rgpd_contacts",
        python=str(PYTHON_BIN_PATH),
        python_callable=_load_rgpd_contacts,
        op_kwargs={"stream_id": STREAM_ID, "source_id": SOURCE_ID},
    )

    dbt_build_brevo = dbt_operator_factory(
        task_id="dbt_build_brevo",
        command="build",
        select="path:models/staging/brevo path:models/intermediate/brevo",
    )

    (start >> extract_rgpd_contacts >> load_rgpd_contacts >> dbt_build_brevo >> end)
