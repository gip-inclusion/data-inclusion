import itertools
import logging

import airflow
import pendulum
from airflow.models import Variable
from airflow.operators import empty, python
from airflow.providers.postgres.hooks import postgres

logger = logging.getLogger(__name__)

default_args = {}


def _publish_to_datagouv():
    import io

    from data_inclusion.scripts.tasks import datagouv
    from data_inclusion.scripts.tasks.utils import log_df_info

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    DATAGOUV_API_URL = Variable.get("DATAGOUV_API_URL")
    DATAGOUV_API_KEY = Variable.get("DATAGOUV_API_KEY")
    DATAGOUV_DI_DATASET_ID = Variable.get("DATAGOUV_DI_DATASET_ID")
    DATAGOUV_DI_RESOURCE_IDS = Variable.get(
        "DATAGOUV_DI_RESOURCE_IDS", deserialize_json=True
    )

    datagouv_client = datagouv.DataGouvClient(
        base_url=DATAGOUV_API_URL,
        api_key=DATAGOUV_API_KEY,
    )

    kinds_formats_product = itertools.product(
        ["structures", "services"],
        ["csv", "json", "xlsx"],
    )
    date_str = pendulum.today().to_date_string()

    # 1. fetch data
    structures_df = pg_hook.get_pandas_df(
        sql="SELECT * FROM public_opendata.opendata_structures",
    )
    log_df_info(structures_df, logger)

    services_df = pg_hook.get_pandas_df(
        sql="SELECT * FROM public_opendata.opendata_services",
    )
    log_df_info(services_df, logger)

    for kind, format in kinds_formats_product:
        df = structures_df if kind == "structures" else services_df

        to_buf_fn_by_format = {
            "json": lambda df, buf: df.to_json(
                buf, orient="records", force_ascii=False
            ),
            "csv": lambda df, buf: df.to_csv(buf, index=False),
            "xlsx": lambda df, buf: df.to_excel(buf, engine="xlsxwriter"),
        }

        with io.BytesIO() as buf:
            # 2. serialize data
            to_buf_fn_by_format[format](df, buf)

            # 3. upload
            datagouv_client.upload_dataset_resource(
                dataset_id=DATAGOUV_DI_DATASET_ID,
                resource_id=DATAGOUV_DI_RESOURCE_IDS[kind][format],
                buf=buf,
                filename=f"{kind}-inclusion-{date_str}.{format}",
            )


EVERY_MONDAY_AT_2PM = "0 14 * * 1"

with airflow.DAG(
    dag_id="publish",
    description="Publish the consolidated dataset to datagouv",
    start_date=pendulum.datetime(2022, 1, 1),
    default_args=default_args,
    schedule_interval=None,
    # schedule_interval=EVERY_MONDAY_AT_2PM,
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    publish_to_datagouv = python.PythonOperator(
        task_id="publish",
        python_callable=_publish_to_datagouv,
    )

    start >> publish_to_datagouv >> end
