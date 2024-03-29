import pendulum

import airflow
from airflow.operators import empty, python

from dag_utils.virtualenvs import PYTHON_BIN_PATH

default_args = {}


def _publish_to_datagouv():
    import io
    import itertools
    import logging
    import tempfile

    import geopandas as gpd
    import pendulum

    from airflow.models import Variable
    from airflow.providers.postgres.hooks import postgres

    from dag_utils.sources import datagouv, utils

    logger = logging.getLogger(__name__)

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

    date_str = pendulum.today().to_date_string()

    def to_geojson(df, buf) -> None:
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df.longitude, df.latitude),
            crs="EPSG:4326",
        )

        with tempfile.NamedTemporaryFile() as tmp_file:
            gdf.to_file(tmp_file.name, driver="GeoJSON", engine="pyogrio")

            with open(tmp_file.name, "rb") as fp:
                buf.write(fp.read())
                buf.seek(0)

    to_buf_fn_by_format = {
        "json": lambda df, buf: df.to_json(buf, orient="records", force_ascii=False),
        "csv": lambda df, buf: df.to_csv(buf, index=False),
        "xlsx": lambda df, buf: df.to_excel(buf, engine="xlsxwriter"),
        "geojson": to_geojson,
    }

    # 1. fetch data
    structures_df = pg_hook.get_pandas_df(
        sql="SELECT * FROM public_opendata.opendata_structures",
    )
    utils.log_df_info(structures_df, logger)

    services_df = pg_hook.get_pandas_df(
        sql="SELECT * FROM public_opendata.opendata_services",
    )
    utils.log_df_info(services_df, logger)

    for kind, format in itertools.product(
        ["structures", "services"],
        ["csv", "json", "xlsx", "geojson"],
    ):
        df = structures_df if kind == "structures" else services_df

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
    schedule=EVERY_MONDAY_AT_2PM,
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    publish_to_datagouv = python.ExternalPythonOperator(
        task_id="publish",
        python=str(PYTHON_BIN_PATH),
        python_callable=_publish_to_datagouv,
    )

    start >> publish_to_datagouv >> end
