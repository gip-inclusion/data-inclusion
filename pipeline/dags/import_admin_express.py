import logging

import airflow
import pendulum
from airflow.operators import bash, empty, python

from dags.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}


def _download_dataset():
    import requests
    from airflow.models import Variable

    with requests.get(
        Variable.get("IGN_ADMIN_EXPRESS_FILE_URL"), stream=True
    ) as response:
        response.raise_for_status()
        with open("/tmp/ign_admin_express.7z", "wb") as fp:
            for chunck in response.iter_content(chunk_size=32768):
                fp.write(chunck)


def _load_dataset():
    import pathlib
    import textwrap

    import geopandas
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    tmp_dir_path = pathlib.Path("/tmp")
    tmp_file_path = (
        tmp_dir_path
        / "ADMIN-EXPRESS-COG_3-0__SHP__FRA_2021-05-19"
        / "ADMIN-EXPRESS-COG"
        / "1_DONNEES_LIVRAISON_2021-05-19"
        / "ADECOG_3-0_SHP_WGS84G_FRA"
        / "COMMUNE.shp"
    )

    df = geopandas.read_file(tmp_file_path)

    df = df.rename(
        columns={
            "INSEE_COM": "code_insee",
            "NOM": "nom",
            "INSEE_DEP": "departement",
            "INSEE_REG": "region",
            "SIREN_EPCI": "siren_epci",
        }
    )

    df = df.rename_geometry("geom")

    df = df[["code_insee", "nom", "departement", "region", "siren_epci", "geom"]]

    # optimize future lookups
    df = df.sort_values(by="code_insee")

    engine = pg_hook.get_sqlalchemy_engine()
    target_table = "admin_express_commune"

    with engine.connect() as conn:
        with conn.begin():
            df.to_postgis(
                target_table,
                con=conn,
                if_exists="replace",
                index=False,
            )

            # create an index on simplified geography elements, for fast radius search
            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX admin_express_commune_simple_geography_idx
                        ON {target_table}
                        USING GIST(
                            CAST(
                                ST_Simplify(geom, 0.01) AS geography(geometry, 4326)
                            )
                        );
                    """
                )
            )


with airflow.DAG(
    dag_id="import_admin_express",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval="@once",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    download_dataset = python.ExternalPythonOperator(
        task_id="download",
        python=str(PYTHON_BIN_PATH),
        python_callable=_download_dataset,
    )

    extract_dataset = bash.BashOperator(
        task_id="extract",
        bash_command="7zr -aoa -bd x /tmp/ign_admin_express.7z",
        cwd="/tmp",
    )

    load_dataset = python.ExternalPythonOperator(
        task_id="load",
        python=str(PYTHON_BIN_PATH),
        python_callable=_load_dataset,
    )

    start >> download_dataset >> extract_dataset >> load_dataset >> end
