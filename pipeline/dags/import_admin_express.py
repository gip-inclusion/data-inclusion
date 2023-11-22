import logging

import airflow
import pendulum
from airflow.operators import bash, empty, python

from dag_utils.virtualenvs import PIPX_PYTHON_BIN_PATH, PYTHON_BIN_PATH

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


def _load_communes():
    import pathlib
    import textwrap

    import geopandas
    import tqdm
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    engine = pg_hook.get_sqlalchemy_engine()

    tmp_dir_path = (
        pathlib.Path("/tmp")
        / "ADMIN-EXPRESS-COG_3-0__SHP__FRA_2021-05-19"
        / "ADMIN-EXPRESS-COG"
        / "1_DONNEES_LIVRAISON_2021-05-19"
        / "ADECOG_3-0_SHP_WGS84G_FRA"
    )

    tmp_file_path = tmp_dir_path / "COMMUNE.shp"

    target_table = "admin_express_communes"

    with engine.connect() as conn:
        with conn.begin():
            for i in tqdm.tqdm(range(100)):
                chunck_df = geopandas.read_file(
                    tmp_file_path, rows=slice(1000 * i, 1000 * (i + 1))
                )

                if len(chunck_df) == 0:
                    break

                chunck_df = chunck_df.rename(
                    columns={
                        "INSEE_COM": "code",
                        "NOM": "nom",
                        "INSEE_DEP": "departement",
                        "INSEE_REG": "region",
                        "SIREN_EPCI": "siren_epci",
                    }
                )
                chunck_df = chunck_df.rename_geometry("geom")
                chunck_df = chunck_df[
                    ["code", "nom", "departement", "region", "siren_epci", "geom"]
                ]

                # optimize future lookups
                chunck_df = chunck_df.sort_values(by="code")

                chunck_df.to_postgis(
                    target_table,
                    con=conn,
                    if_exists="replace" if i == 0 else "append",
                    index=False,
                )

            conn.execute(f"ALTER TABLE {target_table} ADD PRIMARY KEY (code);")
            # create an index on simplified geography elements, for fast radius search
            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX idx_admin_express_communes_simple_geog
                        ON {target_table}
                        USING GIST(
                            CAST(
                                ST_Simplify(geom, 0.01) AS geography(geometry, 4326)
                            )
                        );
                    """
                )
            )


def _load_epcis():
    import pathlib
    import textwrap

    import geopandas
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    tmp_dir_path = (
        pathlib.Path("/tmp")
        / "ADMIN-EXPRESS-COG_3-0__SHP__FRA_2021-05-19"
        / "ADMIN-EXPRESS-COG"
        / "1_DONNEES_LIVRAISON_2021-05-19"
        / "ADECOG_3-0_SHP_WGS84G_FRA"
    )

    tmp_file_path = tmp_dir_path / "EPCI.shp"
    df = geopandas.read_file(tmp_file_path)
    df = df.rename(
        columns={
            "CODE_SIREN": "code",
            "NOM": "nom",
            "NATURE": "nature",
        }
    )
    df = df.rename_geometry("geom")
    df = df[["code", "nom", "nature", "geom"]]
    df = df.sort_values(by="code")  # optimize future lookups

    engine = pg_hook.get_sqlalchemy_engine()
    target_table = "admin_express_epcis"

    with engine.connect() as conn:
        with conn.begin():
            df.to_postgis(target_table, con=conn, if_exists="replace", index=False)
            conn.execute(f"ALTER TABLE {target_table} ADD PRIMARY KEY (code);")
            # create an index on simplified geography elements, for fast radius search
            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX idx_admin_express_epcis_simple_geog
                        ON {target_table}
                        USING GIST(
                            CAST(
                                ST_Simplify(geom, 0.01) AS geography(geometry, 4326)
                            )
                        );
                    """
                )
            )


def _load_departements():
    import pathlib
    import textwrap

    import geopandas
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    tmp_dir_path = (
        pathlib.Path("/tmp")
        / "ADMIN-EXPRESS-COG_3-0__SHP__FRA_2021-05-19"
        / "ADMIN-EXPRESS-COG"
        / "1_DONNEES_LIVRAISON_2021-05-19"
        / "ADECOG_3-0_SHP_WGS84G_FRA"
    )

    tmp_file_path = tmp_dir_path / "DEPARTEMENT.shp"
    df = geopandas.read_file(tmp_file_path)
    df = df.rename(
        columns={
            "INSEE_DEP": "code",
            "NOM": "nom",
            "INSEE_REG": "insee_reg",
        }
    )
    df = df.rename_geometry("geom")
    df = df[["code", "nom", "insee_reg", "geom"]]
    df = df.sort_values(by="code")  # optimize future lookups

    engine = pg_hook.get_sqlalchemy_engine()
    target_table = "admin_express_departements"

    with engine.connect() as conn:
        with conn.begin():
            df.to_postgis(target_table, con=conn, if_exists="replace", index=False)
            conn.execute(f"ALTER TABLE {target_table} ADD PRIMARY KEY (code);")
            # create an index on simplified geography elements, for fast radius search
            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX idx_admin_express_departements_simple_geog
                        ON {target_table}
                        USING GIST(
                            CAST(
                                ST_Simplify(geom, 0.01) AS geography(geometry, 4326)
                            )
                        );
                    """
                )
            )


def _load_regions():
    import pathlib
    import textwrap

    import geopandas
    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    tmp_dir_path = (
        pathlib.Path("/tmp")
        / "ADMIN-EXPRESS-COG_3-0__SHP__FRA_2021-05-19"
        / "ADMIN-EXPRESS-COG"
        / "1_DONNEES_LIVRAISON_2021-05-19"
        / "ADECOG_3-0_SHP_WGS84G_FRA"
    )

    tmp_file_path = tmp_dir_path / "REGION.shp"
    df = geopandas.read_file(tmp_file_path)
    df = df.rename(
        columns={
            "INSEE_REG": "code",
            "NOM": "nom",
        }
    )
    df = df.rename_geometry("geom")
    df = df[["code", "nom", "geom"]]
    df = df.sort_values(by="code")  # optimize future lookups

    engine = pg_hook.get_sqlalchemy_engine()
    target_table = "admin_express_regions"

    with engine.connect() as conn:
        with conn.begin():
            df.to_postgis(target_table, con=conn, if_exists="replace", index=False)
            conn.execute(f"ALTER TABLE {target_table} ADD PRIMARY KEY (code);")
            # create an index on simplified geography elements, for fast radius search
            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX idx_admin_express_regions_simple_geog
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
        bash_command=(
            f"{PIPX_PYTHON_BIN_PATH.parent / 'pipx'} "
            "run py7zr x /tmp/ign_admin_express.7z"
        ),
        append_env=True,
        cwd="/tmp",
    )

    load_communes = python.ExternalPythonOperator(
        task_id="load_communes",
        python=str(PYTHON_BIN_PATH),
        python_callable=_load_communes,
    )

    load_epcis = python.ExternalPythonOperator(
        task_id="load_epcis",
        python=str(PYTHON_BIN_PATH),
        python_callable=_load_epcis,
    )

    load_departements = python.ExternalPythonOperator(
        task_id="load_departements",
        python=str(PYTHON_BIN_PATH),
        python_callable=_load_departements,
    )

    load_regions = python.ExternalPythonOperator(
        task_id="load_regions",
        python=str(PYTHON_BIN_PATH),
        python_callable=_load_regions,
    )

    (
        start
        >> download_dataset
        >> extract_dataset
        >> [load_communes, load_epcis, load_departements, load_regions]
        >> end
    )
