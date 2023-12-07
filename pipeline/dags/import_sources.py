import logging

import airflow
import pendulum
from airflow.operators import empty, python
from airflow.utils.task_group import TaskGroup

from dag_utils.date import TIME_ZONE
from dag_utils.dbt import dbt_operator_factory
from dag_utils.notifications import format_failure, notify_webhook
from dag_utils.settings import SOURCES_CONFIGS
from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)


default_args = {
    "on_failure_callback": lambda context: notify_webhook(
        context,
        conn_id="mattermost",
        format_fn=format_failure,
    )
}


def extract_from_source_to_datalake_bucket(
    stream_config: dict,
    source_config: dict,
    run_id: str,
    logical_date,
):
    from data_inclusion.scripts.tasks import (
        dora,
        emplois_de_linclusion,
        grist,
        mediation_numerique,
        mes_aides,
        reseau_alpha,
        soliguide,
        un_jeune_une_solution,
        utils,
    )

    from dag_utils import s3

    source_id = source_config["id"]

    # Store this in settings or in a "sources.py" module.
    EXTRACT_FN_BY_SOURCE_ID = {
        "agefiph": utils.extract_http_content,
        "annuaire-du-service-public": utils.extract_http_content,
        "cd35": utils.extract_http_content,
        "cd72": grist.extract,
        "data-inclusion": utils.extract_http_content,
        "dora": dora.extract,
        "emplois-de-linclusion": emplois_de_linclusion.extract,
        "finess": utils.extract_http_content,
        "mes-aides": mes_aides.extract,
        "siao": utils.extract_http_content,
        "un-jeune-une-solution": un_jeune_une_solution.extract,
        "soliguide": soliguide.extract,
        "monenfant": utils.extract_http_content,
        "reseau-alpha": {
            "structures": reseau_alpha.extract_structures,
            "formations": reseau_alpha.extract_formations,
        },
        "pole-emploi": dora.extract,
    }

    # TODO(vperron): Replace by dict of objects
    if source_id.startswith("mediation-numerique-"):
        extract_fn = mediation_numerique.extract
    elif isinstance(EXTRACT_FN_BY_SOURCE_ID[source_id], dict):
        extract_fn = EXTRACT_FN_BY_SOURCE_ID[source_id][stream_config["id"]]
    else:
        extract_fn = EXTRACT_FN_BY_SOURCE_ID[source_id]

    s3.store_content(
        path=s3.source_file_path(
            source_id=source_id,
            filename=stream_config["filename"],
            run_id=run_id,
            logical_date=logical_date,
        ),
        content=extract_fn(**stream_config),
    )


def load_from_s3_to_data_warehouse(
    stream_config: dict,
    source_config: dict,
    run_id: str,
    logical_date,
):
    import pandas as pd
    import sqlalchemy as sqla
    from sqlalchemy.dialects.postgresql import JSONB

    from data_inclusion.scripts.tasks import (
        agefiph,
        annuaire_du_service_public,
        monenfant,
        reseau_alpha,
        soliguide,
        utils,
    )

    from dag_utils import pg, s3

    # Meme remarque.
    READ_FN_BY_SOURCE_ID = {
        "annuaire-du-service-public": annuaire_du_service_public.read,
        "cd35": lambda path: utils.read_csv(path, sep=";"),
        "cd72": lambda path: utils.read_csv(path, sep=","),
        "data-inclusion": utils.read_json,
        "dora": utils.read_json,
        "emplois-de-linclusion": utils.read_json,
        "finess": lambda path: utils.read_csv(path, sep=","),
        "mes-aides": utils.read_json,
        "siao": utils.read_excel,
        "un-jeune-une-solution": utils.read_json,
        "soliguide": soliguide.read,
        "monenfant": monenfant.read,
        "reseau-alpha": {
            "structures": reseau_alpha.read_structures,
            "formations": reseau_alpha.read_formations,
        },
        "agefiph": agefiph.read,
        "pole-emploi": utils.read_json,
    }

    source_id = source_config["id"]
    stream_id = stream_config["id"]

    if source_id.startswith("mediation-numerique-"):
        read_fn = utils.read_json
    elif isinstance(READ_FN_BY_SOURCE_ID[source_id], dict):
        read_fn = READ_FN_BY_SOURCE_ID[source_id][stream_id]
    else:
        read_fn = READ_FN_BY_SOURCE_ID[source_id]

    s3_path = s3.source_file_path(
        source_id, stream_config["filename"], run_id, logical_date
    )
    tmp_filename = s3.download_file(s3_path)
    df = read_fn(path=tmp_filename)

    df = pd.DataFrame().assign(data=df.apply(lambda row: row.to_dict(), axis="columns"))
    df = df.assign(_di_batch_id=run_id)
    df = df.assign(_di_source_id=source_id)
    df = df.assign(_di_stream_id=stream_id)
    df = df.assign(_di_source_url=stream_config["url"])
    df = df.assign(_di_stream_s3_key=s3_path)
    df = df.assign(_di_logical_date=logical_date)

    schema_name = source_id.replace("-", "_")
    table_name = stream_id.replace("-", "_")

    pg.create_schema(schema_name)

    with pg.connect_begin() as conn:
        df.to_sql(
            f"{table_name}_tmp",
            con=conn,
            schema=schema_name,
            if_exists="replace",
            index=False,
            dtype={
                "data": JSONB,
                "_di_logical_date": sqla.Date,
            },
        )

        conn.execute(
            f"""\
            CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
                data              JSONB,
                _di_batch_id      TEXT,
                _di_source_id     TEXT,
                _di_stream_id     TEXT,
                _di_source_url    TEXT,
                _di_stream_s3_key TEXT,
                _di_logical_date  DATE
            );
            TRUNCATE {schema_name}.{table_name};
            INSERT INTO {schema_name}.{table_name}
            SELECT * FROM {schema_name}.{table_name}_tmp;
            DROP TABLE {schema_name}.{table_name}_tmp;"""
        )


# generate a dedicated DAG for every configured sources
for source_config in SOURCES_CONFIGS:
    dag_id = f"import_{source_config['id']}".replace("-", "_")
    dag = airflow.DAG(
        dag_id=dag_id,
        start_date=pendulum.datetime(2022, 1, 1, tz=TIME_ZONE),
        default_args=default_args,
        schedule_interval=source_config["schedule_interval"],
        catchup=False,
        tags=["source"],
    )

    with dag:
        start = empty.EmptyOperator(task_id="start")
        end = empty.EmptyOperator(task_id="end")

        dbt_source_id = source_config["id"].replace("-", "_")

        # tests here can detect impacting changes on the source data
        # before anything happens to the previously existing data
        dbt_test_source = dbt_operator_factory(
            task_id="dbt_test_source",
            command="test",
            select=f"source:{dbt_source_id}",
        )

        # historization of the raw data, if that makes sense
        if source_config["snapshot"]:
            dbt_snapshot_source = dbt_operator_factory(
                task_id="dbt_snapshot_source",
                command="snapshot",
                select=dbt_source_id,
            )
        else:
            dbt_snapshot_source = None

        # create dedicated embranchments for the extract/load of every streams in source
        for stream_config in source_config["streams"]:
            with TaskGroup(group_id=stream_config["id"]) as stream_task_group:
                extract = python.ExternalPythonOperator(
                    task_id="extract",
                    python=str(PYTHON_BIN_PATH),
                    python_callable=extract_from_source_to_datalake_bucket,
                    retries=2,
                    op_kwargs={
                        "stream_config": stream_config,
                        "source_config": source_config,
                    },
                )
                load = python.ExternalPythonOperator(
                    task_id="load",
                    python=str(PYTHON_BIN_PATH),
                    python_callable=load_from_s3_to_data_warehouse,
                    op_kwargs={
                        "stream_config": stream_config,
                        "source_config": source_config,
                    },
                )

                start >> extract >> load

            stream_task_group >> dbt_test_source

            if dbt_snapshot_source is not None:
                dbt_test_source >> dbt_snapshot_source >> end
            else:
                dbt_test_source >> end

    globals()[dag_id] = dag
