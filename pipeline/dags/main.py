import csv
import dataclasses
import io
import json
import logging
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Optional

import airflow
import pendulum
from airflow.models import DAG, DagRun, Variable
from airflow.operators import empty, python
from airflow.providers.amazon.aws.hooks import s3
from airflow.providers.postgres.operators import postgres
from airflow.utils.task_group import TaskGroup

from data_inclusion.scripts.tasks.constants import SourceType

logger = logging.getLogger(__name__)

default_args = {}


def get_key_prefix(
    logical_date: datetime,
    src_alias: str,
    batch_id: str,
    timezone,
) -> str:
    logical_date_ds = pendulum.instance(
        logical_date.astimezone(timezone)
    ).to_date_string()

    return f"data/raw/{logical_date_ds}/{src_alias}/{batch_id}/"


def _extract(
    run_id: str,
    src_alias: str,
    src_url: str,
    src_type: str,
    dag_run: DagRun,
    token: Optional[str] = None,
    user_agent: Optional[str] = None,
):
    from data_inclusion.scripts.tasks import extract

    buf_by_filename = extract.extract(
        src=src_url,
        src_type=SourceType(src_type),
        token=token,
        user_agent=user_agent,
    )

    s3_hook = s3.S3Hook(aws_conn_id="s3")

    key_prefix = get_key_prefix(
        logical_date=dag_run.logical_date,
        src_alias=src_alias,
        batch_id=run_id,
        timezone=dag.timezone,
    )

    for filename, buf in buf_by_filename.items():
        s3_hook.load_file_obj(
            file_obj=buf,
            key=key_prefix + filename,
            replace=True,
        )
        buf.close()


def _load(
    src_url: str,
    src_type: str,
    src_alias: str,
    run_id: str,
    dag: DAG,
    dag_run: DagRun,
):
    import pandas as pd

    from data_inclusion.scripts.tasks import read

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    logical_date_ds = pendulum.instance(
        dag_run.logical_date.astimezone(dag.timezone)
    ).to_date_string()

    def load_to_postgres(input_path: Path, s3_key: str):
        df, index_sr = read.read(path=input_path, src_type=SourceType(src_type))
        df = pd.DataFrame().assign(
            data=df.apply(lambda row: json.dumps(row.to_dict()), axis="columns")
        )
        if index_sr is not None:
            df = df.assign(
                data_normalized=pd.DataFrame()
                .assign(id=index_sr)
                .apply(lambda row: json.dumps(row.to_dict()), axis="columns")
            )
        else:
            df = df.assign(data_normalized=json.dumps({}))
        df = df.assign(batch_id=run_id)
        df = df.assign(src_url=src_url)
        df = df.assign(src_alias=src_alias)
        df = df.assign(file=s3_key)
        df = df.assign(logical_date=logical_date_ds)

        buf = io.StringIO()
        df.to_csv(
            buf,
            index=False,
            header=False,
            sep="|",
            doublequote=False,
            escapechar="\\",
            quoting=csv.QUOTE_ALL,
        )
        buf.seek(0)

        # using the psycopg2 client to copy from file
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    textwrap.dedent(
                        f"""
                        DELETE FROM
                            datalake
                        WHERE
                            src_url = '{src_url}'
                            AND batch_id = '{run_id}'
                            AND file = '{s3_key}';
                    """
                    )
                )
                cursor.copy_expert(
                    "COPY datalake (data, data_normalized, batch_id, src_url, src_alias, file, logical_date) "  # noqa: E501, W505
                    "FROM STDIN WITH CSV HEADER ESCAPE '\\' DELIMITER '|'",
                    buf,
                )

    s3_hook = s3.S3Hook(aws_conn_id="s3")

    key_prefix = get_key_prefix(
        logical_date=dag_run.logical_date,
        src_alias=src_alias,
        batch_id=run_id,
        timezone=dag.timezone,
    )

    for key in s3_hook.list_keys(prefix=key_prefix):
        tmp_filename = s3_hook.download_file(key=key)
        load_to_postgres(input_path=Path(tmp_filename), s3_key=key)


def _reshape(
    src_url: str,
    src_type: str,
    src_alias: str,
    run_id: str,
    dag: DAG,
    dag_run: DagRun,
):
    import numpy as np

    from data_inclusion.scripts.tasks import reshape

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    logical_date_ds = pendulum.instance(
        dag_run.logical_date.astimezone(dag.timezone)
    ).to_date_string()

    df = pg_hook.get_pandas_df(
        sql=textwrap.dedent(
            """
                SELECT
                    file,
                    data
                FROM
                    datalake
                WHERE
                    batch_id = %(batch_id)s
                    AND src_url = %(src_url)s;
            """
        ),
        parameters={"batch_id": run_id, "src_url": src_url},
    )
    df = df.replace({np.nan: None})

    # reshape df
    df = reshape.reshape(df, src_type=SourceType(src_type))

    # load results back to pg
    df = df.assign(batch_id=run_id)
    df = df.assign(src_url=src_url)
    df = df.assign(src_alias=src_alias)
    df = df.assign(logical_date=logical_date_ds)

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():
            conn.execute(
                textwrap.dedent(
                    """
                    DELETE
                    FROM
                        datawarehouse
                    WHERE
                        batch_id = %(batch_id)s
                        AND src_url = %(src_url)s;
                """
                ),
                {"batch_id": run_id, "src_url": src_url},
            )
            df.to_sql("datawarehouse", con=conn, if_exists="append", index=False)


def _geocode(
    src_url: str,
    run_id: str,
):
    from data_inclusion.scripts.tasks import geocoding

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    df = pg_hook.get_pandas_df(
        sql=textwrap.dedent(
            """
                SELECT
                    data
                FROM
                    datawarehouse
                WHERE
                    batch_id = %(batch_id)s
                    AND src_url = %(src_url)s
                    AND data ? 'siret';
            """
        ),
        parameters={"batch_id": run_id, "src_url": src_url},
    )

    df = geocoding.geocode_data(
        df,
        geocoding_backend=geocoding.BaseAdresseNationaleBackend(
            base_url=Variable.get("BAN_API_URL")
        ),
    )

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():
            df[["id", "result_citycode"]].to_sql(
                "tmp", con=conn, if_exists="append", index=False
            )
            conn.execute(
                textwrap.dedent(
                    """
                    UPDATE
                        datawarehouse as dwh
                    SET
                        data_normalized = jsonb_set(
                            COALESCE(
                                data_normalized,
                                '{}'
                            ),
                            '{code_insee}',
                            to_jsonb(tmp.result_citycode)
                        )
                    FROM
                        tmp
                    WHERE
                        dwh.batch_id = %(batch_id)s
                        AND dwh.src_url = %(src_url)s
                        AND dwh.data ? 'siret'
                        AND dwh.data->>'id' = tmp.id;
                    """
                ),
                {"batch_id": run_id, "src_url": src_url},
            )
            conn.execute("DROP TABLE tmp;")


def _validate(
    src_url: str,
    run_id: str,
):
    from data_inclusion.scripts.tasks import validate

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    # structures
    structures_df = pg_hook.get_pandas_df(
        sql=textwrap.dedent(
            """
                SELECT
                    data,
                    data_normalized
                FROM
                    datawarehouse
                WHERE
                    batch_id = %(batch_id)s
                    AND src_url = %(src_url)s
                    AND data ? 'siret';
            """
        ),
        parameters={"batch_id": run_id, "src_url": src_url},
    )

    structures_df = validate.validate_structure_dataframe(structures_df)

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():
            structures_df[["id", "is_valid"]].to_sql(
                "tmp", con=conn, if_exists="append", index=False
            )
            conn.execute(
                textwrap.dedent(
                    """
                    UPDATE
                        datawarehouse as dwh
                    SET
                        data_normalized = jsonb_set(
                            COALESCE(
                                data_normalized,
                                '{}'
                            ),
                            '{is_valid}',
                            to_jsonb(tmp.is_valid)
                        )
                    FROM
                        tmp
                    WHERE
                        dwh.batch_id = %(batch_id)s
                        AND dwh.src_url = %(src_url)s
                        AND dwh.data ? 'siret'
                        AND dwh.data->>'id' = tmp.id;
                    """
                ),
                {"batch_id": run_id, "src_url": src_url},
            )
            conn.execute("DROP TABLE tmp;")

    # services
    services_df = pg_hook.get_pandas_df(
        sql=textwrap.dedent(
            """
                SELECT
                    data,
                    data_normalized
                FROM
                    datawarehouse
                WHERE
                    batch_id = %(batch_id)s
                    AND src_url = %(src_url)s
                    AND NOT data ? 'siret';
            """
        ),
        parameters={"batch_id": run_id, "src_url": src_url},
    )

    if len(services_df) == 0:
        return

    services_df = validate.validate_service_dataframe(services_df)

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():
            services_df[["id", "is_valid"]].to_sql(
                "tmp", con=conn, if_exists="append", index=False
            )
            conn.execute(
                textwrap.dedent(
                    """
                    UPDATE
                        datawarehouse as dwh
                    SET
                        data_normalized = jsonb_set(
                            COALESCE(
                                data_normalized,
                                '{}'
                            ),
                            '{is_valid}',
                            to_jsonb(tmp.is_valid)
                        )
                    FROM
                        tmp
                    WHERE
                        dwh.batch_id = %(batch_id)s
                        AND dwh.src_url = %(src_url)s
                        AND NOT dwh.data ? 'siret'
                        AND dwh.data->>'id' = tmp.id;
                    """
                ),
                {"batch_id": run_id, "src_url": src_url},
            )
            conn.execute("DROP TABLE tmp;")


@dataclasses.dataclass(frozen=True)
class SourceConfig:
    # just a slug identifying the source, that is used in the airflow tasks
    src_alias: str
    # actual src location
    src_url: str
    src_type: SourceType
    token: Optional[str] = None
    user_agent: Optional[str] = None
    skip_post_processing: bool = False

    def as_op_kwargs(self):
        # to pass the config to airflow operators (as op_kwargs), the instance must be
        # serialized to python native types (Enum not handled by dataclasses.asdict)
        return {
            "src_alias": self.src_alias,
            "src_url": self.src_url,
            "src_type": self.src_type.value,
            "token": self.token,
            "user_agent": self.user_agent,
        }


SRC_CONFIGS_LIST = [
    SourceConfig(
        src_alias="mes_aides_aides",
        src_url=Variable.get("MES_AIDES_AIDES_URL", None),
        src_type=SourceType.MES_AIDES,
        token=Variable.get("MES_AIDES_AIRTABLE_KEY", None),
        skip_post_processing=True,
    ),
    SourceConfig(
        src_alias="mes_aides_garages",
        src_url=Variable.get("MES_AIDES_GARAGES_URL", None),
        src_type=SourceType.MES_AIDES,
        token=Variable.get("MES_AIDES_AIRTABLE_KEY", None),
    ),
    SourceConfig(
        src_alias="dora",
        src_url=Variable.get("DORA_API_URL", None),
        src_type=SourceType.DORA,
    ),
    SourceConfig(
        src_alias="emplois_siae",
        src_url=Variable.get("EMPLOIS_API_URL", None) + "?type=siae",
        src_type=SourceType.EMPLOIS,
        token=Variable.get("EMPLOIS_API_TOKEN", None),
    ),
    SourceConfig(
        src_alias="emplois_orga",
        src_url=Variable.get("EMPLOIS_API_URL", None) + "?type=orga",
        src_type=SourceType.EMPLOIS,
        token=Variable.get("EMPLOIS_API_TOKEN", None),
    ),
    SourceConfig(
        src_alias="cd35",
        src_url=Variable.get("CD35_FILE_URL", None),
        src_type=SourceType.CD35,
    ),
    SourceConfig(
        src_alias="finess",
        src_url=Variable.get("FINESS_FILE_URL", None),
        src_type=SourceType.FINESS,
    ),
    SourceConfig(
        src_alias="siao",
        src_url=Variable.get("SIAO_FILE_URL", None),
        src_type=SourceType.SIAO,
    ),
    SourceConfig(
        src_alias="odspep",
        src_url=Variable.get("ODSPEP_FILE_URL", None),
        src_type=SourceType.ODSPEP,
    ),
    SourceConfig(
        src_alias="etab_pub",
        src_url=Variable.get("ETAB_PUB_FILE_URL", None),
        src_type=SourceType.ETAB_PUBLICS,
    ),
    SourceConfig(
        src_alias="cd72",
        src_url=Variable.get("CD72_FILE_URL", None),
        src_type=SourceType.CD72,
    ),
    SourceConfig(
        src_alias="1jeune1solution",
        src_url=Variable.get("UN_JEUNE_UNE_SOLUTION_API_URL", None),
        src_type=SourceType.UN_JEUNE_UNE_SOLUTION,
        skip_post_processing=True,
    ),
    SourceConfig(
        src_alias="conseiller-numerique",
        src_url=Variable.get("CONSEILLER_NUMERIQUE_FILE_URL", None),
        src_type=SourceType.CONSEILLER_NUMERIQUE,
    ),
    SourceConfig(
        src_alias="mednum-hinaura",
        src_url=Variable.get("MEDIATION_NUMERIQUE_HINAURA_DATASET_URL", None),
        src_type=SourceType.MEDIATION_NUMERIQUE,
    ),
    SourceConfig(
        src_alias="mednum-cd49",
        src_url=Variable.get("MEDIATION_NUMERIQUE_CD49_DATASET_URL", None),
        src_type=SourceType.MEDIATION_NUMERIQUE,
    ),
    SourceConfig(
        src_alias="mednum-assembleurs",
        src_url=Variable.get("MEDIATION_NUMERIQUE_ASSEMBLEURS_DATASET_URL", None),
        src_type=SourceType.MEDIATION_NUMERIQUE,
    ),
]


with airflow.DAG(
    dag_id="main",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    start = empty.EmptyOperator(task_id="start")

    setup = postgres.PostgresOperator(
        task_id="setup",
        postgres_conn_id="pg",
        sql="sql/setup.sql",
    )

    start >> setup

    for src_config in SRC_CONFIGS_LIST:
        with TaskGroup(group_id=src_config.src_alias):
            extract = python.PythonOperator(
                task_id="extract",
                python_callable=_extract,
                op_kwargs=src_config.as_op_kwargs(),
                retries=2,
            )

            load = python.PythonOperator(
                task_id="load",
                python_callable=_load,
                op_kwargs=src_config.as_op_kwargs(),
            )

            setup >> extract
            extract >> load

            compute_flux = postgres.PostgresOperator(
                task_id="compute_flux",
                postgres_conn_id="pg",
                sql="sql/compute_flux.sql",
                params={"src_url": src_config.src_url},
            )

            load >> compute_flux

            if not src_config.skip_post_processing:
                reshape = python.PythonOperator(
                    task_id="reshape",
                    python_callable=_reshape,
                    op_kwargs=src_config.as_op_kwargs(),
                )
                check_sirets = postgres.PostgresOperator(
                    task_id="check_sirets",
                    postgres_conn_id="pg",
                    sql="sql/join_dwh_sirene.sql",
                    params={"src_url": src_config.src_url},
                )
                flag_personal_emails = postgres.PostgresOperator(
                    task_id="flag_personal_emails",
                    postgres_conn_id="pg",
                    sql="sql/flag_personal_emails.sql",
                    params={"src_url": src_config.src_url},
                )
                geocode = python.PythonOperator(
                    task_id="geocode",
                    python_callable=_geocode,
                    op_kwargs=src_config.as_op_kwargs(),
                    pool="base_adresse_nationale_api",
                )
                validate = python.PythonOperator(
                    task_id="validate",
                    python_callable=_validate,
                    op_kwargs=src_config.as_op_kwargs(),
                )

                reshape >> [check_sirets, flag_personal_emails]
                load >> reshape >> geocode >> validate
