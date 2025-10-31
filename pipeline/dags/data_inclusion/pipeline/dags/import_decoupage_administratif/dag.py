from airflow.decorators import dag, task
from airflow.models.baseoperator import chain

from data_inclusion.pipeline.common import dags, dbt, tasks


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    retries=2,
)
def extract_and_load(schema: str):
    import pandas as pd
    import sqlalchemy as sqla

    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.dags.import_decoupage_administratif import geoapi

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    geoapi_client = geoapi.GeoApiClient()

    for resource in geoapi.GeoApiClient.Resource:
        df = pd.DataFrame(geoapi_client.list_resource(resource=resource))

        fq_table_name = f"{schema}.{resource.value}"
        print(f"Loading to {fq_table_name}")
        with pg_hook.get_sqlalchemy_engine().begin() as conn:
            df.to_sql(
                f"{resource.value}_tmp",
                con=conn,
                schema=schema,
                if_exists="replace",
                index=False,
                dtype={
                    "centre": sqla.JSON,
                    "codesPostaux": sqla.ARRAY(sqla.TEXT),
                }
                if resource
                in [
                    geoapi.GeoApiClient.Resource.ARRONDISSEMENTS,
                    geoapi.GeoApiClient.Resource.COMMUNES_ASSOCIEES_DELEGUEES,
                    geoapi.GeoApiClient.Resource.COMMUNES,
                ]
                else None,
            )
            conn.execute(
                f"""\
                CREATE TABLE IF NOT EXISTS {fq_table_name}
                (LIKE {fq_table_name}_tmp);
                TRUNCATE {fq_table_name};
                INSERT INTO {fq_table_name}
                (SELECT * FROM {fq_table_name}_tmp);
                DROP TABLE {fq_table_name}_tmp;
                """
            )


@dag(
    schedule="@monthly",
    **dags.common_args(use_sentry=True),
)
def import_decoupage_administratif():
    dbt_build_staging = dbt.dbt_task.override(
        task_id="dbt_build_staging",
    )(
        command="build",
        select="path:models/staging/decoupage_administratif",
    )

    schema = "decoupage_administratif"

    chain(
        tasks.create_schema(name=schema),
        extract_and_load(schema=schema),
        dbt_build_staging,
    )


dag = import_decoupage_administratif()


if __name__ == "__main__":
    dag.test()
