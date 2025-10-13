from airflow.sdk import chain, dag, task

from data_inclusion.pipeline.common import dags, dbt, tasks


@task.external_python(
    python=tasks.PYTHON_BIN_PATH,
    retries=2,
)
def extract_and_load(schema: str):
    import pandas as pd
    import sqlalchemy as sqla
    from furl import furl

    from airflow.providers.postgres.hooks import postgres

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    base_url = furl("https://geo.api.gouv.fr")
    # the default zone parameter is inconsistent between resources
    # so we explicitely set it for all resources
    base_url.set({"zone": ",".join(["metro", "drom", "com"])})
    URL_BY_RESOURCE = {
        "regions": base_url / "regions",
        "departements": base_url / "departements",
        "epcis": base_url / "epcis",
        "communes": (base_url / "communes").set(
            {
                # explicitely list retrieve fields
                # to include the "center" field
                "fields": ",".join(
                    [
                        "nom",
                        "code",
                        "centre",
                        "codesPostaux",
                        "codeEpci",
                        "codeDepartement",
                        "codeRegion",
                    ]
                )
            }
        ),
    }

    # arrondissements do not have a dedicated endpoint
    # they are retrieved using an opt-in parameter
    # on the communes endpoint
    URL_BY_RESOURCE["arrondissements"] = (
        URL_BY_RESOURCE["communes"].copy().add({"type": "arrondissement-municipal"})
    )

    for resource, url in URL_BY_RESOURCE.items():
        print(f"Fetching resource={resource} from url={url}")
        df = pd.read_json(str(url), dtype=False)

        fq_table_name = f"{schema}.{resource}"
        print(f"Loading to {fq_table_name}")
        with pg_hook.get_sqlalchemy_engine().begin() as conn:
            df.to_sql(
                f"{resource}_tmp",
                con=conn,
                schema=schema,
                if_exists="replace",
                index=False,
                dtype={
                    "centre": sqla.JSON,
                    "codesPostaux": sqla.ARRAY(sqla.TEXT),
                }
                if resource in ["communes", "arrondissements"]
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


import_decoupage_administratif()
