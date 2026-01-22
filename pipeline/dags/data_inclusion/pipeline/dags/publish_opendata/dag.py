from airflow.decorators import dag, task

from data_inclusion.pipeline.common import dags


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def publish():
    import io

    import pendulum

    from airflow.models import Variable
    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.dags.publish_opendata import (
        constants,
        datagouv,
        helpers,
    )

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    DATAGOUV_API_KEY = Variable.get("DATAGOUV_API_KEY")

    datagouv_client = datagouv.DataGouvClient(api_key=DATAGOUV_API_KEY)

    date_str = pendulum.today().to_date_string()

    structures_df, services_df = (
        pg_hook.get_df(sql="SELECT * FROM public_marts.marts__structures_v1"),
        pg_hook.get_df(sql="SELECT * FROM public_marts.marts__services_v1"),
    )

    def clean_up(df):
        df = helpers.remove_closed_sources(df)
        df = helpers.remove_invalid_rows(df)
        df = helpers.remove_pii(df)
        df = helpers.remove_internal_columns(df)
        return df

    structures_df, services_df = helpers.remove_closed_structures(
        structures_df, services_df
    )

    structures_df = clean_up(structures_df)
    services_df = clean_up(services_df)

    structures_df.info()
    services_df.info()

    for resource, df in [
        ("structures", structures_df),
        ("services", services_df),
    ]:
        for format_, to_format in helpers.FORMATS_FN.items():
            with io.BytesIO() as buf:
                to_format(df, buf)
                datagouv_client.upload_dataset_resource(
                    dataset_id=constants.DATAGOUV_DI_DATASET_ID,
                    resource_id=constants.DATAGOUV_DI_RESOURCE_IDS[resource][format_],
                    buf=buf,
                    filename=f"{resource}-inclusion-{date_str}.{format_.value}",
                )


EVERY_MONDAY_AT_2PM = "0 14 * * 1"


@dag(
    description="Publish the consolidated dataset to datagouv",
    schedule=EVERY_MONDAY_AT_2PM,
    **dags.common_args(use_sentry=True),
)
def publish_opendata():
    publish()


dag = publish_opendata()


if __name__ == "__main__":
    dag.test()
