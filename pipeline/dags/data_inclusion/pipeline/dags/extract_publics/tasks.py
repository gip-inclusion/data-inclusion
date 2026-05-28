from airflow.sdk import Variable, task

from data_inclusion.pipeline.dags.extract_publics import constants

ENV_VARS = {
    "ANTHROPIC_API_KEY": Variable.get("ANTHROPIC_API_KEY"),
    "OPENAI_API_KEY": Variable.get("ANTHROPIC_API_KEY"),
    "OPENAI_BASE_URL": constants.DEFAULT_PROVIDER_URL,
    "MLFLOW_TRACKING_URI": Variable.get("MLFLOW_TRACKING_URI"),
    "MLFLOW_TRACKING_USERNAME": constants.MLFLOW_TRACKING_USERNAME,
    "MLFLOW_TRACKING_PASSWORD": Variable.get("MLFLOW_TRACKING_PASSWORD", ""),
    "MLFLOW_EXPERIMENT_NAME": constants.EXPERIMENT_NAME,
}


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def init():
    import mlflow
    from mlflow.genai import datasets

    from data_inclusion.pipeline.dags.extract_publics import constants

    try:
        datasets.get_dataset(name=constants.DATASET_NAME)
    except mlflow.MlflowException:
        datasets.create_dataset(name=constants.DATASET_NAME)


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    env_vars=ENV_VARS,
)
def int__extracted_profiles_v1():
    import mlflow
    import sqlalchemy as sa
    from sqlalchemy.dialects.postgresql import JSONB

    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.dags.extract_publics import (
        constants,
        extraction,
        model,
    )

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    services_df = pg_hook.get_df(
        sql="""
            SELECT id, publics, publics_precisions
            FROM public_intermediate.int__union_services_v1
        """,
        df_type="polars",
        infer_schema_length=None,
    )

    with mlflow.context(tags={"workflow": constants.PROMPT_RUN_ALIAS}):
        results_df = model.int__extracted_profiles(
            services_df=services_df,
            extract_fn=extraction.extract,
        )

    if results_df is not None:
        print(results_df)

        with pg_hook.get_sqlalchemy_engine().begin() as conn:
            results_df.write_database(
                table_name="public_intermediate.int__extracted_profiles_v1",
                connection=conn,
                if_table_exists="replace",
                engine_options={
                    "dtype": {
                        "publics": sa.ARRAY(sa.String),
                        "output": JSONB,
                    }
                },
            )


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    env_vars=ENV_VARS,
)
def eval():
    from data_inclusion.pipeline.dags.extract_publics import evaluation

    results_df = evaluation.eval()

    print(results_df)
