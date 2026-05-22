from airflow.sdk import Variable, task

from data_inclusion.pipeline.dags.rename_services import constants

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
    import mlflow.genai
    from mlflow.genai import datasets

    from data_inclusion.pipeline.dags.rename_services import constants, renommage

    try:
        datasets.get_dataset(name=constants.DATASET_NAME)
    except mlflow.MlflowException:
        datasets.create_dataset(name=constants.DATASET_NAME)

    try:
        mlflow.genai.load_prompt(name_or_uri=constants.PROMPT_NAME)
    except mlflow.MlflowException:
        mlflow.genai.register_prompt(
            name=constants.PROMPT_NAME,
            template=renommage.INITIAL_PROMPT,
            response_format=renommage.Response,
            model_config=renommage.PromptModelConfig(
                provider=constants.DEFAULT_PROVIDER,
                model_name=constants.DEFAULT_MODEL,
                max_tokens=None,
                top_p=None,
                top_k=None,
                temperature=constants.DEFAULT_TEMPERATURE,
            ),
        )
        for alias in [
            constants.PROMPT_RUN_ALIAS,
            constants.PROMPT_EVAL_ALIAS,
        ]:
            mlflow.genai.set_prompt_alias(
                name=constants.PROMPT_NAME,
                alias=alias,
                version=1,
            )


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    env_vars=ENV_VARS,
)
def int__renommages_v1(incremental: bool):
    import sqlalchemy as sa

    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.dags.rename_services import constants, model, renommage

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    structures_df = pg_hook.get_df(
        sql="SELECT id, nom FROM public_intermediate.int__union_structures_v1",
        df_type="polars",
        infer_schema_length=None,
    )

    services_df = pg_hook.get_df(
        sql="""
            SELECT
                source,
                date_maj,
                structure_id,
                id,
                nom,
                description,
                thematiques,
                type
            FROM public_intermediate.int__union_services_v1
        """,
        df_type="polars",
        infer_schema_length=None,
    )

    renommages_df = None
    if incremental:
        renommages_df = pg_hook.get_df(
            sql="SELECT * FROM public_intermediate.int__renommages_v1",
            df_type="polars",
            infer_schema_length=None,
        )

    results_df = model.int__renommages(
        structures_df=structures_df,
        services_df=services_df,
        rename_fn=lambda service: renommage.rename(
            service=service, workflow=constants.PROMPT_RUN_ALIAS
        ),
        existing_df=renommages_df,
    )

    if results_df is not None:
        print(results_df)

        with pg_hook.get_sqlalchemy_engine().begin() as conn:
            results_df.write_database(
                table_name="public_intermediate.int__renommages_v1",
                connection=conn,
                if_table_exists="replace",
                engine_options={"dtype": {"thematiques": sa.ARRAY(sa.String)}},
            )


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
    env_vars=ENV_VARS,
)
def eval():
    from data_inclusion.pipeline.dags.rename_services import evaluation

    results_df = evaluation.eval()

    print(results_df)
