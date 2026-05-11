from airflow.sdk import Variable, chain, dag, task

from data_inclusion.pipeline.common import dags
from data_inclusion.pipeline.dags.rename_services import constants


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def init():
    import mlflow
    import mlflow.genai
    from mlflow.genai import datasets

    from data_inclusion.pipeline.dags.rename_services import (
        constants,
        evaluation,
        renommage,
    )

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
    try:
        mlflow.genai.get_scorer(name=constants.FALC_SCORER_NAME)
    except mlflow.MlflowException:
        falc_judge = mlflow.genai.make_judge(
            name=constants.FALC_SCORER_NAME,
            instructions=evaluation.FALC_GUIDELINES,
            model=constants.MODEL_URI,
            feedback_value_type=bool,
            inference_params={"temperature": 0.0},
        )
        falc_judge.register()


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def run():
    import mlflow

    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.dags.rename_services import constants, model, renommage

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    structures_df = pg_hook.get_df(
        sql="SELECT id, nom FROM api__structures_v1",
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
            FROM api__services_v1
        """,
        df_type="polars",
        infer_schema_length=None,
    )

    with mlflow.context(tags={"data.inclusion.workflow": constants.PROMPT_RUN_ALIAS}):
        results_df = model.int__renommages(
            structures_df=structures_df,
            services_df=services_df,
            rename_fn=lambda service: renommage.rename(
                service=service,
                prompt_uri=constants.PROMPT_RUN_URI,
            ),
        )

    print(results_df)


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def eval():
    import mlflow

    from data_inclusion.pipeline.dags.rename_services import constants, evaluation

    with mlflow.context(tags={"data.inclusion.workflow": constants.PROMPT_EVAL_ALIAS}):
        results_df = evaluation.eval()

    print(results_df)


@dag(
    schedule=None,
    **dags.common_args(use_sentry=True),
)
def rename_services():
    env_vars = {
        "ANTHROPIC_API_KEY": Variable.get("ANTHROPIC_API_KEY"),
        "OPENAI_API_KEY": Variable.get("ANTHROPIC_API_KEY"),
        "OPENAI_BASE_URL": constants.DEFAULT_PROVIDER_URL,
        "MLFLOW_TRACKING_URI": Variable.get("MLFLOW_TRACKING_URI"),
        "MLFLOW_TRACKING_USERNAME": "admin",
        "MLFLOW_TRACKING_PASSWORD": Variable.get("MLFLOW_TRACKING_PASSWORD"),
        "MLFLOW_EXPERIMENT_NAME": constants.EXPERIMENT_NAME,
    }

    chain(
        init.override(env_vars=env_vars)(),
        run.override(env_vars=env_vars)(),
        eval.override(env_vars=env_vars)(),
    )


dag = rename_services()


if __name__ == "__main__":
    dag.test()
