from airflow.sdk import chain, dag

from data_inclusion.pipeline.common import dags
from data_inclusion.pipeline.dags.extract_publics import tasks


@dag(
    schedule=None,
    **dags.common_args(use_sentry=True),
)
def eval_profiles_extractions():
    chain(
        tasks.init(),
        tasks.eval(),
    )


@dag(
    schedule=None,
    **dags.common_args(use_sentry=True),
)
def refresh_profiles_extractions():
    chain(
        tasks.init(),
        tasks.int__extracted_profiles_v1(),
    )


eval_profiles_extractions()
refresh_profiles_extractions()
