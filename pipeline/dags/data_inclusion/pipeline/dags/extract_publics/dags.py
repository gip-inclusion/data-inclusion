from airflow.sdk import chain, dag

from data_inclusion.pipeline.common import dags
from data_inclusion.pipeline.dags.extract_publics import tasks


@dag(
    schedule=None,
    **dags.common_args(use_sentry=True),
)
def eval_extractions():
    chain(
        tasks.init(),
        tasks.eval(),
    )


@dag(
    schedule=None,
    **dags.common_args(use_sentry=True),
)
def refresh_extractions():
    chain(
        tasks.init(),
        tasks.int__publics_extractions_v1(),
    )


eval_extractions()
refresh_extractions()
