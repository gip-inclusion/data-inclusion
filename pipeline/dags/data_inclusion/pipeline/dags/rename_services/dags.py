from airflow.sdk import chain, dag

from data_inclusion.pipeline.common import dags
from data_inclusion.pipeline.dags.rename_services import tasks


@dag(
    schedule=None,
    **dags.common_args(use_sentry=True),
)
def eval_renamings():
    chain(
        tasks.init(),
        tasks.eval(),
    )


@dag(
    schedule=None,
    **dags.common_args(use_sentry=True),
)
def refresh_renamings():
    chain(
        tasks.init(),
        tasks.int__renommages_v1(incremental=False),
    )


eval_renamings()
refresh_renamings()
