from pathlib import Path

import pendulum

from airflow import configuration

from data_inclusion.pipeline.common import sentry

AIRFLOW_HOME = Path(configuration.get_airflow_home()).resolve()


def common_args(use_sentry: bool = False):
    default_args = {}

    if use_sentry:
        default_args["on_failure_callback"] = sentry.fill_sentry_scope

    return {
        "start_date": pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
        "catchup": False,
        "default_args": default_args,
        "max_active_runs": 1,
        "template_searchpath": [str(AIRFLOW_HOME)],
    }
