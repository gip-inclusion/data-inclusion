import pendulum

from airflow.decorators import dag, task
from airflow.operators import empty

from dag_utils import date, notifications
from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(
    python=str(PYTHON_BIN_PATH),
    retries=2,
)
def extract_load_from_api(run_id, logical_date):
    import logging

    import pandas as pd
    from furl import furl
    from sqlalchemy import types

    from dag_utils import pg

    logger = logging.getLogger(__name__)

    city_params = {
        "fields": ("nom,code,codesPostaux,codeDepartement,codeRegion,codeEpci,centre"),
        "format": "json",
    }
    city_dtypes = {
        "centre": types.JSON,
        "codesPostaux": types.ARRAY(types.TEXT),
    }

    for resource, query_params, table_name, dtypes in [
        ("departements", {"zone": "metro,drom,com"}, None, None),
        ("regions", {"zone": "metro,drom,com"}, None, None),
        ("epcis", None, None, None),
        ("communes", city_params, "communes", city_dtypes),
        (
            "communes",
            city_params | {"type": "arrondissement-municipal"},
            "districts",
            city_dtypes,
        ),
    ]:
        url = (
            (furl("https://geo.api.gouv.fr") / resource)
            .set(query_params=query_params)
            .url
        )
        logger.info(f"> fetching resource={resource} from url={url}")
        df = pd.read_json(url, dtype=False)
        with pg.connect_begin() as conn:
            schema = "decoupage_administratif"
            table_name = table_name or resource
            df.to_sql(
                f"{table_name}_tmp",
                con=conn,
                schema=schema,
                if_exists="replace",
                index=False,
                dtype=dtypes,
            )
            conn.execute(
                f"""\
                CREATE TABLE IF NOT EXISTS {schema}.{table_name}
                (LIKE {schema}.{table_name}_tmp);
                TRUNCATE {schema}.{table_name};
                INSERT INTO {schema}.{table_name}
                (SELECT * FROM {schema}.{table_name}_tmp);
                DROP TABLE {schema}.{table_name}_tmp;
                """
            )


@dag(
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    default_args=notifications.notify_failure_args(),
    schedule="@monthly",
    catchup=False,
    tags=["source"],
)
def sync_cities():
    start = empty.EmptyOperator(task_id="start")
    end = empty.EmptyOperator(task_id="end")

    (start >> extract_load_from_api() >> end)


sync_cities()
