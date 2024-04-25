from airflow.operators import python

from dag_utils.virtualenvs import PYTHON_BIN_PATH


def _export_di_dataset_to_s3(logical_date, run_id):
    import logging

    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.postgres.hooks import postgres

    from dag_utils import date
    from dag_utils.sources.utils import log_df_info

    logger = logging.getLogger(__name__)

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")
    s3_hook = s3.S3Hook(aws_conn_id="s3")

    prefix = f"data/marts/{date.local_date_str(logical_date)}/{run_id}/"

    for ressource in ["structures", "services"]:
        # TODO(vmttn): normalize table name after db split
        table_name = ressource.rstrip("s")
        key = f"{prefix}{ressource}.parquet"
        query = f"SELECT * FROM public_marts.marts_inclusion__{table_name}"

        logger.info("downloading data from query='%s'", query)
        df = pg_hook.get_pandas_df(sql=query)

        log_df_info(df, logger=logger)
        bytes_data = df.to_parquet(compression="gzip")

        logger.info("uploading data to bucket='%s'", key)
        s3_hook.load_bytes(bytes_data, key=key, replace=True)


def export_di_dataset_to_s3():
    return python.ExternalPythonOperator(
        task_id="python_export_di_dataset_to_s3",
        python=str(PYTHON_BIN_PATH),
        python_callable=_export_di_dataset_to_s3,
    )
