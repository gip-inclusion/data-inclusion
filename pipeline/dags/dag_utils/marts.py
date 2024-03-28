from airflow.providers.amazon.aws.hooks import s3
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.utils.task_group import TaskGroup


def export_di_dataset_to_s3():
    with TaskGroup(group_id="export_di_dataset_to_s3") as task_group:
        for ressource in ["structures", "services"]:
            # TODO(vmttn): normalize table name after db split
            table_name = ressource.rstrip("s")

            s3_bucket = s3.S3Hook(aws_conn_id="s3").get_bucket().name
            prefix = "data/marts/{{ local_ds(ts) }}/{{ run_id }}/"
            s3_key = f"{prefix}{table_name}.parquet.gzip"
            query = f"SELECT * FROM public.{table_name}"

            SqlToS3Operator(
                task_id=ressource,
                sql_conn_id="pg",
                aws_conn_id="s3",
                s3_key=s3_key,
                s3_bucket=s3_bucket,
                query=query,
                replace=True,
                pd_kwargs={"compression": "gzip"},
                file_format="parquet",
            )

        return task_group
