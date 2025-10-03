from airflow.sdk import chain, dag, task, task_group

from data_inclusion.pipeline import sources
from data_inclusion.pipeline.common import dags, s3, tasks


@task.external_python(
    python=tasks.PYTHON_BIN_PATH,
    retries=2,
)
def extract(source_id, stream_id, to_s3_path):
    import io

    from airflow.providers.amazon.aws.hooks import s3

    from data_inclusion.pipeline import sources

    source = sources.SOURCES_CONFIGS[source_id]
    stream = source["streams"][stream_id]
    url = stream["url"]

    print(f"Using {url}")

    extract_fn = sources.get_extractor(source_id, stream_id)
    content = extract_fn(url=url, token=stream.get("token"), id=stream_id)

    s3_hook = s3.S3Hook(aws_conn_id="s3")
    with io.BytesIO(content) as buf:
        s3_hook.load_file_obj(
            key=to_s3_path,
            file_obj=buf,
            replace=True,
        )


@task.external_python(python=tasks.PYTHON_BIN_PATH)
def load(schema_name: str, source_id, stream_id, from_s3_path):
    import tempfile
    from pathlib import Path

    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline import sources
    from data_inclusion.pipeline.common import pg

    read_fn = sources.get_reader(source_id, stream_id)

    print(f"Using {from_s3_path}")

    with tempfile.TemporaryDirectory() as tmpdir:
        s3_hook = s3.S3Hook(aws_conn_id="s3")
        tmp_file_path = Path(
            s3_hook.download_file(
                key=from_s3_path,
                local_path=tmpdir,
            )
        )
        df = read_fn(path=tmp_file_path)

    pg.to_pg(
        hook=postgres.PostgresHook(postgres_conn_id="pg"),
        df=df,
        schema_name=schema_name,
        table_name=stream_id.replace("-", "_"),
    )


for source_id, source_config in sources.SOURCES_CONFIGS.items():
    if "streams" not in source_config:
        continue
    model_name = source_id.replace("-", "_")
    dag_id = f"import_{model_name}"

    @dag(
        dag_id=dag_id,
        schedule=source_config["schedule"],
        tags=["source"],
        **dags.common_args(use_sentry=True),
    )
    def _dag():
        base_s3_path = s3.get_key(stage="raw", source_id=source_id)
        schema_name = source_id.replace("-", "_")

        create_schema_task = tasks.create_schema(name=schema_name)

        for stream_id, stream in source_config["streams"].items():
            s3_path = str(base_s3_path / stream["filename"])

            @task_group(group_id=stream_id)
            def tg():
                chain(
                    extract(
                        source_id=source_id,
                        stream_id=stream_id,
                        to_s3_path=s3_path,
                    ),
                    load(
                        schema_name=schema_name,
                        source_id=source_id,
                        stream_id=stream_id,
                        from_s3_path=s3_path,
                    ),
                )

            chain(create_schema_task, tg())

    globals()[dag_id] = _dag()
