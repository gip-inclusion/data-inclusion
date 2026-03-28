from airflow.sdk import dag, task

from data_inclusion.pipeline.common import dags


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def compare_and_summarize():
    import tempfile
    from pathlib import Path

    from airflow.providers.amazon.aws.fs import s3 as s3fs
    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.slack.hooks import slack

    from data_inclusion.pipeline.scripts.compare import compare

    VERSION = "v1"
    FILE = "services.parquet"

    slack_hook = slack.SlackHook(slack_conn_id="slack")
    s3_hook = s3.S3Hook(aws_conn_id="s3")
    s3fs_client = s3fs.get_fs(conn_id="s3")

    BASE_KEY = Path(s3_hook.service_config["bucket_name"]) / "data" / "marts"

    before_date, after_date = sorted(s3fs_client.ls(BASE_KEY))[-2:]

    before_run = sorted(s3fs_client.ls(before_date))[0]
    before_key = Path(before_run) / VERSION / FILE

    after_run = sorted(s3fs_client.ls(after_date))[0]
    after_key = Path(after_run) / VERSION / FILE

    print(f"Before: {before_key}")
    print(f"After: {after_key}")

    with tempfile.TemporaryDirectory() as tmpdir:
        s3fs_client.get_file(rpath=before_key, lpath=Path(tmpdir) / "before.parquet")
        s3fs_client.get_file(rpath=after_key, lpath=Path(tmpdir) / "after.parquet")

        before_df = compare.read(Path(tmpdir) / "before.parquet")
        after_df = compare.read(Path(tmpdir) / "after.parquet")

    diff_df = compare.compare(
        before_df,
        after_df,
        index_column="id",
        meta_columns=["source"],
    )

    texts = compare.summarize(diff_df)

    CHANNEL = "#lab-data-inclusion-alertes"

    before_ds = before_date.split("/")[-1]
    after_ds = after_date.split("/")[-1]
    response = slack_hook.client.chat_postMessage(
        channel=CHANNEL,
        markdown_text=f"### Changements entre {before_ds} et {after_ds}",
    )

    thread_ts = response["ts"]

    MD_MAX_CHUNK_SIZE = 11_500  # Slack limits markdown to 12k chars
    for text in texts:
        for i in range(0, len(text), MD_MAX_CHUNK_SIZE):
            slack_hook.client.chat_postMessage(
                channel=CHANNEL,
                markdown_text=text[i : i + MD_MAX_CHUNK_SIZE],
                thread_ts=thread_ts,
            )


@dag(
    schedule="45 8 * * *",
    **dags.common_args(use_sentry=True),
)
def summarize_changes():
    compare_and_summarize()


dag = summarize_changes()


if __name__ == "__main__":
    dag.test()
