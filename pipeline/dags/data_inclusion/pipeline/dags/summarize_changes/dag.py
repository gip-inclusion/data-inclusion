from airflow.sdk import Variable, dag, task

from data_inclusion.pipeline.common import dags


@task.virtualenv(
    requirements="requirements/tasks/requirements.txt",
    system_site_packages=False,
    venv_cache_path="/tmp/",
)
def compare_and_summarize():
    import tempfile
    from datetime import timedelta
    from pathlib import Path

    from slack_sdk.models import blocks

    from airflow.providers.amazon.aws.fs import s3 as s3fs
    from airflow.providers.amazon.aws.hooks import s3
    from airflow.providers.slack.hooks import slack

    from data_inclusion.pipeline.common.s3 import to_s3
    from data_inclusion.pipeline.scripts.compare import compare

    VERSION = "v1"
    FILE = "services.parquet"

    slack_hook = slack.SlackHook(slack_conn_id="slack")
    s3_hook = s3.S3Hook(aws_conn_id="s3")
    s3fs_client = s3fs.get_fs(conn_id="s3")

    BASE_KEY = Path(s3_hook.service_config["bucket_name"]) / "data" / "marts"

    before_date, after_date = sorted(s3fs_client.ls(BASE_KEY))[-2:]

    before_run = sorted(s3fs_client.ls(before_date))[-1]
    before_key = Path(before_run) / VERSION / FILE

    after_run = sorted(s3fs_client.ls(after_date))[-1]
    after_key = Path(after_run) / VERSION / FILE

    print(f"Before: {before_key}")
    print(f"After: {after_key}")

    with tempfile.TemporaryDirectory() as tmpdir:
        s3fs_client.get_file(rpath=before_key, lpath=Path(tmpdir) / "before.parquet")
        s3fs_client.get_file(rpath=after_key, lpath=Path(tmpdir) / "after.parquet")

        before_df = compare.read(path=Path(tmpdir) / "before.parquet")
        after_df = compare.read(path=Path(tmpdir) / "after.parquet")

    diff = compare.Diff(
        before_df=before_df,
        after_df=after_df,
        pk_col="id",
        meta_cols=["source"],
        tolerances={
            compare.cs.date() | compare.cs.datetime(): compare.TimeDeltaTolerance(
                timedelta(weeks=4)
            ),
            compare.cs.float(): compare.ThresholdTolerance(0.8),
        },
    )

    before_ds = before_date.split("/")[-1]
    after_ds = after_date.split("/")[-1]
    title = f"## Changements entre {before_ds} et {after_ds}"
    summary = diff.summarize(llm=True)

    # show summary in logs
    print(summary)

    # save summary to s3
    out_path = after_key.parent.relative_to(s3_hook.service_config["bucket_name"])
    to_s3(out_path / "changes_summary.md", "\n\n".join([title, summary]))

    # notify on slack
    CHANNEL = "#lab-data-inclusion-alertes"
    response = slack_hook.client.chat_postMessage(
        channel=CHANNEL,
        markdown_text=title,
    )
    thread_ts = response["ts"]

    for text in summary.split("\n---\n"):
        if len(text) < blocks.MarkdownBlock.text_max_length:
            slack_hook.client.chat_postMessage(
                channel=CHANNEL,
                markdown_text=text,
                thread_ts=thread_ts,
            )


@dag(
    schedule="45 8 * * *",
    **dags.common_args(use_sentry=True),
)
def summarize_changes():
    OPENAI_API_KEY = Variable.get("ANTHROPIC_API_KEY")
    OPENAI_BASE_URL = "https://api.anthropic.com/v1/"

    compare_and_summarize.override(
        env_vars={
            "OPENAI_API_KEY": OPENAI_API_KEY,
            "OPENAI_BASE_URL": OPENAI_BASE_URL,
        }
    )()


dag = summarize_changes()


if __name__ == "__main__":
    dag.test()
