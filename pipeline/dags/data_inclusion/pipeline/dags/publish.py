from airflow.decorators import dag, task

from data_inclusion.pipeline.common import dags, tasks


@task.external_python(python=tasks.PYTHON_BIN_PATH)
def publish_to_datagouv():
    import io
    import tempfile

    import geopandas as gpd
    import pendulum

    from airflow.models import Variable
    from airflow.providers.postgres.hooks import postgres

    from data_inclusion.pipeline.sources import datagouv
    from data_inclusion.schema import v0

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    DATAGOUV_API_URL = "https://www.data.gouv.fr/api/"
    DATAGOUV_API_KEY = Variable.get("DATAGOUV_API_KEY")
    DATAGOUV_DI_DATASET_ID = "6233723c2c1e4a54af2f6b2d"
    DATAGOUV_DI_RESOURCE_IDS = {
        "structures": {
            "json": "4fc64287-e869-4550-8fb9-b1e0b7809ffa",
            "csv": "fd4cb3ef-5c31-4c99-92fe-2cd8016c0ca5",
            "xlsx": "fad88958-c9a7-4914-a9b8-89d1285c210a",
            "geojson": "42d46a21-eeef-433c-b3c3-961e1c37bc93",
        },
        "services": {
            "json": "0eac1faa-66f9-4e49-8fb3-f0721027d89f",
            "csv": "5abc151a-5729-4055-b0a9-d5691276f461",
            "xlsx": "de2eb57b-113d-48eb-95d2-59a69ba36eb1",
            "geojson": "307529c0-dcc5-449a-a88d-9290a8a86a14",
        },
    }

    datagouv_client = datagouv.DataGouvClient(
        base_url=DATAGOUV_API_URL,
        api_key=DATAGOUV_API_KEY,
    )

    date_str = pendulum.today().to_date_string()

    def to_geojson(df, buf) -> None:
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(df.longitude, df.latitude),
            crs="EPSG:4326",
        )

        with tempfile.NamedTemporaryFile() as tmp_file:
            gdf.to_file(tmp_file.name, driver="GeoJSON", engine="pyogrio")

            with open(tmp_file.name, "rb") as fp:
                buf.write(fp.read())
                buf.seek(0)

    to_buf_fn_by_format = {
        "json": lambda df, buf: df.to_json(buf, orient="records", force_ascii=False),
        "csv": lambda df, buf: df.to_csv(buf, index=False),
        "xlsx": lambda df, buf: df.to_excel(
            buf,
            engine="xlsxwriter",
            engine_kwargs={"options": {"strings_to_urls": False}},
        ),
        "geojson": to_geojson,
    }

    for resource in ["structures", "services"]:
        df = pg_hook.get_pandas_df(
            sql=f"SELECT * FROM public_marts.marts__{resource}",
        )

        # remove closed sources
        df = df.loc[df["_in_opendata"]]
        df = df.drop(columns="_in_opendata")

        # remove closed structures
        if resource == "structures":
            df = df.loc[~df["_is_closed"]]
            df = df.drop(columns="_is_closed")

        # remove pii
        df = df.assign(courriel=df["courriel"].mask(df["_has_pii"], None))
        df = df.assign(telephone=df["telephone"].mask(df["_has_pii"], None))
        df = df.drop(columns="_has_pii")

        # remove invalid rows
        df = df.loc[~df["_is_valid_v0"]]

        # keep only the columns we want to publish
        if resource == "structures":
            df = df[list(v0.Structure.model_fields.keys()) + ["_di_surrogate_id"]]
        elif resource == "services":
            df = df[
                list(v0.Service.model_fields.keys())
                + ["_di_surrogate_id", "_di_structure_surrogate_id"]
            ]

        df.info()

        for format in ["csv", "json", "xlsx", "geojson"]:
            with io.BytesIO() as buf:
                to_buf_fn_by_format[format](df, buf)
                datagouv_client.upload_dataset_resource(
                    dataset_id=DATAGOUV_DI_DATASET_ID,
                    resource_id=DATAGOUV_DI_RESOURCE_IDS[resource][format],
                    buf=buf,
                    filename=f"{resource}-inclusion-{date_str}.{format}",
                )


EVERY_MONDAY_AT_2PM = "0 14 * * 1"


@dag(
    description="Publish the consolidated dataset to datagouv",
    schedule=EVERY_MONDAY_AT_2PM,
    **dags.common_args(use_sentry=True),
)
def publish():
    publish_to_datagouv()


publish()
