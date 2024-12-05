from airflow.operators import python

from dag_utils.virtualenvs import PYTHON_BIN_PATH


def _preprocess_structures(logical_date, run_id):
    import logging

    import pandas as pd
    import phonenumbers
    from sqlalchemy.types import ARRAY, Float
    from unidecode import unidecode

    from dag_utils import pg

    logger = logging.getLogger(__name__)

    def format_phonenumber(s):
        if not s:
            return None
        try:
            p = phonenumbers.parse(s, "FR")
        except phonenumbers.phonenumberutil.NumberParseException:
            return None
        return phonenumbers.format_number(p, phonenumbers.PhoneNumberFormat.NATIONAL)

    logger.info("reading base marts structure table")
    with pg.connect_begin() as conn:
        structures_df = pd.read_sql_table(
            "marts_inclusion__structures",
            conn,
            schema="public_marts",
        )

    # focus on these sources
    structures_df = structures_df.loc[
        structures_df["source"].isin(
            [
                "action-logement",
                "dora",
                "fredo",
                "emplois-de-linclusion",
                "france-travail",
                "mediation-numerique",
                "mes-aides",
                "soliguide",
            ]
        )
    ]

    # exclude structures with long ds (mednum...)
    structures_df = structures_df.loc[
        structures_df["_di_surrogate_id"].str.len() <= 256
    ]

    # ignore antennes
    structures_df = structures_df.loc[structures_df["antenne"] != True]  # noqa: E712

    # ignore structures with no city code (no address, no geolocation)
    structures_df = structures_df.loc[structures_df["code_insee"].notnull()]

    structures_df["id"] = structures_df["_di_surrogate_id"]
    structures_df["date_maj"] = pd.to_datetime(structures_df["date_maj"]).dt.strftime(
        "%m/%d/%Y"
    )
    structures_df["nom"] = structures_df["nom"].str.lower().str.strip().apply(unidecode)
    structures_df["location"] = structures_df.apply(
        lambda row: [
            float(row["latitude"] if pd.notnull(row["latitude"]) else 0.0),
            float(row["longitude"] if pd.notnull(row["longitude"]) else 0.0),
        ],
        axis=1,
    )
    structures_df["siren"] = structures_df["siret"].str[:9]
    structures_df["telephone"] = structures_df["telephone"].apply(format_phonenumber)

    structures_df = structures_df[
        [
            "id",
            "source",
            "date_maj",
            "nom",
            "commune",
            "adresse",
            "location",
            "code_postal",
            "code_insee",
            "siret",
            "siren",
            "telephone",
            "courriel",
        ]
    ]

    structures_df = structures_df.set_index("id")

    with pg.connect_begin() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS dedupe;")
        structures_df.to_sql(
            "preprocessed_structures",
            conn,
            schema="dedupe",
            if_exists="replace",
            index=True,
            dtype={"location": ARRAY(Float)},
        )
        conn.execute("ALTER TABLE dedupe.preprocessed_structures ADD PRIMARY KEY (id);")


def preprocess_structures():
    return python.ExternalPythonOperator(
        task_id="python_preprocess_structures",
        python=str(PYTHON_BIN_PATH),
        python_callable=_preprocess_structures,
    )


def _deduplicate_structures(logical_date, run_id):
    import csv
    import json
    import logging

    import dedupe
    import pandas

    from dag_utils import pg
    from dag_utils.sources.utils import extract_http_content

    THRESHOLD = 0.75  # as defined in our Jupyter simulations
    MODEL_URL = (
        "https://github.com/gip-inclusion/data-inclusion"
        "/raw/2c4502c0c1a1166d4d216e58064e9dac12d3b6fe/deduplication/model.bin"
    )

    logger = logging.getLogger(__name__)

    logger.info(f"reading training data from {MODEL_URL=}")
    with open("/tmp/model.bin", "wb") as f:
        f.write(extract_http_content(MODEL_URL))

    with open("/tmp/model.bin", "rb") as f:
        deduper = dedupe.StaticDedupe(f)

    pg.create_schema("dedupe")

    logger.info("clustering the preprocessed structure table...")
    with pg.connect_begin() as conn:
        df = pandas.read_sql_table("preprocessed_structures", conn, schema="dedupe")

    data = json.loads(df.to_json(orient="records"))
    clustered_dupes = deduper.partition(
        data={d["id"]: d for d in data},
        threshold=THRESHOLD,
    )

    logger.info("formatting the clusters as CSV for insertion as an SQL table")
    # Airflow's copy_expert() method doesn't support StringIO objects
    with open("/tmp/structure_clusters.csv", "w") as f:
        writer = csv.writer(f)
        cluster_id = 0
        for cluster, scores in clustered_dupes:
            if len(cluster) >= 1:
                cluster_id += 1
                for structure_id, score in zip(cluster, scores):
                    writer.writerow(
                        [str(cluster_id), structure_id, score, len(cluster)]
                    )

    logger.info("creating and filling 'dedupe.structure_clusters' table")
    with pg.connect_begin() as conn:
        conn.execute("DROP TABLE IF EXISTS dedupe.structure_clusters")
        conn.execute(
            """
            CREATE TABLE dedupe.structure_clusters (
                id TEXT,
                structure_id TEXT,
                score FLOAT,
                size INTEGER,
                PRIMARY KEY(structure_id)
            )
            """
        )

    pg_hook = pg.hook()
    pg_hook.copy_expert(
        "COPY dedupe.structure_clusters FROM STDIN WITH CSV",
        filename="/tmp/structure_clusters.csv",
    )

    with pg.connect_begin() as conn:
        conn.execute("DROP INDEX IF EXISTS structure_cluster_id_idx")
        conn.execute(
            "CREATE INDEX structure_cluster_id_idx ON dedupe.structure_clusters (id)"
        )


def deduplicate_structures():
    return python.ExternalPythonOperator(
        task_id="python_deduplicate_structures",
        python=str(PYTHON_BIN_PATH),
        python_callable=_deduplicate_structures,
    )
