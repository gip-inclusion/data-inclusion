import pendulum

from airflow.decorators import dag, task

from dag_utils import date, dbt
from dag_utils.virtualenvs import PYTHON_BIN_PATH


@task.external_python(python=str(PYTHON_BIN_PATH))
def create_schema(name: str):
    from dag_utils import pg

    pg.create_schema(schema_name=name)


@task.external_python(python=str(PYTHON_BIN_PATH))
def import_stock_etablissement_historique():
    import pandas as pd
    import sqlalchemy as sqla

    from dag_utils import pg

    URL = "https://www.data.gouv.fr/api/1/datasets/r/88fbb6b4-0320-443e-b739-b4376a012c32"
    SCHEMA_NAME = "sirene"
    TABLE_NAME = "etablissement_historique"

    reader = pd.read_csv(
        URL,
        usecols=[
            "siret",
            "dateDebut",
            "dateFin",
            "changementEtatAdministratifEtablissement",
            "etatAdministratifEtablissement",
        ],
        parse_dates=["dateFin", "dateDebut"],
        dtype={"siret": str},
        chunksize=10000,
        encoding="utf-8",
        compression="zip",
    )

    with pg.connect_begin() as conn:
        for i, df_chunk in enumerate(reader):
            df_chunk.to_sql(
                schema=SCHEMA_NAME,
                name=TABLE_NAME,
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False,
                dtype={"dateDebut": sqla.Date(), "dateFin": sqla.Date()},
            )

        conn.execute(
            f"""
            CREATE INDEX sirene_etab_histo_siret_idx
            ON {SCHEMA_NAME}.{TABLE_NAME} (siret);
            """
        )


@task.external_python(python=str(PYTHON_BIN_PATH))
def import_stock_etablissement_liens_succession():
    import pandas as pd
    import sqlalchemy as sqla

    from dag_utils import pg

    URL = "https://www.data.gouv.fr/api/1/datasets/r/9c4d5d9c-4bbb-4b9c-837a-6155cb589e26"
    SCHEMA_NAME = "sirene"
    TABLE_NAME = "etablissement_succession"

    reader = pd.read_csv(
        URL,
        usecols=[
            "siretEtablissementPredecesseur",
            "siretEtablissementSuccesseur",
            "dateLienSuccession",
            "transfertSiege",
            "continuiteEconomique",
            "dateDernierTraitementLienSuccession",
        ],
        parse_dates=["dateLienSuccession"],
        dtype={
            "siretEtablissementPredecesseur": str,
            "siretEtablissementSuccesseur": str,
        },
        chunksize=10000,
        encoding="utf-8",
        compression="zip",
    )

    with pg.connect_begin() as conn:
        for i, df_chunk in enumerate(reader):
            df_chunk.to_sql(
                schema=SCHEMA_NAME,
                name=TABLE_NAME,
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False,
                dtype={"dateLienSuccession": sqla.Date()},
            )

        conn.execute(
            f"""
            CREATE INDEX sirene_etab_succession_siret_idx
            ON {SCHEMA_NAME}.{TABLE_NAME} ("siretEtablissementPredecesseur");
            """
        )


@task.external_python(python=str(PYTHON_BIN_PATH))
def import_stock_unite_legale():
    import pandas as pd

    from dag_utils import pg

    URL = "https://www.data.gouv.fr/api/1/datasets/r/825f4199-cadd-486c-ac46-a65a8ea1a047"
    SCHEMA_NAME = "sirene"
    TABLE_NAME = "stock_unite_legale"

    reader = pd.read_csv(
        URL,
        usecols=[
            "caractereEmployeurUniteLegale",
            "categorieEntreprise",
            "categorieJuridiqueUniteLegale",
            "denominationUniteLegale",
            "denominationUsuelle1UniteLegale",
            "denominationUsuelle2UniteLegale",
            "denominationUsuelle3UniteLegale",
            "economieSocialeSolidaireUniteLegale",
            "etatAdministratifUniteLegale",
            "identifiantAssociationUniteLegale",
            "nicSiegeUniteLegale",
            "sigleUniteLegale",
            "siren",
            "societeMissionUniteLegale",
            "trancheEffectifsUniteLegale",
        ],
        dtype={
            "siren": str,
            "denominationUsuelle1UniteLegale": str,
            "denominationUsuelle2UniteLegale": str,
            "denominationUsuelle3UniteLegale": str,
        },
        chunksize=10000,
        encoding="utf-8",
        compression="zip",
    )

    with pg.connect_begin() as conn:
        for i, df_chunk in enumerate(reader):
            df_chunk.to_sql(
                schema=SCHEMA_NAME,
                name=TABLE_NAME,
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False,
            )

        conn.execute(
            f"""
            CREATE INDEX sirene_stock_unite_legale_siren_idx
            ON {SCHEMA_NAME}.{TABLE_NAME} (siren);
            """
        )

        conn.execute(
            f"""
            CREATE INDEX sirene_stock_unite_legale_cat_juridique_idx
            ON {SCHEMA_NAME}.{TABLE_NAME} ("categorieJuridiqueUniteLegale");
            """
        )


@task.external_python(python=str(PYTHON_BIN_PATH))
def import_stock_etablissement():
    import pandas as pd

    from dag_utils import pg

    URL = (
        "https://www.data.gouv.fr/api/1/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576"
    )
    SCHEMA_NAME = "sirene"
    TABLE_NAME = "stock_etablissement"

    reader = pd.read_csv(
        URL,
        usecols=[
            "activitePrincipaleEtablissement",
            "caractereEmployeurEtablissement",
            "codeCedex2Etablissement",
            "codeCedexEtablissement",
            "codeCommune2Etablissement",
            "codeCommuneEtablissement",
            "codePostal2Etablissement",
            "codePostalEtablissement",
            "complementAdresse2Etablissement",
            "complementAdresseEtablissement",
            "denominationUsuelleEtablissement",
            "enseigne1Etablissement",
            "enseigne2Etablissement",
            "enseigne3Etablissement",
            "etablissementSiege",
            "etatAdministratifEtablissement",
            "indiceRepetition2Etablissement",
            "indiceRepetitionEtablissement",
            "libelleCedex2Etablissement",
            "libelleCedexEtablissement",
            "libelleCommune2Etablissement",
            "libelleCommuneEtablissement",
            "libelleVoie2Etablissement",
            "libelleVoieEtablissement",
            "nomenclatureActivitePrincipaleEtablissement",
            "numeroVoie2Etablissement",
            "numeroVoieEtablissement",
            "statutDiffusionEtablissement",
            "typeVoie2Etablissement",
            "typeVoieEtablissement",
            "siret",
        ],
        dtype={
            "activitePrincipaleEtablissement": str,
            "caractereEmployeurEtablissement": str,
            "codeCedex2Etablissement": str,
            "codeCedexEtablissement": str,
            "codeCommune2Etablissement": str,
            "codeCommuneEtablissement": str,
            "codePostal2Etablissement": str,
            "codePostalEtablissement": str,
            "complementAdresse2Etablissement": str,
            "complementAdresseEtablissement": str,
            "denominationUsuelleEtablissement": str,
            "enseigne1Etablissement": str,
            "enseigne2Etablissement": str,
            "enseigne3Etablissement": str,
            "etatAdministratifEtablissement": str,
            "indiceRepetition2Etablissement": str,
            "indiceRepetitionEtablissement": str,
            "libelleCedex2Etablissement": str,
            "libelleCedexEtablissement": str,
            "libelleCommune2Etablissement": str,
            "libelleCommuneEtablissement": str,
            "libelleVoie2Etablissement": str,
            "libelleVoieEtablissement": str,
            "nomenclatureActivitePrincipaleEtablissement": str,
            "numeroVoie2Etablissement": str,
            "numeroVoieEtablissement": str,
            "statutDiffusionEtablissement": str,
            "typeVoie2Etablissement": str,
            "typeVoieEtablissement": str,
            "siret": str,
        },
        chunksize=10000,
        encoding="utf-8",
        compression="zip",
    )

    with pg.connect_begin() as conn:
        for i, df_chunk in enumerate(reader):
            df_chunk.to_sql(
                schema=SCHEMA_NAME,
                name=TABLE_NAME,
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False,
            )

        # Add primary key (i.e. create much needed indexes on siret column)
        conn.execute(f"ALTER TABLE {SCHEMA_NAME}.{TABLE_NAME} ADD PRIMARY KEY (siret);")

        conn.execute(
            f"""
            CREATE INDEX import_stock_etablissement_code_commune_like
            ON {SCHEMA_NAME}.{TABLE_NAME} (
                "codeCommuneEtablissement" varchar_pattern_ops
            );
            """
        )

        conn.execute(
            f"""
            CREATE INDEX import_stock_etablissement_commune_trgm_idx
            ON {SCHEMA_NAME}.{TABLE_NAME} USING gin (
                "libelleCommuneEtablissement" gin_trgm_ops
            );
            """
        )


EVERY_MONTH_ON_THE_10TH = "30 3 10 * *"


@dag(
    start_date=pendulum.datetime(2022, 1, 1, tz=date.TIME_ZONE),
    schedule=EVERY_MONTH_ON_THE_10TH,
    catchup=False,
    concurrency=1,
)
def import_sirene():
    dbt_build_staging = dbt.dbt_operator_factory(
        task_id="dbt_build_staging",
        command="build",
        select="path:models/staging/sirene",
    )

    (
        create_schema(name="sirene")
        >> [
            import_stock_etablissement_historique(),
            import_stock_etablissement_liens_succession(),
            import_stock_etablissement(),
            import_stock_unite_legale(),
        ]
        >> dbt_build_staging
    )


import_sirene()
