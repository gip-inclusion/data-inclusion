import logging

import airflow
import pendulum
from airflow.operators import empty, python

from dag_utils.date import TIME_ZONE
from dag_utils.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}


def _import_stock_etablissement_historique():
    import pandas as pd
    import sqlalchemy as sqla
    from airflow.models import Variable

    from dag_utils import pg

    TABLE_NAME = "sirene_etablissement_historique"
    reader = pd.read_csv(
        Variable.get("SIRENE_STOCK_ETAB_HIST_FILE_URL"),
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
                TABLE_NAME,
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False,
                dtype={"dateDebut": sqla.Date(), "dateFin": sqla.Date()},
            )

        conn.execute(
            f'CREATE INDEX sirene_etab_histo_siret_idx ON {TABLE_NAME} ("siret");'
        )


def _import_stock_etablissement_liens_succession():
    import pandas as pd
    import sqlalchemy as sqla
    from airflow.models import Variable

    from dag_utils import pg

    TABLE_NAME = "sirene_etablissement_succession"
    reader = pd.read_csv(
        Variable.get("SIRENE_STOCK_ETAB_LIENS_SUCCESSION_URL"),
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
                TABLE_NAME,
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False,
                dtype={"dateLienSuccession": sqla.Date()},
            )

        conn.execute(
            "CREATE INDEX sirene_etab_succession_siret_idx "
            f'ON {TABLE_NAME} ("siretEtablissementPredecesseur");'
        )


def _import_stock_unite_legale():
    import pandas as pd
    from airflow.models import Variable

    from dag_utils import pg

    TABLE_NAME = "sirene_stock_unite_legale"
    reader = pd.read_csv(
        Variable.get("SIRENE_STOCK_UNITE_LEGALE_FILE_URL"),
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
                TABLE_NAME,
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False,
            )

        conn.execute(
            "CREATE INDEX sirene_stock_unite_legale_siren_idx "
            f'ON {TABLE_NAME} ("siren");'
        )

        conn.execute(
            "CREATE INDEX sirene_stock_unite_legale_cat_juridique_idx "
            f'ON {TABLE_NAME} ("categorieJuridiqueUniteLegale");'
        )


def _import_stock_etablissement_geocode():
    import geopandas
    import pandas as pd
    from airflow.models import Variable

    from dag_utils import pg

    TABLE_NAME = "sirene_etablissement_geocode"
    reader = pd.read_csv(
        Variable.get("SIRENE_STOCK_ETAB_GEOCODE_FILE_URL"),
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
            "longitude",
            "latitude",
            "geo_score",
            "geo_type",
            "geo_adresse",
            "geo_id",
            "geo_ligne",
            "geo_l4",
            "geo_l5",
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
            "geo_type": str,
            "geo_adresse": str,
            "geo_id": str,
            "geo_ligne": str,
            "geo_l4": str,
            "geo_l5": str,
        },
        chunksize=10000,
        encoding="utf-8",
    )

    with pg.connect_begin() as conn:
        for i, chunk in enumerate(reader):
            # Add geometry from lon/lat
            # Computing it now rather than later (unlike searchable fields),
            # because it will likely be immutable (whereas searchable fields
            # could need small adjustments that would not need full download
            # and import)
            df_chunk = geopandas.GeoDataFrame(
                data=chunk,
                geometry=geopandas.points_from_xy(
                    chunk.longitude,
                    chunk.latitude,
                    crs="EPSG:4326",
                ),
            )

            df_chunk.to_postgis(
                TABLE_NAME,
                con=conn,
                if_exists="replace" if i == 0 else "append",
                index=False,
            )

        # Add primary key (i.e. create much needed indexes on siret column)
        conn.execute(f"ALTER TABLE {TABLE_NAME} ADD PRIMARY KEY (siret);")

        # Index geometry column using the geography type cast (to be able to compute
        # and compare in meters)
        conn.execute(
            "CREATE INDEX sirene_etablissement_geocode_geom_idx "
            f"ON {TABLE_NAME} USING gist((geometry::geography));"
        )

        conn.execute(
            "CREATE INDEX sirene_etablissement_geocode_code_commune_like "
            f'ON {TABLE_NAME} ("codeCommuneEtablissement" varchar_pattern_ops)'
        )

        conn.execute(
            "CREATE INDEX sirene_etablissement_geocode_commune_trgm_idx "
            f'ON {TABLE_NAME} USING gin ("libelleCommuneEtablissement" gin_trgm_ops)'
        )


with airflow.DAG(
    dag_id="import_sirene",
    start_date=pendulum.datetime(2022, 1, 1, tz=TIME_ZONE),
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    concurrency=1,
) as dag:
    start = empty.EmptyOperator(task_id="start")

    import_stock_etablissement_historique = python.ExternalPythonOperator(
        task_id="import_stock_etablissement_historique",
        python=str(PYTHON_BIN_PATH),
        python_callable=_import_stock_etablissement_historique,
    )

    import_stock_etablissement_liens_succession = python.ExternalPythonOperator(
        task_id="import_stock_etablissement_liens_succession",
        python=str(PYTHON_BIN_PATH),
        python_callable=_import_stock_etablissement_liens_succession,
    )

    import_stock_unite_legale = python.ExternalPythonOperator(
        task_id="import_stock_unite_legale",
        python=str(PYTHON_BIN_PATH),
        python_callable=_import_stock_unite_legale,
    )

    import_stock_etablissement_geocode = python.ExternalPythonOperator(
        task_id="import_stock_etablissement_geocode",
        python=str(PYTHON_BIN_PATH),
        python_callable=_import_stock_etablissement_geocode,
    )

    (
        start
        >> [
            import_stock_etablissement_historique,
            import_stock_etablissement_liens_succession,
            import_stock_etablissement_geocode,
            import_stock_unite_legale,
        ]
    )
