import logging
import textwrap

import airflow
import geopandas
import pandas as pd
import pendulum
from airflow.models import Variable
from airflow.operators import empty, python
from airflow.providers.postgres.operators import postgres

logger = logging.getLogger(__name__)

default_args = {}


def _import_stock_etablissement_historique():
    import sqlalchemy as sqla
    import tqdm

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    target_table = "sirene_etablissement_historique"

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():

            # process the csv file by chunk
            with pd.read_csv(
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
            ) as reader:

                with tqdm.tqdm() as pbar:

                    for chunck_df in reader:
                        chunck_df.to_sql(
                            target_table,
                            con=conn,
                            if_exists="replace" if pbar.n == 0 else "append",
                            index=False,
                            dtype={"dateDebut": sqla.Date(), "dateFin": sqla.Date()},
                        )

                        pbar.update(len(chunck_df))

            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX sirene_etab_histo_siret_idx
                        ON {target_table} ("siret");
                    """
                )
            )


def _import_stock_etablissement_liens_succession():
    import sqlalchemy as sqla
    import tqdm

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    target_table = "sirene_etablissement_succession"

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():

            # process the csv file by chunk
            with pd.read_csv(
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
            ) as reader:

                with tqdm.tqdm() as pbar:

                    for chunck_df in reader:
                        chunck_df.to_sql(
                            target_table,
                            con=conn,
                            if_exists="replace" if pbar.n == 0 else "append",
                            index=False,
                            dtype={"dateLienSuccession": sqla.Date()},
                        )

                        pbar.update(len(chunck_df))

            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX sirene_etab_succession_siret_idx
                        ON {target_table} ("siretEtablissementPredecesseur");
                    """
                )
            )


def _import_stock_unite_legale():
    import tqdm

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    target_table = "sirene_stock_unite_legale"

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():

            # process the csv file by chunk
            with pd.read_csv(
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
            ) as reader:

                with tqdm.tqdm() as pbar:

                    for chunck_df in reader:
                        chunck_df.to_sql(
                            target_table,
                            con=conn,
                            if_exists="replace" if pbar.n == 0 else "append",
                            index=False,
                        )

                        pbar.update(len(chunck_df))

            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX sirene_stock_unite_legale_siren_idx
                        ON {target_table} ("siren");
                    """
                )
            )

            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX sirene_stock_unite_legale_cat_juridique_idx
                        ON {target_table} ("categorieJuridiqueUniteLegale");
                    """
                )
            )


def _import_stock_etablissement_geocode():
    import tqdm

    pg_hook = postgres.PostgresHook(postgres_conn_id="pg")

    target_table = "sirene_etablissement_geocode"

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():

            # process the csv file by chunk
            with pd.read_csv(
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
            ) as reader:

                with tqdm.tqdm() as pbar:

                    for chunck_df in reader:
                        # Add geometry from lon/lat
                        # Computing it now rather than later (unlike searchable fields),
                        # because it will likely be immutable (whereas searchable fields
                        # could need small adjustments that would not need full download
                        # and import)
                        chunck_df = geopandas.GeoDataFrame(
                            data=chunck_df,
                            geometry=geopandas.points_from_xy(
                                chunck_df.longitude,
                                chunck_df.latitude,
                                crs="EPSG:4326",
                            ),
                        )

                        chunck_df.to_postgis(
                            target_table,
                            con=conn,
                            if_exists="replace" if pbar.n == 0 else "append",
                            index=False,
                        )

                        pbar.update(len(chunck_df))

            # Add primary key (i.e. create much needed indexes on siret column)
            conn.execute(
                textwrap.dedent(
                    f"""
                        ALTER TABLE {target_table}
                        ADD PRIMARY KEY (siret);
                    """
                )
            )

            # Index geometry column using the geography type cast (to be able to compute
            # and compare in meters)
            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX sirene_etablissement_geocode_geom_idx
                        ON {target_table}
                        USING gist((geometry::geography));
                    """
                )
            )

            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX sirene_etablissement_geocode_code_commune_like
                        ON {target_table}
                        ("codeCommuneEtablissement" varchar_pattern_ops);
                    """
                )
            )


with airflow.DAG(
    dag_id="import_sirene",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    concurrency=1,
) as dag:
    start = empty.EmptyOperator(task_id="start")

    import_stock_etablissement_historique = python.PythonOperator(
        task_id="import_stock_etablissement_historique",
        python_callable=_import_stock_etablissement_historique,
    )

    import_stock_etablissement_liens_succession = python.PythonOperator(
        task_id="import_stock_etablissement_liens_succession",
        python_callable=_import_stock_etablissement_liens_succession,
    )

    import_stock_unite_legale = python.PythonOperator(
        task_id="import_stock_unite_legale",
        python_callable=_import_stock_unite_legale,
    )

    import_stock_etablissement_geocode = python.PythonOperator(
        task_id="import_stock_etablissement_geocode",
        python_callable=_import_stock_etablissement_geocode,
    )

    add_stock_etablissement_searchable_name = postgres.PostgresOperator(
        task_id="add_stock_etablissement_searchable_name",
        postgres_conn_id="pg",
        sql="sql/sirene/etab_add_searchable_name.sql",
    )

    add_stock_etablissement_searchable_l4 = postgres.PostgresOperator(
        task_id="add_stock_etablissement_searchable_l4",
        postgres_conn_id="pg",
        sql="sql/sirene/etab_add_searchable_l4.sql",
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

    (
        [
            import_stock_etablissement_geocode,
            import_stock_unite_legale,
        ]
        >> add_stock_etablissement_searchable_l4
        >> add_stock_etablissement_searchable_name
    )
