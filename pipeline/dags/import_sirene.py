import logging

import airflow
import pendulum
from airflow.operators import empty, python

from dags.virtualenvs import PYTHON_BIN_PATH

logger = logging.getLogger(__name__)

default_args = {}


def _import_stock_etablissement_historique():
    import textwrap

    import pandas as pd
    import sqlalchemy as sqla
    import tqdm
    from airflow.models import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="pg")

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
                        chunck_df = chunck_df.rename(
                            columns={
                                "siret": "siret",  # noqa: E501
                                "dateDebut": "date_debut",  # noqa: E501
                                "dateFin": "date_fin",  # noqa: E501
                                "changementEtatAdministratifEtablissement": "changement_etat_administratif_etablissement",  # noqa: E501
                                "etatAdministratifEtablissement": "etat_administratif_etablissement",  # noqa: E501
                            }
                        )

                        chunck_df.to_sql(
                            target_table,
                            con=conn,
                            if_exists="replace" if pbar.n == 0 else "append",
                            index=False,
                            dtype={
                                "date_debut": sqla.Date(),
                                "date_fin": sqla.Date(),
                            },
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
    import textwrap

    import pandas as pd
    import sqlalchemy as sqla
    import tqdm
    from airflow.models import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="pg")

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
                        chunck_df = chunck_df.rename(
                            columns={
                                "siretEtablissementPredecesseur": "siret_etablissement_predecesseur",  # noqa: E501
                                "siretEtablissementSuccesseur": "siret_etablissement_successeur",  # noqa: E501
                                "dateLienSuccession": "date_lien_succession",  # noqa: E501
                                "transfertSiege": "transfert_siege",  # noqa: E501
                                "continuiteEconomique": "continuite_economique",  # noqa: E501
                                "dateDernierTraitementLienSuccession": "date_dernier_traitement_lien_succession",  # noqa: E501
                            }
                        )
                        chunck_df.to_sql(
                            target_table,
                            con=conn,
                            if_exists="replace" if pbar.n == 0 else "append",
                            index=False,
                            dtype={"date_lien_succession": sqla.Date()},
                        )

                        pbar.update(len(chunck_df))

            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX sirene_etab_succession_siret_idx
                        ON {target_table} ("siret_etablissement_predecesseur");
                    """
                )
            )


def _import_stock_unite_legale():
    import textwrap

    import pandas as pd
    import sqlalchemy as sqla
    import tqdm
    from airflow.models import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="pg")

    target_table = "sirene_stock_unite_legale"

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():
            # process the csv file by chunk
            with pd.read_csv(
                Variable.get("SIRENE_STOCK_UNITE_LEGALE_FILE_URL"),
                usecols=[
                    "activitePrincipaleUniteLegale",
                    "caractereEmployeurUniteLegale",
                    "categorieEntreprise",
                    "categorieJuridiqueUniteLegale",
                    "dateCreationUniteLegale",
                    # "dateDernierTraitementUniteLegale",
                    "denominationUniteLegale",
                    "denominationUsuelle1UniteLegale",
                    "denominationUsuelle2UniteLegale",
                    "denominationUsuelle3UniteLegale",
                    "economieSocialeSolidaireUniteLegale",
                    "etatAdministratifUniteLegale",
                    "identifiantAssociationUniteLegale",
                    "nicSiegeUniteLegale",
                    "nomUniteLegale",
                    "nomUsageUniteLegale",
                    "prenom1UniteLegale",
                    "sigleUniteLegale",
                    "siren",
                    "societeMissionUniteLegale",
                    "trancheEffectifsUniteLegale",
                ],
                parse_dates=["dateCreationUniteLegale"],
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
                        chunck_df = chunck_df.rename(
                            columns={
                                "activitePrincipaleUniteLegale": "activite_principale_unite_legale",  # noqa: E501
                                "caractereEmployeurUniteLegale": "caractere_employeur_unite_legale",  # noqa: E501
                                "categorieEntreprise": "categorie_entreprise",  # noqa: E501
                                "categorieJuridiqueUniteLegale": "categorie_juridique_unite_legale",  # noqa: E501
                                "dateCreationUniteLegale": "date_creation_unite_legale",  # noqa: E501
                                "denominationUniteLegale": "denomination_unite_legale",  # noqa: E501
                                "denominationUsuelle1UniteLegale": "denomination_usuelle_1_unite_legale",  # noqa: E501
                                "denominationUsuelle2UniteLegale": "denomination_usuelle_2_unite_legale",  # noqa: E501
                                "denominationUsuelle3UniteLegale": "denomination_usuelle_3_unite_legale",  # noqa: E501
                                "economieSocialeSolidaireUniteLegale": "economie_sociale_solidaire_unite_legale",  # noqa: E501
                                "etatAdministratifUniteLegale": "etat_administratif_unite_legale",  # noqa: E501
                                "identifiantAssociationUniteLegale": "identifiant_association_unite_legale",  # noqa: E501
                                "nicSiegeUniteLegale": "nic_siege_unite_legale",  # noqa: E501
                                "nomUniteLegale": "nom_unite_legale",  # noqa: E501
                                "nomUsageUniteLegale": "nom_usage_unite_legale",  # noqa: E501
                                "prenom1UniteLegale": "prenom_1_unite_legale",  # noqa: E501
                                "sigleUniteLegale": "sigle_unite_legale",  # noqa: E501
                                "siren": "siren",  # noqa: E501
                                "societeMissionUniteLegale": "societe_mission_unite_legale",  # noqa: E501
                                "trancheEffectifsUniteLegale": "tranche_effectifs_unite_legale",  # noqa: E501
                            }
                        )

                        chunck_df.to_sql(
                            target_table,
                            con=conn,
                            if_exists="replace" if pbar.n == 0 else "append",
                            index=False,
                            dtype={"date_creation_unite_legale": sqla.Date()},
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
                        ON {target_table} ("categorie_juridique_unite_legale");
                    """
                )
            )


def _import_stock_etablissement_geocode():
    import textwrap

    import geopandas
    import pandas as pd
    import sqlalchemy as sqla
    import tqdm
    from airflow.models import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="pg")

    target_table = "sirene_etablissement_geocode"

    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():
            # process the csv file by chunk
            with pd.read_csv(
                Variable.get("SIRENE_STOCK_ETAB_GEOCODE_FILE_URL"),
                # unused fields are commented out to reduce overall size
                usecols=[
                    "activitePrincipaleEtablissement",
                    # "activitePrincipaleRegistreMetiersEtablissement",
                    "caractereEmployeurEtablissement",
                    "codeCedex2Etablissement",
                    "codeCedexEtablissement",
                    "codeCommune2Etablissement",
                    "codeCommuneEtablissement",
                    # "codePaysEtranger2Etablissement",
                    # "codePaysEtrangerEtablissement",
                    "codePostal2Etablissement",
                    "codePostalEtablissement",
                    "complementAdresse2Etablissement",
                    "complementAdresseEtablissement",
                    "dateCreationEtablissement",
                    "dateDebut",
                    "denominationUsuelleEtablissement",
                    # "distributionSpeciale2Etablissement",
                    # "distributionSpecialeEtablissement",
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
                    # "libelleCommuneEtranger2Etablissement",
                    # "libelleCommuneEtrangerEtablissement",
                    # "libellePaysEtranger2Etablissement",
                    # "libellePaysEtrangerEtablissement",
                    "libelleVoie2Etablissement",
                    "libelleVoieEtablissement",
                    "nomenclatureActivitePrincipaleEtablissement",
                    "numeroVoie2Etablissement",
                    "numeroVoieEtablissement",
                    "statutDiffusionEtablissement",
                    "trancheEffectifsEtablissement",
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
                parse_dates=["dateCreationEtablissement", "dateDebut"],
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
                    "trancheEffectifsEtablissement": str,
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
                        chunck_df = chunck_df.rename(
                            columns={
                                "activitePrincipaleEtablissement": "activite_principale_etablissement",  # noqa: E501
                                "caractereEmployeurEtablissement": "caractere_employeur_etablissement",  # noqa: E501
                                "codeCedex2Etablissement": "code_cedex_2_etablissement",  # noqa: E501
                                "codeCedexEtablissement": "code_cedex_etablissement",  # noqa: E501
                                "codeCommune2Etablissement": "code_commune_2_etablissement",  # noqa: E501
                                "codeCommuneEtablissement": "code_commune_etablissement",  # noqa: E501
                                "codePostal2Etablissement": "code_postal_2_etablissement",  # noqa: E501
                                "codePostalEtablissement": "code_postal_etablissement",  # noqa: E501
                                "complementAdresse2Etablissement": "complement_adresse_2_etablissement",  # noqa: E501
                                "complementAdresseEtablissement": "complement_adresse_etablissement",  # noqa: E501
                                "dateCreationEtablissement": "date_creation_etablissement",  # noqa: E501
                                "dateDebut": "date_debut",  # noqa: E501
                                "denominationUsuelleEtablissement": "denomination_usuelle_etablissement",  # noqa: E501
                                "enseigne1Etablissement": "enseigne_1_etablissement",  # noqa: E501
                                "enseigne2Etablissement": "enseigne_2_etablissement",  # noqa: E501
                                "enseigne3Etablissement": "enseigne_3_etablissement",  # noqa: E501
                                "etablissementSiege": "etablissement_siege",  # noqa: E501
                                "etatAdministratifEtablissement": "etat_administratif_etablissement",  # noqa: E501
                                "indiceRepetition2Etablissement": "indice_repetition_2_etablissement",  # noqa: E501
                                "indiceRepetitionEtablissement": "indice_repetition_etablissement",  # noqa: E501
                                "libelleCedex2Etablissement": "libelle_cedex_2_etablissement",  # noqa: E501
                                "libelleCedexEtablissement": "libelle_cedex_etablissement",  # noqa: E501
                                "libelleCommune2Etablissement": "libelle_commune_2_etablissement",  # noqa: E501
                                "libelleCommuneEtablissement": "libelle_commune_etablissement",  # noqa: E501
                                "libelleVoie2Etablissement": "libelle_voie_2_etablissement",  # noqa: E501
                                "libelleVoieEtablissement": "libelle_voie_etablissement",  # noqa: E501
                                "nomenclatureActivitePrincipaleEtablissement": "nomenclature_activite_principale_etablissement",  # noqa: E501
                                "numeroVoie2Etablissement": "numero_voie_2_etablissement",  # noqa: E501
                                "numeroVoieEtablissement": "numero_voie_etablissement",  # noqa: E501
                                "statutDiffusionEtablissement": "statut_diffusion_etablissement",  # noqa: E501
                                "trancheEffectifsEtablissement": "tranche_effectifs_etablissement",  # noqa: E501
                                "typeVoie2Etablissement": "type_voie_2_etablissement",  # noqa: E501
                                "typeVoieEtablissement": "type_voie_etablissement",  # noqa: E501
                                "siret": "siret",
                                "longitude": "longitude",
                                "latitude": "latitude",
                                "geo_score": "geo_score",
                                "geo_type": "geo_type",
                                "geo_adresse": "geo_adresse",
                                "geo_id": "geo_id",
                                "geo_ligne": "geo_ligne",
                                "geo_l4": "geo_l4",
                                "geo_l5": "geo_l5",
                            }
                        )

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
                            dtype={
                                "date_creation_etablissement": sqla.Date(),
                                "date_debut": sqla.Date(),
                            },
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
                        ("code_commune_etablissement" varchar_pattern_ops);
                    """
                )
            )

            conn.execute(
                textwrap.dedent(
                    f"""
                        CREATE INDEX sirene_etablissement_geocode_commune_trgm_idx
                        ON {target_table}
                        USING gin ("libelle_commune_etablissement" gin_trgm_ops);
                    """
                )
            )


def _index_etablissements():
    import logging
    import textwrap

    import elasticsearch_dsl
    import pandas as pd
    import tqdm
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from elasticsearch import helpers
    from elasticsearch_dsl import (
        Boolean,
        GeoPoint,
        Keyword,
        Text,
        analyzer,
        connections,
        token_filter,
        tokenizer,
    )

    logger = logging.getLogger(__name__)

    connections.create_connection(
        hosts=["http://elasticsearch:9200"],
        http_auth=("elastic", "changeme"),
        retry_on_timeout=True,
    )

    es_conn = connections.get_connection()
    logger.info(es_conn.cluster.health())

    # Define filters
    french_elision = token_filter(
        "french_elision",
        type="elision",
        articles_case=True,
        articles=[
            "l",
            "m",
            "t",
            "qu",
            "n",
            "s",
            "j",
            "d",
            "c",
            "jusqu",
            "quoiqu",
            "lorsqu",
            "puisqu",
        ],
    )

    annuaire_analyzer = analyzer(
        "annuaire_analyzer",
        tokenizer=tokenizer("icu_tokenizer"),
        filter=[
            "lowercase",
            french_elision,
            token_filter("french_stop", type="stop", stopwords="_french_"),
            "icu_folding",
            # ignore_case option deprecated, use lowercase filter before synonym filter
            token_filter("french_synonym", type="synonym", expand=True, synonyms=[]),
            "asciifolding",
            token_filter("french_stemmer", type="stemmer", language="light_french"),
        ],
    )

    target_index_name = "siret"

    class EtablissementDocument(elasticsearch_dsl.Document):
        siren = Keyword(required=True)
        siret = Keyword(required=True)

        nom_complet = Text(analyzer=annuaire_analyzer, fields={"keyword": Keyword()})

        denomination_unite_legale = Text(analyzer=annuaire_analyzer)
        sigle_unite_legale = Keyword()

        etablissement_siege = Boolean()
        denomination_usuelle_etablissement = Text(analyzer=annuaire_analyzer)
        activite_principale_etablissement = Keyword()
        code_commune_etablissement = Keyword()
        code_postal_etablissement = Keyword()
        libelle_commune_etablissement = Text(analyzer=annuaire_analyzer)
        libelle_voie_etablissement = Text(analyzer=annuaire_analyzer)
        numero_voie_etablissement = Text()
        categorie_juridique_unite_legale = Keyword()

        label_code_naf = Text(analyzer=annuaire_analyzer)
        class_label_code_naf = Text(analyzer=annuaire_analyzer)
        group_label_code_naf = Text(analyzer=annuaire_analyzer)
        division_label_code_naf = Text(analyzer=annuaire_analyzer)
        section_label_code_naf = Text(analyzer=annuaire_analyzer)

        adresse_complete = Text(analyzer=annuaire_analyzer)

        departement = Keyword()

        coordonnees = GeoPoint()
        longitude = Text()
        latitude = Text()

        class Index:
            settings = {"number_of_shards": 1, "number_of_replicas": 0}
            name = target_index_name

    index = elasticsearch_dsl.Index(target_index_name)
    if index.exists():
        index.delete()
    EtablissementDocument.init(target_index_name)

    def format_nom_complet(df: pd.DataFrame) -> pd.Series:
        nom_complet = df[
            [
                "denomination_unite_legale",
                "denomination_usuelle_etablissement",
                "sigle_unite_legale",
            ]
        ].apply(lambda row: row.str.cat(sep=" "), axis=1)
        nom_complet = nom_complet.str.lower()

        # TODO: manage individual company without official company name ?

        return nom_complet

    def format_adresse_complete(df: pd.DataFrame) -> pd.Series:
        # le libelle de la commune ne fait pas partie de l'adresse complete
        # car c'est redondant et on a la chance d'avoir souvent le libelle de la commune
        # clairement séparée du reste de l'adresse
        # adresse_complete = l4

        adresse_complete = df[
            [
                "numero_voie_etablissement",
                "indice_repetition_etablissement",
                "type_voie_etablissement",
                "libelle_voie_etablissement",
                "complement_adresse_etablissement",
            ]
        ].apply(lambda row: row.str.cat(sep=" "), axis=1)

        # TODO: cedex ?

        adresse_complete = adresse_complete.str.lower()
        return adresse_complete

    def format_departement(df: pd.DataFrame) -> pd.Series:
        departement = df.code_commune_etablissement.copy()

        in_outremer_idx = departement.str.startswith("97", na=False)
        departement.loc[in_outremer_idx] = departement.loc[in_outremer_idx].str[:3]
        departement.loc[~in_outremer_idx] = departement.loc[~in_outremer_idx].str[:2]
        return departement

    def format_coordonnees(df: pd.DataFrame) -> pd.Series:
        coordonnees = df.latitude.astype(str) + "," + df.longitude.astype(str)
        coordonnees.loc[df.latitude.isna() | df.longitude.isna()] = None
        return coordonnees

    pg_hook = PostgresHook(postgres_conn_id="pg")
    engine = pg_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        with conn.begin():
            # process by chunk
            with tqdm.tqdm() as pbar:
                for chunck_df in pd.read_sql_query(
                    sql=textwrap.dedent(
                        """\
                            SELECT
                                sirene_etablissement_geocode.siret,
                                sirene_etablissement_geocode.etablissement_siege,
                                sirene_etablissement_geocode.longitude,
                                sirene_etablissement_geocode.latitude,
                                sirene_etablissement_geocode.complement_adresse_etablissement,
                                sirene_etablissement_geocode.numero_voie_etablissement,
                                sirene_etablissement_geocode.indice_repetition_etablissement,
                                sirene_etablissement_geocode.type_voie_etablissement,
                                sirene_etablissement_geocode.libelle_voie_etablissement,
                                sirene_etablissement_geocode.denomination_usuelle_etablissement,
                                sirene_etablissement_geocode.activite_principale_etablissement,
                                sirene_etablissement_geocode.code_commune_etablissement,
                                sirene_etablissement_geocode.code_postal_etablissement,
                                sirene_etablissement_geocode.libelle_commune_etablissement,
                                sirene_stock_unite_legale.siren,
                                sirene_stock_unite_legale.denomination_unite_legale,
                                sirene_stock_unite_legale.sigle_unite_legale,
                                sirene_stock_unite_legale.categorie_juridique_unite_legale,
                                insee.code_naf.label AS label_code_naf,
                                insee.code_naf.class_label AS class_label_code_naf,
                                insee.code_naf.group_label AS group_label_code_naf,
                                insee.code_naf.division_label AS division_label_code_naf,
                                insee.code_naf.section_label AS section_label_code_naf
                            FROM sirene_etablissement_geocode
                            INNER JOIN sirene_stock_unite_legale
                            ON LEFT(sirene_etablissement_geocode.siret, 9)
                                = sirene_stock_unite_legale.siren
                            LEFT JOIN insee.code_naf
                            ON sirene_etablissement_geocode.activite_principale_etablissement
                                = insee.code_naf.code
                            WHERE sirene_etablissement_geocode.statut_diffusion_etablissement = 'O';
                        """  # noqa: E501
                    ),
                    con=conn,
                    chunksize=10000,
                ):
                    pbar.update(len(chunck_df))

                    # generate derivate fields
                    chunck_df["nom_complet"] = format_nom_complet(chunck_df)
                    chunck_df["adresse_complete"] = format_adresse_complete(chunck_df)
                    chunck_df["departement"] = format_departement(chunck_df)
                    chunck_df["coordonnees"] = format_coordonnees(chunck_df)
                    # TODO: libelle pour code APE

                    chunck_df = chunck_df.fillna("")
                    chunck_df = chunck_df.replace("", None)

                    # convert to document
                    serialized_documents = (
                        EtablissementDocument(
                            meta={"id": row["siret"]}, **row.to_dict()
                        ).to_dict(include_meta=True)
                        for _, row in chunck_df.iterrows()
                    )

                    # disable verbose elasticsearch logs
                    logger = logging.getLogger("elasticsearch")
                    logger.setLevel(logging.WARNING)

                    # send to ES
                    for success, info in helpers.parallel_bulk(
                        es_conn, serialized_documents, chunk_size=1000
                    ):
                        if not success:
                            logger.warning("Failed indexing:", info)


with airflow.DAG(
    dag_id="import_sirene",
    start_date=pendulum.datetime(2022, 1, 1, tz="Europe/Paris"),
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

    index_etablissements = python.ExternalPythonOperator(
        task_id="index_etablissements",
        python=str(PYTHON_BIN_PATH),
        python_callable=_index_etablissements,
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
        >> index_etablissements
    )
