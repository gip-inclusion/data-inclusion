import logging
import textwrap
from pathlib import Path
from typing import Optional

import pandas as pd
import sqlalchemy as sqla
from sqlalchemy import exc as sqla_exc
from sqlalchemy.engine import Engine
from tqdm import tqdm

from data_inclusion.schema import models
from data_inclusion.scripts.tasks import utils

logger = logging.getLogger(__name__)

tqdm.pandas()

DIR = Path(__file__).parent


NEAREST_ESTABLISHMENTS_STMT = sqla.text(
    textwrap.dedent(
        """
-- 1. filter out etablissements farther than 1km
CREATE TEMP TABLE structures_with_similarities AS
WITH etablissement_within_radius AS (SELECT siret
    FROM sirene_etablissement_geocode
    WHERE ST_DWITHIN(
        ST_SETSRID(ST_MAKEPOINT(:longitude, :latitude)::geography, 4326),
        geometry::geography,
        1000)
),

-- 2. enhance with searchable fields
etablissement_with_searchable_fields AS (SELECT
    etablissement_within_radius.siret,
    sirene_etablissement_searchable_name.searchable_name,
    sirene_etablissement_searchable_l4.searchable_l4
    FROM etablissement_within_radius
    INNER JOIN sirene_etablissement_searchable_name
        ON etablissement_within_radius.siret
           = sirene_etablissement_searchable_name.siret
    INNER JOIN sirene_etablissement_searchable_l4
        ON etablissement_within_radius.siret
           = sirene_etablissement_searchable_l4.siret
),

-- 3. compute similarity scores
structures_with_similarities AS (SELECT
    siret,
    searchable_l4,
    searchable_name,
    SIMILARITY(
        searchable_l4, :normalized_address_str
    ) AS l4_similarity,
    SIMILARITY(
        searchable_name, :normalized_name_str
    ) AS name_similarity
    FROM etablissement_with_searchable_fields
    WHERE
        searchable_name % :normalized_name_str
        OR searchable_l4 % :normalized_address_str
)

-- 4. materialized results to speed up next WHERE clause
SELECT * FROM structures_with_similarities;

-- 5. compute score and get highest
SELECT
    siret,
    searchable_l4,
    searchable_name,
    (COALESCE(l4_similarity, 0) + 2 * COALESCE(name_similarity, 0)) / 3 AS score
FROM structures_with_similarities
WHERE
    name_similarity > 0.3
    AND l4_similarity > 0.3
ORDER BY 4 DESC
LIMIT 1;

"""
    )
)


def search_establishment_by_similarities(
    engine: Engine,
    nom: Optional[str] = None,
    adresse: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
) -> Optional[dict]:

    if nom is None or adresse is None or latitude is None or longitude is None:
        logger.debug("Missing data")
        return None

    with engine.connect() as conn:
        try:
            establishments_list = conn.execute(
                NEAREST_ESTABLISHMENTS_STMT,
                {
                    "normalized_name_str": nom.replace("'", " "),
                    "normalized_address_str": adresse.replace("'", " "),
                    "latitude": latitude,
                    "longitude": longitude,
                },
            ).all()
        except sqla_exc.DataError as exc:
            logger.error(exc)
            return None

    if len(establishments_list) == 0:
        logger.debug("No matching establishment")
        return None

    return dict(establishments_list[0])


SEARCH_MUNI_STMT = sqla.text(
    textwrap.dedent(
        """
-- 1. filter for active entreprises with categorie juridique of interest
WITH entreprise_filtered AS (SELECT
    siren,
    "nicSiegeUniteLegale"
    FROM sirene_stock_unite_legale
    WHERE "etatAdministratifUniteLegale" = 'A'
        AND "categorieJuridiqueUniteLegale" IN ('7210', '7313', '7312')
),

-- 2. get the associated etablishments within 10km and with activity of interest
etablissement_filtered AS (
    SELECT sirene_etablissement_geocode.*
    FROM sirene_etablissement_geocode
    INNER JOIN entreprise_filtered
        ON
            LEFT(
                sirene_etablissement_geocode.siret, 9
            ) = entreprise_filtered.siren
    WHERE
        ST_DWITHIN(
            ST_SETSRID(ST_MAKEPOINT(:longitude, :latitude)::geography, 4326),
            sirene_etablissement_geocode.geometry::geography,
            10000
        )
        AND sirene_etablissement_geocode."activitePrincipaleEtablissement"
        = '84.11Z'
),

-- 3. enhance with searchable fields
etablissement_with_searchable_fields AS (SELECT
    etablissement_filtered.siret,
    sirene_etablissement_searchable_name.searchable_name
    FROM etablissement_filtered
    INNER JOIN sirene_etablissement_searchable_name
        ON etablissement_filtered.siret
           = sirene_etablissement_searchable_name.siret
),

-- 4. annotate with similarity
etablissement_with_similarities AS (
    SELECT
        siret,
        WORD_SIMILARITY(searchable_name, :normalized_commune_str) AS name_similarity
    FROM etablissement_with_searchable_fields
)

-- 3. compute score and get highest
SELECT *
FROM etablissement_with_similarities
ORDER BY name_similarity DESC;
"""
    )
)


def search_municipality(
    engine: Engine,
    commune: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
):
    if commune is None or latitude is None or longitude is None:
        logger.debug("Missing data")
        return None

    with engine.connect() as conn:
        try:
            establishments_list = conn.execute(
                SEARCH_MUNI_STMT,
                {
                    "normalized_commune_str": commune.replace("'", " "),
                    "latitude": latitude,
                    "longitude": longitude,
                },
            ).all()
        except sqla_exc.DataError as exc:
            logger.error(exc)
            return None

    if len(establishments_list) == 0:
        logger.debug("No matching establishment")
        return None

    return dict(establishments_list[0])


def search_establishment(
    engine: Engine,
    commune: Optional[str] = None,
    nom: Optional[str] = None,
    adresse: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
    typologie: Optional[models.Typologie] = None,
):
    if typologie == models.Typologie.MUNI:
        return search_municipality(
            commune=commune,
            latitude=latitude,
            longitude=longitude,
            engine=engine,
        )

    return search_establishment_by_similarities(
        nom=nom,
        adresse=adresse,
        latitude=latitude,
        longitude=longitude,
        engine=engine,
    )


def siretize_normalized_dataframe(
    structures_df: pd.DataFrame,
    sirene_database_url: str,
) -> pd.DataFrame:
    utils.log_df_info(structures_df, logger)

    engine = sqla.create_engine(sirene_database_url)

    def get_typologie(value: Optional[str]) -> Optional[models.Typologie]:
        if value is None:
            return None
        return next(
            (typologie for typologie in models.Typologie if typologie.value == value),
            None,
        )

    establishments_df = structures_df.progress_apply(
        lambda row: search_establishment(
            engine=engine,
            nom=row.nom,
            adresse=row.adresse,
            latitude=row.latitude,
            longitude=row.longitude,
            commune=row.commune,
            typologie=get_typologie(row.typologie),
        )
        or {"siret": None},
        axis="columns",
        result_type="expand",
    )

    output_df = structures_df
    output_df = output_df.assign(siret=establishments_df.siret)
    output_df = output_df.assign(
        antenne=establishments_df.antenne if "antenne" in establishments_df else None
    )

    utils.log_df_info(output_df, logger)

    return output_df
