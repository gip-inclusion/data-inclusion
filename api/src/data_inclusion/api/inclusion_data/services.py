import functools
import json
from collections import defaultdict
from pathlib import Path

import geoalchemy2
import sqlalchemy as sqla
from sqlalchemy import orm

from data_inclusion.api.decoupage_administratif import constants
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data import models, parameters
from data_inclusion.schema import v1


@functools.cache
def get_thematiques_by_group() -> dict[str, list[str]]:
    thematiques = defaultdict(list)
    for thematique in v1.Thematique:
        try:
            theme, _ = str(thematique.value).split("--")
        except ValueError:
            continue
        thematiques[theme].append(thematique.value)
    return thematiques


def list_structures_query(
    params: parameters.ListStructuresQueryParams,
    include_soliguide: bool,
) -> sqla.Select[tuple[models.Structure]]:
    query = sqla.select(models.Structure).options(
        orm.joinedload(models.Structure.doublons),
    )

    if not include_soliguide:
        query = query.filter(models.Structure.source != "soliguide")

    if params.sources is not None:
        query = query.filter(
            models.Structure.source == sqla.any_(sqla.literal(params.sources))
        )

    if params.code_commune is not None:
        query = query.filter_by(code_insee=params.code_commune)

    if params.departement is not None:
        query = query.filter(
            models.Structure.code_insee.startswith(params.departement.code)
        )

    if params.region is not None:
        query = query.join(Commune).options(
            orm.contains_eager(models.Structure.commune_)
        )
        query = query.filter(Commune.region == params.region.code)

    if params.reseaux_porteurs is not None:
        query = query.filter(
            sqla.exists(
                sqla.select(sqla.literal(1))
                .select_from(
                    sqla.func.unnest(models.Structure.reseaux_porteurs).alias("item")
                )
                .where(
                    sqla.literal_column("item")
                    == sqla.any_(
                        sqla.literal([f.value for f in params.reseaux_porteurs])
                    )
                )
            )
        )

    if params.exclure_doublons:
        cluster_key = sqla.func.coalesce(
            models.Structure._cluster_id,
            models.Structure.id,
        )
        query = query.distinct(cluster_key).order_by(
            cluster_key,
            models.Structure.score_qualite.desc(),
            models.Structure.date_maj.desc().nulls_last(),
            models.Structure.id,
        )

    query = query.order_by(models.Structure.id)
    return query


def retrieve_structure_query(
    params: parameters.RetrieveStructurePathParams,
) -> sqla.Select[tuple[models.Structure]]:
    return (
        sqla.select(models.Structure)
        .options(orm.selectinload(models.Structure.services))
        .options(orm.selectinload(models.Structure.doublons))
        .filter_by(id=params.id)
        .limit(1)
    )


def retrieve_structure(
    db_session: orm.Session,
    params: parameters.RetrieveStructurePathParams,
) -> models.Structure | None:
    return db_session.execute(
        retrieve_structure_query(params=params)
    ).scalar_one_or_none()


@functools.cache
def list_sources() -> list[dict]:
    return json.loads((Path(__file__).parent / "sources.json").read_text())


def filter_services(
    query: sqla.Select,
    params: parameters.ListServicesQueryParams
    | parameters.SearchServicesQueryParams
    | parameters.SearchQueryParams,
) -> sqla.Select:
    if params.sources is not None:
        query = query.filter(
            models.Service.source == sqla.any_(sqla.literal(params.sources))
        )

    if params.thematiques is not None:
        thematiques = [
            get_thematiques_by_group()[t.value]
            if isinstance(t, v1.Categorie)
            else t.value
            for t in params.thematiques
        ]
        query = query.filter(
            sqla.text(
                f"{models.Service.__tablename__}.thematiques && :thematiques"
            ).bindparams(thematiques=thematiques),
        )

    if params.frais is not None:
        query = query.filter(
            models.Service.frais == sqla.any_(sqla.literal(params.frais))
        )

    if params.publics is not None and v1.Public.TOUS_PUBLICS not in params.publics:
        # also match services for all publics
        publics = params.publics + [v1.Public.TOUS_PUBLICS]

        query = query.filter(
            sqla.exists(
                sqla.select(sqla.literal(1))
                .select_from(sqla.func.unnest(models.Service.publics).alias("item"))
                .where(
                    sqla.literal_column("item")
                    == sqla.any_(sqla.literal([p.value for p in publics]))
                )
            )
        )

    if params.modes_accueil is not None:
        query = query.filter(
            sqla.exists(
                sqla.select(sqla.literal(1))
                .select_from(
                    sqla.func.unnest(models.Service.modes_accueil).alias("item")
                )
                .where(
                    sqla.literal_column("item")
                    == sqla.any_(sqla.literal([f.value for f in params.modes_accueil]))
                )
            )
        )

    if params.types is not None:
        query = query.filter(
            models.Service.type == sqla.any_(sqla.literal(params.types))
        )

    if params.score_qualite_minimum is not None:
        query = query.filter(
            models.Service.score_qualite >= params.score_qualite_minimum
        )

    if (
        isinstance(
            params,
            (
                parameters.SearchServicesQueryParams,
                parameters.ListServicesQueryParams,
            ),
        )
        and params.recherche_public is not None
    ):
        websearch_to_tsquery = sqla.func.websearch_to_tsquery(
            "french", params.recherche_public
        )
        query = query.filter(
            sqla.or_(
                models.Service.searchable_index_publics.bool_op("@@")(
                    websearch_to_tsquery
                ),
                models.Service.searchable_index_publics_precisions.bool_op("@@")(
                    websearch_to_tsquery
                ),
            )
        )

    return query


def list_services_query(
    params: parameters.ListServicesQueryParams,
    include_soliguide: bool,
):
    query = (
        sqla.select(models.Service)
        .join(models.Structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if not include_soliguide:
        query = query.filter(models.Structure.source != "soliguide")

    if params.departement is not None:
        query = query.filter(
            models.Service.code_insee.startswith(params.departement.code)
        )

    if params.region is not None:
        query = query.join(models.Service.commune_).options(
            orm.contains_eager(models.Service.commune_)
        )
        query = query.filter(Commune.region == params.region.code)

    if params.code_commune is not None:
        query = query.filter(models.Service.code_insee == params.code_commune)

    query = filter_services(query=query, params=params)

    query = query.order_by(models.Service.id)

    return query


def search_services_query(
    params: parameters.SearchServicesQueryParams,
    include_soliguide: bool,
    include_remote_services: bool = True,
    commune_instance: Commune | None = None,
) -> tuple[sqla.Select[tuple[models.Service, int]], tuple[str, str]]:
    query = (
        sqla.select(models.Service)
        .join(models.Structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if not include_soliguide:
        query = query.filter(models.Structure.source != "soliguide")

    if commune_instance is not None:
        zone_eligibilite_codes = [
            commune_instance.code,
            commune_instance.departement,
            constants.PaysEnum.FRANCE.value.code,
            constants.PaysEnum.FRANCE.value.slug,
        ]
        if commune_instance.siren_epci is not None:
            zone_eligibilite_codes.append(commune_instance.siren_epci)

        query = query.filter(
            sqla.or_(
                models.Service.zone_eligibilite.is_(None),
                models.Service.zone_eligibilite.op("&&")(
                    sqla.literal(zone_eligibilite_codes)
                ),
            )
        )

        src_geometry = sqla.cast(
            geoalchemy2.functions.ST_MakePoint(
                models.Service.longitude, models.Service.latitude
            ),
            geoalchemy2.Geography(geometry_type="GEOMETRY", srid=4326),
        )

        if params.lon is not None and params.lat is not None:
            dest_geometry = f"POINT({params.lon} {params.lat})"
        else:
            dest_geometry = commune_instance.centre

        is_within_range = geoalchemy2.functions.ST_DWithin(
            src_geometry, dest_geometry, 50_000
        )
        is_available_on_site = models.Service.modes_accueil.contains(
            sqla.literal([v1.ModeAccueil.EN_PRESENTIEL.value])
        )
        is_available_remotely = models.Service.modes_accueil.contains(
            sqla.literal([v1.ModeAccueil.A_DISTANCE.value])
        )

        if include_remote_services:
            query = query.filter(sqla.or_(is_within_range, is_available_remotely))
        else:
            query = query.filter(is_within_range, is_available_on_site)

        distance_km = (
            geoalchemy2.functions.ST_Distance(src_geometry, dest_geometry) / 1000
        ).cast(sqla.Integer)
        query = query.add_columns(
            sqla.case(
                (sqla.and_(is_available_on_site, is_within_range), distance_km),
                else_=sqla.null().cast(sqla.Integer),
            ).label("distance")
        )

    else:
        query = query.add_columns(sqla.null().cast(sqla.Integer).label("distance"))

    query = filter_services(query=query, params=params)

    if params.exclure_doublons:
        cluster_key = sqla.func.coalesce(
            models.Structure._cluster_id,
            models.Structure.id,
        )
        structure_rank = (
            sqla.func.dense_rank()
            .over(
                partition_by=cluster_key,
                order_by=[
                    models.Structure.score_qualite.desc(),
                    models.Structure.date_maj.desc().nulls_last(),
                ],
            )
            .label("_structure_rank")
        )
        ranked_subq = query.add_columns(structure_rank).subquery()

        query = query.where(
            models.Service.id.in_(
                sqla.select(ranked_subq.c.id).where(ranked_subq.c._structure_rank == 1)
            )
        )

    query = query.order_by(
        sqla.column("distance").nulls_last(),
        models.Service.id,
    )

    return query, ("service", "distance")


def retrieve_service_query(
    params: parameters.RetrieveServicePathParams,
) -> sqla.Select[tuple[models.Service]]:
    return (
        sqla.select(models.Service)
        .join(models.Structure)
        .filter(models.Service.id == params.id)
        .limit(1)
    )


def retrieve_service(
    db_session: orm.Session,
    params: parameters.RetrieveServicePathParams,
) -> models.Service | None:
    return db_session.execute(
        retrieve_service_query(params=params)
    ).scalar_one_or_none()


def search_query(
    params: parameters.SearchQueryParams,
    include_soliguide: bool,
) -> tuple[sqla.Select[tuple[models.Service, int]], tuple[str, str]]:
    query = (
        sqla.select(models.Service)
        .join(models.Structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if not include_soliguide:
        query = query.filter(models.Structure.source != "soliguide")

    query = filter_services(query=query, params=params)

    if params.q is not None:
        plainto_tsquery = sqla.func.plainto_tsquery("french", params.q)
        score_recherche_expr = sqla.func.round(
            sqla.cast(
                sqla.func.ts_rank_cd(
                    models.Service.search_vector,
                    plainto_tsquery,
                    32,
                ),
                sqla.Numeric,
            ),
            2,
        ).label("score_recherche")

        query = query.filter(
            models.Service.search_vector.bool_op("@@")(plainto_tsquery)
        )
        query = query.add_columns(score_recherche_expr)
        query = query.order_by((sqla.func.round(score_recherche_expr * 10) / 2).desc())

    query = query.order_by(models.Service.score_qualite.desc())

    return query, ("data", "score_recherche")


def build_search_index(
    db_session: orm.Session,
) -> None:
    db_session.execute(
        sqla.text("""
            WITH thematiques AS (
            SELECT
                api__services_v1.id AS service_id,
                STRING_AGG(api__thematiques_v1.label, ', ') AS labels
            FROM api__services_v1,
                UNNEST(api__services_v1.thematiques) AS item
            INNER JOIN api__thematiques_v1
                ON api__thematiques_v1.value = item
            GROUP BY api__services_v1.id
        ),
        publics AS (
            SELECT
                api__services_v1.id AS service_id,
                STRING_AGG(api__publics_v1.label, ', ') AS labels
            FROM api__services_v1,
                UNNEST(api__services_v1.publics) AS item
            INNER JOIN api__publics_v1
                ON api__publics_v1.value = item
            GROUP BY api__services_v1.id
        ),
        types AS (
            SELECT
                api__services_v1.id AS service_id,
                api__types_services_v1.label AS label
            FROM api__services_v1
            LEFT JOIN api__types_services_v1
                ON api__types_services_v1.value = api__services_v1.type
        )
        UPDATE api__services_v1
        SET search_vector =
            SETWEIGHT(TO_TSVECTOR('french', COALESCE(api__structures_v1.nom,              '')), 'A') ||
            SETWEIGHT(TO_TSVECTOR('french', COALESCE(api__services_v1.nom,                '')), 'A') ||
            SETWEIGHT(TO_TSVECTOR('french', COALESCE(thematiques.labels,                  '')), 'B') ||
            SETWEIGHT(TO_TSVECTOR('french', COALESCE(publics.labels,                      '')), 'B') ||
            SETWEIGHT(TO_TSVECTOR('french', COALESCE(api__services_v1.publics_precisions, '')), 'B') ||
            SETWEIGHT(TO_TSVECTOR('french', COALESCE(api__services_v1.description,        '')), 'C') ||
            SETWEIGHT(TO_TSVECTOR('french', COALESCE(api__structures_v1.description,      '')), 'C')
        FROM api__structures_v1
        LEFT JOIN thematiques ON TRUE
        LEFT JOIN publics ON TRUE
        LEFT JOIN types ON TRUE
        WHERE api__services_v1.structure_id = api__structures_v1.id
            AND thematiques.service_id = api__services_v1.id
            AND publics.service_id = api__services_v1.id
            AND types.service_id = api__services_v1.id
    """)  # noqa: E501
    )
    db_session.commit()
