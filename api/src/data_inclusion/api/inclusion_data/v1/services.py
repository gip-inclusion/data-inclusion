import functools
import json
from collections import defaultdict
from pathlib import Path

import geoalchemy2
import sqlalchemy as sqla
from sqlalchemy import orm

from data_inclusion.api.decoupage_administratif import constants
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data.v1 import models, parameters
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


def filter_soliguide[T: tuple[models.Structure] | tuple[models.Service]](
    query: sqla.Select[T],
) -> sqla.Select[T]:
    return query.filter(
        sqla.or_(
            models.Structure.source != "soliguide",
            models.Structure.code_insee.startswith("59"),  # Nord
            models.Structure.code_insee.startswith("67"),  # Bas-Rhin
        )
    )


def list_structures_query(
    params: parameters.ListStructuresQueryParams,
    include_all_soliguide: bool,
) -> sqla.Select[tuple[models.Structure]]:
    query = sqla.select(models.Structure).options(
        orm.joinedload(models.Structure.doublons),
    )

    if not include_all_soliguide:
        query = filter_soliguide(query)

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
        query = query.filter(
            sqla.or_(
                models.Structure._cluster_id.is_(None),
                models.Structure._is_best_duplicate.is_(True),
            )
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
    return json.loads((Path(__file__).parent.parent / "sources.json").read_text())


def get_sub_thematiques(thematiques: list[v1.Thematique]) -> list[str]:
    all_thematiques = set()
    for t in thematiques:
        all_thematiques.add(t.value)
        group = get_thematiques_by_group()[t.value]
        if group:
            all_thematiques.update(group)
    return list(all_thematiques)


def filter_services(
    query: sqla.Select,
    params: parameters.ListServicesQueryParams | parameters.SearchServicesQueryParams,
) -> sqla.Select:
    if params.sources is not None:
        query = query.filter(
            models.Service.source == sqla.any_(sqla.literal(params.sources))
        )

    if params.thematiques is not None:
        query = query.filter(
            sqla.text(
                f"{models.Service.__tablename__}.thematiques && :thematiques"
            ).bindparams(
                thematiques=get_sub_thematiques(thematiques=params.thematiques),
            )
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

    if params.recherche_public is not None:
        publics_only = params.recherche_public.split(" ")
        publics_only = [p.strip() for p in publics_only]
        query = query.filter(
            sqla.or_(
                models.Service.searchable_index_publics.bool_op("@@")(
                    sqla.func.to_tsquery("french_di", " | ".join(publics_only))
                ),
                models.Service.searchable_index_publics_precisions.bool_op("@@")(
                    sqla.func.websearch_to_tsquery("french_di", params.recherche_public)
                ),
            )
        )

    return query


def list_services_query(
    params: parameters.ListServicesQueryParams,
    include_all_soliguide: bool,
):
    query = (
        sqla.select(models.Service)
        .join(models.Structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if not include_all_soliguide:
        query = filter_soliguide(query)

    if params.departement is not None:
        query = query.filter(
            models.Service.code_insee.startswith(params.departement.code)
        )

    if params.region is not None:
        query = query.join(Commune).options(orm.contains_eager(models.Service.commune_))
        query = query.filter(Commune.region == params.region.code)

    if params.code_commune is not None:
        query = query.filter(models.Service.code_insee == params.code_commune)

    query = filter_services(query=query, params=params)

    query = query.order_by(models.Service.id)

    return query


def search_services_query(
    params: parameters.SearchServicesQueryParams,
    include_all_soliguide: bool,
    commune_instance: Commune | None = None,
) -> tuple[sqla.Select[tuple[models.Service, int]], tuple[str, str]]:
    query = (
        sqla.select(models.Service)
        .join(models.Structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if not include_all_soliguide:
        query = filter_soliguide(query)

    if commune_instance is not None:
        query = query.filter(
            sqla.or_(
                models.Service.zone_eligibilite.is_(None),
                models.Service.zone_eligibilite.op("&&")(
                    sqla.literal(
                        [
                            commune_instance.code,
                            commune_instance.departement,
                            commune_instance.siren_epci,
                            constants.PaysEnum.FRANCE.value.code,
                            constants.PaysEnum.FRANCE.value.slug,
                        ]
                    )
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

        query = query.filter(
            sqla.or_(
                # either `en-presentiel` within a given distance
                geoalchemy2.functions.ST_DWithin(
                    src_geometry,
                    dest_geometry,
                    50_000,  # meters
                ),
                # or `a-distance`
                models.Service.modes_accueil.contains(
                    sqla.literal([v1.ModeAccueil.A_DISTANCE.value])
                ),
            )
        )

        # annotate distance
        query = query.add_columns(
            (
                sqla.case(
                    (
                        models.Service.modes_accueil.contains(
                            sqla.literal([v1.ModeAccueil.EN_PRESENTIEL.value])
                        ),
                        (
                            geoalchemy2.functions.ST_Distance(
                                src_geometry,
                                dest_geometry,
                            )
                            / 1000  # conversion to kms
                        ).cast(sqla.Integer),
                    ),
                    else_=sqla.null().cast(sqla.Integer),
                )
            ).label("distance")
        )

    else:
        query = query.add_columns(sqla.null().cast(sqla.Integer).label("distance"))

    query = filter_services(query=query, params=params)

    if params.exclure_doublons:
        query = query.filter(
            sqla.or_(
                models.Structure._cluster_id.is_(None),
                models.Structure._is_best_duplicate.is_(True),
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
