import functools
import json
from collections import defaultdict
from pathlib import Path

import geoalchemy2
import sqlalchemy as sqla
from sqlalchemy import func, or_, orm

from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data.v0 import models, parameters
from data_inclusion.schema import v0


@functools.cache
def get_thematiques_by_group() -> dict[str, list[str]]:
    thematiques = defaultdict(list)
    for thematique in v0.Thematique:
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
        orm.joinedload(models.Structure.doublons)
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

    if params.typologie is not None:
        query = query.filter_by(typologie=params.typologie.value)

    if params.region is not None:
        query = query.join(Commune).options(
            orm.contains_eager(models.Structure.commune_)
        )
        query = query.filter(Commune.region == params.region.code)

    if params.label_national is not None:
        query = query.filter(
            models.Structure.labels_nationaux.contains([params.label_national.value])
        )

    if params.thematiques is not None:
        query = query.filter(
            sqla.text(
                f"{models.Structure.__tablename__}.thematiques && :thematiques"
            ).bindparams(
                thematiques=get_sub_thematiques(thematiques=params.thematiques),
            )
        )

    if params.exclure_doublons:
        query = query.filter(
            sqla.or_(
                models.Structure._cluster_id.is_(None),
                models.Structure._is_best_duplicate.is_(True),
            )
        )

    query = query.order_by(models.Structure._di_surrogate_id)

    return query


def retrieve_structure_query(
    params: parameters.RetrieveStructurePathParams,
) -> sqla.Select[tuple[models.Structure]]:
    return (
        sqla.select(models.Structure)
        .options(orm.selectinload(models.Structure.services))
        .options(orm.selectinload(models.Structure.doublons))
        .filter_by(source=params.source)
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


def get_sub_thematiques(thematiques: list[v0.Thematique]) -> list[str]:
    all_thematiques = set()
    for t in thematiques:
        all_thematiques.add(t.value)
        group = get_thematiques_by_group()[t.value]
        if group:
            all_thematiques.update(group)
    return list(all_thematiques)


def _filter_array_field(query, model, field_name, values):
    filter_stmt = f"""\
    EXISTS(
        SELECT
        FROM unnest({model.__tablename__}.{field_name}) {field_name}
        WHERE {field_name} = ANY(:{field_name})
    )
    """
    query = query.filter(
        sqla.text(filter_stmt).bindparams(**{field_name: [f.value for f in values]})
    )
    return query


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
        query = _filter_array_field(query, models.Service, "frais", params.frais)

    if params.profils is not None:
        query = _filter_array_field(query, models.Service, "profils", params.profils)

    if params.modes_accueil is not None:
        query = _filter_array_field(
            query,
            models.Service,
            "modes_accueil",
            params.modes_accueil,
        )

    if params.types is not None:
        query = _filter_array_field(query, models.Service, "types", params.types)

    if params.score_qualite_minimum is not None:
        query = query.filter(
            models.Service.score_qualite >= params.score_qualite_minimum
        )

    if params.recherche_public is not None:
        profils_only = params.recherche_public.split(" ")
        profils_only = [p.strip() for p in profils_only]
        query = query.filter(
            or_(
                models.Service.searchable_index_profils.bool_op("@@")(
                    func.to_tsquery("french_di", " | ".join(profils_only))
                ),
                models.Service.searchable_index_profils_precisions.bool_op("@@")(
                    func.websearch_to_tsquery("french_di", params.recherche_public)
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
        query = query.join(Commune).options(orm.contains_eager(models.Service.commune_))
        query = query.filter(Commune.region == params.region.code)

    if params.code_commune is not None:
        query = query.filter(models.Service.code_insee == params.code_commune)

    query = filter_services(query=query, params=params)

    query = query.order_by(models.Service._di_surrogate_id)

    return query


def search_services_query(
    params: parameters.SearchServicesQueryParams,
    include_soliguide: bool,
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
        query = query.filter(
            sqla.or_(
                models.Service.zone_diffusion_type.is_(None),
                models.Service.zone_diffusion_type == v0.ZoneDiffusionType.PAYS.value,
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == v0.ZoneDiffusionType.COMMUNE.value,
                    models.Service.zone_diffusion_code == commune_instance.code,
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == v0.ZoneDiffusionType.EPCI.value,
                    sqla.literal(commune_instance.siren_epci).contains(
                        models.Service.zone_diffusion_code
                    ),
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == v0.ZoneDiffusionType.DEPARTEMENT.value,
                    models.Service.zone_diffusion_code == commune_instance.departement,
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == v0.ZoneDiffusionType.REGION.value,
                    models.Service.zone_diffusion_code == commune_instance.region,
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
                    sqla.literal([v0.ModeAccueil.A_DISTANCE.value])
                ),
            )
        )

        # annotate distance
        query = query.add_columns(
            (
                sqla.case(
                    (
                        models.Service.modes_accueil.contains(
                            sqla.literal([v0.ModeAccueil.EN_PRESENTIEL.value])
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
        models.Service._di_surrogate_id,
    )

    return query, ("service", "distance")


def retrieve_service_query(
    params: parameters.RetrieveServicePathParams,
) -> sqla.Select[tuple[models.Service]]:
    return (
        sqla.select(models.Service)
        .join(models.Structure)
        .filter(models.Service.source == params.source)
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
