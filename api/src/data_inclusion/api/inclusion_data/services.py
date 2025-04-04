import functools
import json
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path

import geoalchemy2
import sqlalchemy as sqla
from sqlalchemy import func, or_, orm

import fastapi

# TODO(vmttn): handle pagination outside ?
from fastapi_pagination.ext.sqlalchemy import paginate

from data_inclusion import schema as di_schema
from data_inclusion.api.decoupage_administratif.constants import (
    Departement,
    Region,
)
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data import models

logger = logging.getLogger(__name__)


@functools.cache
def get_thematiques_by_group():
    thematiques = defaultdict(list)
    for thematique in di_schema.Thematique:
        try:
            theme, _ = str(thematique.value).split("--")
        except ValueError:
            continue
        thematiques[theme].append(thematique.value)
    return thematiques


def get_sub_thematiques(thematiques: list[di_schema.Thematique]) -> list[str]:
    """
    get_sub_thematiques(Thematique.MOBILITE) -> [
        "mobilite",
        "mobilite--accompagnement-a-la-mobilite"
        ...
    ]

    get_sub_thematiques(Thematique.MOBILITE_ACCOMPAGNEMENT_A_LA_MOBILITE") -> [
        "mobilite--accompagnement-a-la-mobilite"
    ]
    """
    all_thematiques = set()
    for t in thematiques:
        all_thematiques.add(t.value)
        group = get_thematiques_by_group().get(t.value)
        if group:
            all_thematiques.update(group)
    return list(all_thematiques)


def filter_services_by_thematiques(
    query: sqla.Select,
    thematiques: list[di_schema.Thematique],
):
    return query.filter(
        sqla.text("api__services.thematiques && :thematiques").bindparams(
            thematiques=get_sub_thematiques(thematiques),
        )
    )


def filter_structures_by_thematiques(
    query: sqla.Select,
    thematiques: list[di_schema.Thematique],
):
    return query.filter(
        sqla.text("api__structures.thematiques && :thematiques").bindparams(
            thematiques=get_sub_thematiques(thematiques),
        )
    )


def filter_services_by_frais(
    query: sqla.Select,
    frais: list[di_schema.Frais],
):
    filter_stmt = """\
    EXISTS(
        SELECT
        FROM unnest(api__services.frais) frais
        WHERE frais = ANY(:frais)
    )
    """
    return query.filter(
        sqla.text(filter_stmt).bindparams(frais=[f.value for f in frais])
    )


def filter_services_by_modes_accueil(
    query: sqla.Select,
    modes_accueil: list[di_schema.ModeAccueil],
):
    filter_stmt = """\
    EXISTS(
        SELECT
        FROM unnest(api__services.modes_accueil) modes_accueil
        WHERE modes_accueil = ANY(:modes_accueil)
    )
    """
    return query.filter(
        sqla.text(filter_stmt).bindparams(
            modes_accueil=[m.value for m in modes_accueil]
        )
    )


def filter_services_by_profils(
    query: sqla.Select,
    profils: list[di_schema.Profil],
):
    filter_stmt = """\
    EXISTS(
        SELECT
        FROM unnest(api__services.profils) profils
        WHERE profils = ANY(:profils)
    )
    """
    return query.filter(
        sqla.text(filter_stmt).bindparams(profils=[p.value for p in profils])
    )


def filter_services_by_profils_search(
    query: sqla.Select,
    profils_search: str,
):
    profils_only = profils_search.split(" ")
    profils_only = [p.strip() for p in profils_only]
    return query.filter(
        or_(
            models.Service.searchable_index_profils.bool_op("@@")(
                func.to_tsquery("french_di", " | ".join(profils_only))
            ),
            models.Service.searchable_index_profils_precisions.bool_op("@@")(
                func.websearch_to_tsquery("french_di", profils_search)
            ),
        )
    )


def filter_services_by_types(
    query: sqla.Select,
    types: list[di_schema.TypologieService],
):
    filter_stmt = """\
    EXISTS(
        SELECT
        FROM unnest(api__services.types) types
        WHERE types = ANY(:types)
    )
    """
    return query.filter(
        sqla.text(filter_stmt).bindparams(types=[t.value for t in types])
    )


def filter_services_by_score_qualite(
    query: sqla.Select,
    score_qualite_minimum: float,
):
    return query.filter(models.Service.score_qualite >= score_qualite_minimum)


def filter_outdated_services(
    query: sqla.Select,
):
    return query.filter(
        sqla.or_(
            models.Service.date_suspension.is_(None),
            models.Service.date_suspension >= date.today(),
        )
    )


def filter_restricted(
    query: sqla.Select,
    request: fastapi.Request,
) -> sqla.Select:
    if not request.user.is_authenticated or not request.user.username.startswith(
        "dora-"
    ):
        query = query.filter(
            sqla.or_(
                models.Structure.source != "soliguide",
                models.Structure.code_insee.startswith("59"),  # Nord
                models.Structure.code_insee.startswith("67"),  # Bas-Rhin
            )
        )

    return query


def list_structures(
    request: fastapi.Request,
    db_session: orm.Session,
    sources: list[str] | None = None,
    id_: str | None = None,
    typologie: di_schema.TypologieStructure | None = None,
    label_national: di_schema.LabelNational | None = None,
    departement: Departement | None = None,
    region: Region | None = None,
    commune_code: di_schema.CodeCommune | None = None,
    thematiques: list[di_schema.Thematique] | None = None,
    deduplicate: bool = False,
) -> list:
    query = sqla.select(models.Structure)
    query = filter_restricted(query, request)

    if sources is not None:
        query = query.filter(
            models.Structure.source == sqla.any_(sqla.literal(sources))
        )

    if id_ is not None:
        query = query.filter_by(id=id_)

    if commune_code is not None:
        query = query.filter(models.Structure.code_insee == commune_code)

    if departement is not None:
        query = query.filter(models.Structure.code_insee.startswith(departement.code))

    if typologie is not None:
        query = query.filter_by(typologie=typologie.value)

    if region is not None:
        query = query.join(Commune).options(
            orm.contains_eager(models.Structure.commune_)
        )
        query = query.filter(Commune.region == region.code)

    if label_national is not None:
        query = query.filter(
            models.Structure.labels_nationaux.contains([label_national.value])
        )

    if thematiques is not None:
        query = filter_structures_by_thematiques(query, thematiques)

    if deduplicate:
        query = query.filter(
            sqla.or_(
                models.Structure.cluster_best_duplicate.is_(None),
                models.Structure._di_surrogate_id
                == models.Structure.cluster_best_duplicate,
            )
        )

    return paginate(db_session, query)


def retrieve_structure(
    db_session: orm.Session,
    source: str,
    id_: str,
) -> models.Structure:
    structure_instance = db_session.scalars(
        sqla.select(models.Structure)
        .options(orm.selectinload(models.Structure.services))
        .filter_by(source=source)
        .filter_by(id=id_)
    ).first()

    if structure_instance is None:
        raise fastapi.HTTPException(status_code=404)

    return structure_instance


@functools.cache
def list_sources(request: fastapi.Request) -> list[dict]:
    return json.loads((Path(__file__).parent / "sources.json").read_text())


def filter_services(
    query: sqla.Select,
    sources: list[str] | None = None,
    thematiques: list[di_schema.Thematique] | None = None,
    frais: list[di_schema.Frais] | None = None,
    profils: list[di_schema.Profil] | None = None,
    profils_search: str | None = None,
    modes_accueil: list[di_schema.ModeAccueil] | None = None,
    types: list[di_schema.TypologieService] | None = None,
    score_qualite_minimum: float | None = None,
    include_outdated: bool | None = False,
) -> sqla.Select:
    """Common filters for services."""

    if sources is not None:
        query = query.filter(models.Service.source == sqla.any_(sqla.literal(sources)))

    if thematiques is not None:
        query = filter_services_by_thematiques(query, thematiques)

    if frais is not None:
        query = filter_services_by_frais(query, frais)

    if profils is not None:
        query = filter_services_by_profils(query, profils)

    if modes_accueil is not None:
        query = filter_services_by_modes_accueil(query, modes_accueil)

    if types is not None:
        query = filter_services_by_types(query, types)

    if score_qualite_minimum is not None:
        query = filter_services_by_score_qualite(query, score_qualite_minimum)

    if not include_outdated:
        query = filter_outdated_services(query)

    if profils_search is not None:
        query = filter_services_by_profils_search(query, profils_search)

    return query


def list_services(
    request: fastapi.Request,
    db_session: orm.Session,
    sources: list[str] | None = None,
    thematiques: list[di_schema.Thematique] | None = None,
    departement: Departement | None = None,
    region: Region | None = None,
    code_commune: di_schema.CodeCommune | None = None,
    frais: list[di_schema.Frais] | None = None,
    profils: list[di_schema.Profil] | None = None,
    recherche_public: str | None = None,
    modes_accueil: list[di_schema.ModeAccueil] | None = None,
    types: list[di_schema.TypologieService] | None = None,
    score_qualite_minimum: float | None = None,
    include_outdated: bool | None = False,
):
    query = (
        sqla.select(models.Service)
        .join(models.Structure)
        .options(orm.contains_eager(models.Service.structure))
    )
    query = filter_restricted(query, request)

    if departement is not None:
        query = query.filter(models.Service.code_insee.startswith(departement.code))

    if region is not None:
        query = query.join(Commune).options(orm.contains_eager(models.Service.commune_))
        query = query.filter(Commune.region == region.code)

    if code_commune is not None:
        query = query.filter(models.Service.code_insee == code_commune)

    query = filter_services(
        query=query,
        sources=sources,
        thematiques=thematiques,
        frais=frais,
        profils=profils,
        profils_search=recherche_public,
        modes_accueil=modes_accueil,
        types=types,
        score_qualite_minimum=score_qualite_minimum,
        include_outdated=include_outdated,
    )

    return paginate(db_session, query, unique=False)


def search_services(
    request: fastapi.Request,
    db_session: orm.Session,
    sources: list[str] | None = None,
    commune_instance: Commune | None = None,
    thematiques: list[di_schema.Thematique] | None = None,
    frais: list[di_schema.Frais] | None = None,
    modes_accueil: list[di_schema.ModeAccueil] | None = None,
    profils: list[di_schema.Profil] | None = None,
    profils_search: str | None = None,
    types: list[di_schema.TypologieService] | None = None,
    search_point: str | None = None,
    score_qualite_minimum: float | None = None,
    include_outdated: bool | None = False,
    deduplicate: bool | None = False,
):
    query = (
        sqla.select(models.Service)
        .join(models.Structure)
        .options(orm.contains_eager(models.Service.structure))
    )
    query = filter_restricted(query, request)

    if commune_instance is not None:
        # filter by zone de diffusion
        query = query.filter(
            sqla.or_(
                models.Service.zone_diffusion_type.is_(None),
                models.Service.zone_diffusion_type
                == di_schema.ZoneDiffusionType.PAYS.value,
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == di_schema.ZoneDiffusionType.COMMUNE.value,
                    models.Service.zone_diffusion_code == commune_instance.code,
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == di_schema.ZoneDiffusionType.EPCI.value,
                    sqla.literal(commune_instance.siren_epci).contains(
                        models.Service.zone_diffusion_code
                    ),
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == di_schema.ZoneDiffusionType.DEPARTEMENT.value,
                    models.Service.zone_diffusion_code == commune_instance.departement,
                ),
                sqla.and_(
                    models.Service.zone_diffusion_type
                    == di_schema.ZoneDiffusionType.REGION.value,
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

        if search_point is not None:
            dest_geometry = search_point
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
                    sqla.literal([di_schema.ModeAccueil.A_DISTANCE.value])
                ),
            )
        )

        # annotate distance
        query = query.add_columns(
            (
                sqla.case(
                    (
                        models.Service.modes_accueil.contains(
                            sqla.literal([di_schema.ModeAccueil.EN_PRESENTIEL.value])
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

    query = filter_services(
        query=query,
        sources=sources,
        thematiques=thematiques,
        frais=frais,
        profils=profils,
        profils_search=profils_search,
        modes_accueil=modes_accueil,
        types=types,
        score_qualite_minimum=score_qualite_minimum,
        include_outdated=include_outdated,
    )

    if deduplicate:
        query = query.filter(
            sqla.or_(
                models.Structure.cluster_best_duplicate.is_(None),
                models.Service._di_structure_surrogate_id
                == models.Structure.cluster_best_duplicate,
            )
        )

    query = query.order_by(sqla.column("distance").nulls_last())

    def _items_to_mappings(items: list) -> list[dict]:
        # convert rows returned by `Session.execute` to a list of dicts that will be
        # used to instanciate pydantic models
        return [{"service": item[0], "distance": item[1]} for item in items]

    return paginate(db_session, query, unique=False, transformer=_items_to_mappings)


def retrieve_service(
    db_session: orm.Session,
    source: str,
    id_: str,
) -> models.Service:
    service_instance = db_session.scalars(
        sqla.select(models.Service)
        .options(orm.selectinload(models.Service.structure))
        .filter_by(source=source)
        .filter_by(id=id_)
    ).first()

    if service_instance is None:
        raise fastapi.HTTPException(status_code=404)

    return service_instance
