import functools
import json
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path

import geoalchemy2
import sqlalchemy as sqla
from sqlalchemy import orm

import fastapi

# TODO(vmttn): handle pagination outside ?
from fastapi_pagination.ext.sqlalchemy import paginate

from data_inclusion import schema as di_schema
from data_inclusion.api.inclusion_data import models, schemas
from data_inclusion.api.utils import code_officiel_geographique

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


def list_structures(
    request: fastapi.Request,
    db_session: orm.Session,
    source: str | None = None,
    id_: str | None = None,
    typologie: di_schema.Typologie | None = None,
    label_national: di_schema.LabelNational | None = None,
    departement: schemas.DepartementCOG | None = None,
    departement_slug: schemas.DepartementSlug | None = None,
    code_postal: di_schema.CodePostal | None = None,
    thematique: di_schema.Thematique | None = None,
) -> list:
    query = sqla.select(models.Structure)

    if source is not None:
        query = query.filter_by(source=source)

    if not request.user.is_authenticated or "dora" not in request.user.username:
        query = query.filter(models.Structure.source != "soliguide")
        query = query.filter(models.Structure.source != "data-inclusion")

    if id_ is not None:
        query = query.filter_by(id=id_)

    if departement is not None:
        query = query.filter(
            sqla.or_(
                models.Structure.code_insee.startswith(departement.value),
                models.Structure._di_geocodage_code_insee.startswith(departement.value),
            )
        )

    if departement_slug is not None:
        query = query.filter(
            sqla.or_(
                models.Structure.code_insee.startswith(
                    schemas.DepartementCOG[departement_slug.name].value
                ),
                models.Structure._di_geocodage_code_insee.startswith(
                    schemas.DepartementCOG[departement_slug.name].value
                ),
            )
        )

    if code_postal is not None:
        query = query.filter_by(code_postal=code_postal)

    if typologie is not None:
        query = query.filter_by(typologie=typologie.value)

    if label_national is not None:
        query = query.filter(
            models.Structure.labels_nationaux.contains([label_national.value])
        )

    if thematique is not None:
        filter_stmt = """\
        EXISTS(
            SELECT
            FROM unnest(thematiques) thematique
            WHERE thematique ~ ('^' || :thematique)
        )
        """
        query = query.filter(
            sqla.text(filter_stmt).bindparams(thematique=thematique.value)
        )

    query = query.order_by(
        models.Structure.source,
        models.Structure.id,
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
def read_sources():
    return json.loads((Path(__file__).parent / "sources.json").read_text())


def list_sources(request: fastapi.Request) -> list[dict]:
    sources = read_sources()
    if not request.user.is_authenticated or "dora" not in request.user.username:
        sources = [
            d for d in sources if d["slug"] not in ["data-inclusion", "soliguide"]
        ]
    return sources


def list_services(
    request: fastapi.Request,
    db_session: orm.Session,
    source: str | None = None,
    thematique: di_schema.Thematique | None = None,
    departement: schemas.DepartementCOG | None = None,
    departement_slug: schemas.DepartementSlug | None = None,
    code_insee: di_schema.CodeCommune | None = None,
):
    query = (
        sqla.select(models.Service)
        .join(models.Service.structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if source is not None:
        query = query.filter(models.Structure.source == source)

    if not request.user.is_authenticated or "dora" not in request.user.username:
        query = query.filter(models.Structure.source != "soliguide")
        query = query.filter(models.Structure.source != "data-inclusion")

    if departement is not None:
        query = query.filter(
            sqla.or_(
                models.Service.code_insee.startswith(departement.value),
                models.Service._di_geocodage_code_insee.startswith(departement.value),
            )
        )

    if departement_slug is not None:
        query = query.filter(
            sqla.or_(
                models.Service.code_insee.startswith(
                    schemas.DepartementCOG[departement_slug.name].value
                ),
                models.Service._di_geocodage_code_insee.startswith(
                    schemas.DepartementCOG[departement_slug.name].value
                ),
            )
        )

    if code_insee is not None:
        code_insee = code_officiel_geographique.CODE_COMMUNE_BY_CODE_ARRONDISSEMENT.get(
            code_insee, code_insee
        )

        query = query.filter(
            sqla.or_(
                models.Service.code_insee == code_insee,
                models.Service._di_geocodage_code_insee == code_insee,
            )
        )

    if thematique is not None:
        filter_stmt = """\
        EXISTS(
            SELECT
            FROM unnest(service.thematiques) thematique
            WHERE thematique ~ ('^' || :thematique)
        )
        """
        query = query.filter(
            sqla.text(filter_stmt).bindparams(thematique=thematique.value)
        )

    query = query.order_by(
        models.Service.source,
        models.Service.id,
    )

    return paginate(db_session, query, unique=False)


def search_services(
    request: fastapi.Request,
    db_session: orm.Session,
    sources: list[str] | None = None,
    commune_instance: models.Commune | None = None,
    thematiques: list[di_schema.Thematique] | None = None,
    frais: list[di_schema.Frais] | None = None,
    types: list[di_schema.TypologieService] | None = None,
    search_point: str | None = None,
    include_outdated: bool | None = False,
):
    query = (
        sqla.select(models.Service)
        .join(models.Service.structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if sources is not None:
        query = query.filter(models.Service.source == sqla.any_(sqla.literal(sources)))

    if not request.user.is_authenticated or "dora" not in request.user.username:
        query = query.filter(models.Structure.source != "soliguide")
        query = query.filter(models.Structure.source != "data-inclusion")

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
            dest_geometry = (
                sqla.select(
                    sqla.cast(
                        geoalchemy2.functions.ST_Simplify(models.Commune.geom, 0.01),
                        geoalchemy2.Geography(geometry_type="GEOMETRY", srid=4326),
                    )
                )
                .filter(models.Commune.code == commune_instance.code)
                .scalar_subquery()
            )

        query = query.filter(
            sqla.or_(
                # either `en-presentiel` within a given distance
                geoalchemy2.functions.ST_DWithin(
                    src_geometry,
                    dest_geometry,
                    50_000,  # meters or 50km
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
                            / 1000
                        ).cast(sqla.Integer),  # conversion to kms
                    ),
                    else_=sqla.null().cast(sqla.Integer),
                )
            ).label("distance")
        )

    else:
        query = query.add_columns(sqla.null().cast(sqla.Integer).label("distance"))

    if thematiques is not None:
        query = query.filter(
            sqla.text("service.thematiques && :thematiques").bindparams(
                thematiques=get_sub_thematiques(thematiques),
            )
        )

    if frais is not None:
        filter_stmt = """\
        EXISTS(
            SELECT
            FROM unnest(service.frais) frais
            WHERE frais = ANY(:frais)
        )
        """
        query = query.filter(
            sqla.text(filter_stmt).bindparams(frais=[f.value for f in frais])
        )

    if types is not None:
        filter_stmt = """\
        EXISTS(
            SELECT
            FROM unnest(service.types) types
            WHERE types = ANY(:types)
        )
        """
        query = query.filter(
            sqla.text(filter_stmt).bindparams(types=[t.value for t in types])
        )

    if not include_outdated:
        query = query.filter(
            sqla.or_(
                models.Service.date_suspension.is_(None),
                models.Service.date_suspension >= date.today(),
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
