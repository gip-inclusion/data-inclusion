from typing import Literal

import ua_parser
from sqlalchemy import orm

import fastapi

from data_inclusion.api.analytics.models import (
    ConsultServiceEvent,
    ConsultStructureEvent,
    ListServicesEvent,
    ListStructuresEvent,
    SearchServicesEvent,
)
from data_inclusion.api.decoupage_administratif.constants import (
    Departement,
    Region,
)
from data_inclusion.api.inclusion_data.v0 import schemas
from data_inclusion.api.utils import pagination
from data_inclusion.schema import v0 as di_schema


def get_schema_version(request: fastapi.Request) -> Literal["v0", "v1"]:
    return "v0" if "v0" in str(request.url) else "v1"


def is_bot(user_agent):
    if not user_agent:
        return False  # probably most of our consumers :/
    match ua_parser.parse(user_agent):
        case ua_parser.Result(device=ua_parser.Device(family="Spider")):
            return True
        case _:
            return False


def save_consult_structure_event(
    request: fastapi.Request,
    structure: schemas.DetailedStructure,
    db_session: orm.Session,
):
    user = request.scope.get("user")
    if user is None or not user.is_authenticated:
        return

    user_agent = request.headers.get("User-Agent")
    if is_bot(user_agent):
        return

    event = ConsultStructureEvent(
        structure_id=structure.id,
        source=structure.source,
        user=user.username,
        schema_version=get_schema_version(request),
    )
    db_session.add(event)
    db_session.commit()


def save_consult_service_event(
    request: fastapi.Request,
    service: schemas.DetailedService,
    db_session: orm.Session,
):
    user = request.scope.get("user")
    if user is None or not user.is_authenticated:
        return

    user_agent = request.headers.get("User-Agent")
    if is_bot(user_agent):
        return

    event = ConsultServiceEvent(
        service_id=service.id,
        source=service.source,
        user=user.username,
        score_qualite=service.score_qualite,
        schema_version=get_schema_version(request),
    )
    db_session.add(event)
    db_session.commit()


def save_list_services_event(
    request: fastapi.Request,
    db_session: orm.Session,
    sources: list[str] | None = None,
    thematiques: list[di_schema.Thematique] | None = None,
    departement: Departement | None = None,
    region: Region | None = None,
    code_commune: di_schema.CodeCommune | None = None,
    frais: list[di_schema.Frais] | None = None,
    profils: list[di_schema.Profil] | None = None,
    modes_accueil: list[di_schema.ModeAccueil] | None = None,
    types: list[di_schema.TypologieService] | None = None,
    recherche_public: str | None = None,
    score_qualite_minimum: float | None = None,
):
    user = request.scope.get("user")
    if user is None or not user.is_authenticated:
        return

    event = ListServicesEvent(
        user=user.username,
        sources=sources,
        thematiques=thematiques,
        code_departement=departement.code if departement else None,
        code_region=region.code if region else None,
        code_commune=code_commune,
        frais=frais,
        profils=profils,
        modes_accueil=modes_accueil,
        types=types,
        recherche_public=recherche_public,
        score_qualite_minimum=score_qualite_minimum,
        schema_version=get_schema_version(request),
    )
    db_session.add(event)
    db_session.commit()


def save_list_structures_event(
    request: fastapi.Request,
    db_session: orm.Session,
    sources: list[str] | None = None,
    typologie: di_schema.TypologieStructure | None = None,
    label_national: di_schema.LabelNational | None = None,
    departement: Departement | None = None,
    region: Region | None = None,
    code_commune: di_schema.CodeCommune | None = None,
    thematiques: list[di_schema.Thematique] | None = None,
    exclure_doublons: bool | None = False,
):
    user = request.scope.get("user")
    if user is None or not user.is_authenticated:
        return

    event = ListStructuresEvent(
        user=user.username,
        sources=sources,
        typologie=typologie,
        label_national=label_national,
        code_departement=departement.code if departement else None,
        code_region=region.code if region else None,
        code_commune=code_commune,
        thematiques=thematiques,
        exclure_doublons=exclure_doublons,
        schema_version=get_schema_version(request),
    )
    db_session.add(event)
    db_session.commit()


def save_search_services_event(
    request: fastapi.Request,
    db_session: orm.Session,
    results: pagination.BigPage[schemas.ServiceSearchResult],
    sources: list[str] | None = None,
    code_commune: str | None = None,
    lat: float | None = None,
    lon: float | None = None,
    thematiques: list[di_schema.Thematique] | None = None,
    frais: list[di_schema.Frais] | None = None,
    modes_accueil: list[di_schema.ModeAccueil] | None = None,
    profils: list[di_schema.Profil] | None = None,
    types: list[di_schema.TypologieService] | None = None,
    recherche_public: str | None = None,
    score_qualite_minimum: float | None = None,
    exclure_doublons: bool | None = False,
):
    user = request.scope.get("user")
    if user is None or not user.is_authenticated:
        return

    if results.page is not None and results.page > 1:
        return

    event = SearchServicesEvent(
        user=user.username,
        first_services=[
            {
                "id": result.service.id,
                "score_qualite": result.service.score_qualite,
                "distance": result.distance,
            }
            for result in results.items[:10]
        ],
        total_services=results.total,
        sources=sources,
        code_commune=code_commune,
        lat=lat,
        lon=lon,
        thematiques=thematiques,
        frais=frais,
        modes_accueil=modes_accueil,
        profils=profils,
        types=types,
        recherche_public=recherche_public,
        score_qualite_minimum=score_qualite_minimum,
        exclure_doublons=exclure_doublons,
        schema_version=get_schema_version(request),
    )
    db_session.add(event)
    db_session.commit()
