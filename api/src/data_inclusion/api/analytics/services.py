from sqlalchemy import orm

import fastapi

from data_inclusion import schema as di_schema
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
from data_inclusion.api.inclusion_data import schemas
from data_inclusion.api.utils import pagination


def save_consult_structure_event(
    request: fastapi.Request,
    structure: schemas.DetailedStructure,
    db_session: orm.Session,
):
    user = request.scope.get("user")
    if user is None or not user.is_authenticated:
        return

    event = ConsultStructureEvent(
        structure_id=structure.id, source=structure.source, user=user.username
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

    event = ConsultServiceEvent(
        service_id=service.id,
        source=service.source,
        user=user.username,
        score_qualite=service.score_qualite,
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
    inclure_suspendus: bool | None = False,
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
        inclure_suspendus=inclure_suspendus,
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
    inclure_suspendus: bool | None = False,
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
        inclure_suspendus=inclure_suspendus,
    )
    db_session.add(event)
    db_session.commit()
