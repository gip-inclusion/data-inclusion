from sqlalchemy import orm

import fastapi

from data_inclusion import schema as di_schema
from data_inclusion.api.analytics.models import (
    ConsultServiceEvent,
    ConsultStructureEvent,
    ListServicesEvent,
    ListStructuresEvent,
    ServicesSearch,
)
from data_inclusion.api.decoupage_administratif.constants import (
    Departement,
    Region,
)
from data_inclusion.api.inclusion_data import models


def save_analytics_consult_structure_event(
    request: fastapi.Request,
    structure_id: str,
    source: str,
    db_session: orm.Session,
):
    user = request.scope.get("user")
    if user is None or not user.is_authenticated:
        return

    analytics = ConsultStructureEvent(
        structure_id=structure_id, source=source, user=user.username
    )
    db_session.add(analytics)
    db_session.commit()


def save_analytics_consult_service_event(
    request: fastapi.Request,
    result: models.Service,
    service_id: str,
    source: str,
    db_session: orm.Session,
):
    user = request.scope.get("user")
    if user is None or not user.is_authenticated:
        return

    analytics = ConsultServiceEvent(
        service_id=service_id,
        source=source,
        user=user.username,
        score_qualite=result.score_qualite,
    )
    db_session.add(analytics)
    db_session.commit()


def save_analytics_services_list_event(
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

    analytics = ListServicesEvent(
        user=user.username,
        sources=sources,
        thematiques=thematiques,
        departement=departement.code if departement else None,
        region=region.code if region else None,
        code_commune=code_commune,
        frais=frais,
        profils=profils,
        modes_accueil=modes_accueil,
        types=types,
        inclure_suspendus=inclure_suspendus,
    )
    db_session.add(analytics)
    db_session.commit()


def save_analytics_structures_list_event(
    request: fastapi.Request,
    db_session: orm.Session,
    sources: list[str] | None = None,
    structure_id: str | None = None,
    typologie: di_schema.Typologie | None = None,
    label_national: di_schema.LabelNational | None = None,
    departement: Departement | None = None,
    region: Region | None = None,
    code_commune: di_schema.CodeCommune | None = None,
    thematiques: list[di_schema.Thematique] | None = None,
):
    user = request.scope.get("user")
    if user is None or not user.is_authenticated:
        return

    analytics = ListStructuresEvent(
        user=user.username,
        sources=sources,
        structure_id=structure_id,
        typologie=typologie,
        label_national=label_national,
        departement=departement.code if departement else None,
        region=region.code if region else None,
        code_commune=code_commune,
        thematiques=thematiques,
    )
    db_session.add(analytics)
    db_session.commit()


def save_analytics_services_search(
    request: fastapi.Request,
    db_session: orm.Session,
    services_listed: list,
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
    try:
        user = request.scope.get("user")
        if user is None or not user.is_authenticated:
            return
        first_services_ids_and_quality_score = [
            {"id": s.service.id, "score_qualite": s.service.score_qualite}
            for s in services_listed.items[:10]
        ]

        analytics = ServicesSearch(
            user=user.username,
            first_services_ids_and_quality_score=first_services_ids_and_quality_score,
            total_services_count=int(services_listed.total),
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
        db_session.add(analytics)
        db_session.commit()
    except Exception as e:
        print(e)
