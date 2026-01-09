from sqlalchemy import orm

import fastapi

from data_inclusion.api.analytics import helpers
from data_inclusion.api.analytics.v1 import models
from data_inclusion.api.inclusion_data.v1 import parameters


def save_event(
    request: fastapi.Request,
    db_session: orm.Session,
    params: parameters.ListStructuresQueryParams
    | parameters.RetrieveStructurePathParams
    | parameters.ListServicesQueryParams
    | parameters.RetrieveServicePathParams
    | parameters.SearchServicesQueryParams,
    score_qualite: float | None = None,
    first_results_page: dict | None = None,
):
    user = request.scope.get("user")
    if user is None or not user.is_authenticated:
        return

    if helpers.is_bot(request):
        return

    match params:
        case parameters.ListStructuresQueryParams():
            event = models.ListStructuresEvent(
                user=user.username,
                sources=params.sources,
                reseaux_porteurs=params.reseaux_porteurs,
                code_departement=params.departement.code
                if params.departement
                else None,
                code_region=params.region.code if params.region else None,
                code_commune=params.code_commune,
                exclure_doublons=params.exclure_doublons,
            )
        case parameters.RetrieveStructurePathParams():
            event = models.ConsultStructureEvent(
                structure_id=params.id,
                user=user.username,
            )
        case parameters.ListServicesQueryParams():
            event = models.ListServicesEvent(
                user=user.username,
                sources=params.sources,
                thematiques=params.thematiques,
                code_departement=params.departement.code
                if params.departement
                else None,
                code_region=params.region.code if params.region else None,
                code_commune=params.code_commune,
                frais=params.frais,
                publics=params.publics,
                modes_accueil=params.modes_accueil,
                types=params.types,
                recherche_public=params.recherche_public,
                score_qualite_minimum=params.score_qualite_minimum,
            )
        case parameters.RetrieveServicePathParams():
            event = models.ConsultServiceEvent(
                service_id=params.id,
                user=user.username,
                score_qualite=score_qualite,
            )
        case parameters.SearchServicesQueryParams():
            if first_results_page is None:
                raise ValueError(
                    "first_results_page must be provided for SearchServicesParams"
                )

            if (
                first_results_page["page"] is not None
                and first_results_page["page"] > 1
            ):
                return

            event = models.SearchServicesEvent(
                user=user.username,
                first_services=[
                    {
                        "id": result["service"].id,
                        "score_qualite": result["service"].score_qualite,
                        "distance": result["distance"],
                    }
                    for result in first_results_page["items"][:10]
                ],
                total_services=first_results_page["total"],
                sources=params.sources,
                code_commune=params.code_commune,
                lat=params.lat,
                lon=params.lon,
                thematiques=params.thematiques,
                frais=params.frais,
                modes_accueil=params.modes_accueil,
                publics=params.publics,
                types=params.types,
                recherche_public=params.recherche_public,
                score_qualite_minimum=params.score_qualite_minimum,
                exclure_doublons=getattr(params.exclure_doublons, "value", None),
            )
        case _:
            raise ValueError(f"Unsupported parameters type: {type(params)}")

    db_session.add(event)
    db_session.commit()
