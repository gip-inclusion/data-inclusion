import logging
from typing import Annotated

import httpx
import sqlalchemy as sqla

import fastapi

from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.inclusion_data import models

logger = logging.getLogger(__name__)


def log_error(response):
    try:
        response.raise_for_status()
    except httpx.HTTPError as err:
        logger.error(response.json())
        logger.error(err)


class SoliguideAPIClient:
    def __init__(self) -> None:
        self.client = httpx.Client(
            base_url=settings.SOLIGUIDE_API_URL,
            headers={"Authorization": f"JWT {settings.SOLIGUIDE_API_TOKEN}"},
            event_hooks={"response": [log_error]},
            timeout=1,  # seconds
        )

    def retrieve_place(self, place_id: str) -> dict:
        return self.client.get(f"/place/{place_id}").json()


def notify_soliguide_dependency(
    request: fastapi.Request,
    source: Annotated[str, fastapi.Path()],
    id: Annotated[str, fastapi.Path()],
    soliguide_client: Annotated[
        SoliguideAPIClient, fastapi.Depends(SoliguideAPIClient)
    ],
    background_tasks: fastapi.BackgroundTasks,
    db_session=fastapi.Depends(db.get_session),
):
    if source != "soliguide":
        return

    route = request.scope.get("route")

    if route is None:
        return

    match route.name:
        case "retrieve_structure_endpoint":
            place_id = id
        case "retrieve_service_endpoint":
            service_instance = db_session.scalar(
                sqla.select(models.Service)
                .filter(models.Service.source == "soliguide")
                .filter(models.Service.id == id)
            )

            if service_instance is None:
                return

            place_id = service_instance.structure_id
        case _:
            return

    logger.info(f"Notifying soliguide for place_id={place_id}")
    background_tasks.add_task(soliguide_client.retrieve_place, place_id=place_id)
