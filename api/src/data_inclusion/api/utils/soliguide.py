import logging
from typing import Annotated

import httpx
import sqlalchemy as sqla

import fastapi

from data_inclusion.api import models, settings
from data_inclusion.api.core import db

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

    if route.name == "retrieve_structure_endpoint":
        place_id = id
    elif route.name == "retrieve_service_endpoint":
        place_id = (
            db_session.execute(
                sqla.select(models.Service)
                .filter(models.Service.source == "soliguide")
                .filter(models.Service.id == id)
            )
            .scalar()
            .structure_id
        )
    else:
        return

    logger.info(f"Notifying soliguide for place_id={place_id}")
    background_tasks.add_task(soliguide_client.retrieve_place, place_id=place_id)
