import logging
from typing import Annotated

import httpx
import sqlalchemy as sqla

import fastapi

from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.inclusion_data.v0 import models as models_v0
from data_inclusion.api.inclusion_data.v1 import models as models_v1

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


def is_allowed_user(request: fastapi.Request) -> bool:
    if not request.user.is_authenticated:
        return False

    if not (
        request.user.username.startswith("dora-")
        # les-emplois API token, not the widget token
        or request.user.username == "les-emplois-2025-04-10"
    ):
        return False

    return True


def notify_soliguide_dependency(
    request: fastapi.Request,
    soliguide_client: Annotated[
        SoliguideAPIClient, fastapi.Depends(SoliguideAPIClient)
    ],
    background_tasks: fastapi.BackgroundTasks,
    db_session=fastapi.Depends(db.get_session),
):
    match request.url.path.strip("/").split("/"):
        # in v0, `id` are scoped by `source`
        case ["api", "v0", "structures", "soliguide", id]:
            place_id = id
        case ["api", "v0", "services", "soliguide", id]:
            service_instance = db_session.scalar(
                sqla.select(models_v0.Service)
                .filter(models_v0.Service.source == "soliguide")
                .filter(models_v0.Service.id == id)
            )
            if service_instance is None:
                return
            place_id = service_instance.structure_id

        # in v1, `id` are globally unique and prefixed by the source
        case ["api", "v1", "structures", id] if id.startswith("soliguide--"):
            place_id = id.split("--", 1)[1]
        case ["api", "v1", "services", id] if id.startswith("soliguide--"):
            service_instance = db_session.scalar(
                sqla.select(models_v1.Service).filter(models_v1.Service.id == id)
            )
            if service_instance is None:
                return
            place_id = service_instance.structure_id.split("--", 1)[1]
        case _:
            return

    logger.info(f"Notifying soliguide for place_id={place_id}")
    background_tasks.add_task(soliguide_client.retrieve_place, place_id=place_id)
