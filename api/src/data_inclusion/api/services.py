import logging
from datetime import timedelta
from typing import Optional
from urllib.parse import urljoin

import requests
import sqlalchemy as sqla
from sqlalchemy import orm

from data_inclusion.api import models, settings
from data_inclusion.api.core.request.models import Request
from data_inclusion.api.utils import timezone

logger = logging.getLogger(__name__)


def log_and_raise(resp, *args, **kwargs):
    import requests

    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


class SoliguideAPIClient:
    def __init__(self) -> None:
        self.base_url = settings.SOLIGUIDE_API_URL
        self.session = requests.Session()
        self.session.hooks["response"] = [log_and_raise]
        self.session.headers.update(
            {"Authorization": f"JWT {settings.SOLIGUIDE_API_TOKEN}"}
        )

    def retrieve_place(self, place_id: str) -> dict:
        return self.session.get(urljoin(self.base_url, f"place/{place_id}")).json()


def batch_forward_requests_to_soliguide(
    db_session: orm.Session,
    soliguide_api_client: SoliguideAPIClient,
):
    """Forward recent requests to soliguide.

    1. Get the requests logged in the last 24H for soliguide data
    2. List the associated place ids
    3. Send requests to soliguide's API
    """

    query = sqla.select(Request)
    query = query.filter(sqla.text("path_params ->> 'source' = 'soliguide'"))
    query = query.filter(timezone.now() - Request.created_at <= timedelta(hours=24))
    request_instances = db_session.scalars(query).all()

    logger.info(
        f"{len(request_instances)} requests made for soliguide data in the last 24H."
    )

    def get_soliguide_place_id(request_instance: Request) -> Optional[str]:
        if str(request_instance.endpoint_name) == "retrieve_structure_endpoint":
            return str(request_instance.path_params["id"])

        service_id = str(request_instance.path_params["id"])
        service_instance = db_session.execute(
            sqla.select(models.Service)
            .filter(models.Service.source == "soliguide")
            .filter(models.Service.id == service_id)
        ).scalar_one_or_none()

        # the service may have been deleted by the ETL by now
        if service_instance is None:
            return None

        return str(service_instance.structure_id)

    place_ids_list = [get_soliguide_place_id(r) for r in request_instances]
    place_ids_list = [id_ for id_ in place_ids_list if id_ is not None]

    for place_id in place_ids_list:
        soliguide_api_client.retrieve_place(place_id=place_id)
