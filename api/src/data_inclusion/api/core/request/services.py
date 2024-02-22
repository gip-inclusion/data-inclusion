from fastapi import requests, responses

from data_inclusion.api.core import db
from data_inclusion.api.core.request import models


def save_request(request: requests.Request, response: responses.Response) -> None:
    with db.SessionLocal() as session:
        request_instance = models.Request(
            status_code=response.status_code,
            method=request.method,
            path=request.url.path,
            base_url=str(request.base_url),
            user=request.user.username if request.user.is_authenticated else None,
            path_params=request.path_params,
            query_params={
                key: ",".join(request.query_params.getlist(key))
                for key in request.query_params.keys()
            },
            client_host=request.client.host if request.client is not None else None,
            client_port=request.client.port if request.client is not None else None,
            endpoint_name=(
                request.scope["route"].name if "route" in request.scope else None
            ),
        )  # type: ignore
        session.add(request_instance)
        session.commit()
