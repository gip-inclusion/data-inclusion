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
            query_params=dict(request.query_params),
            client_host=request.client.host,
            client_port=request.client.port,
            endpoint_name=request.scope["route"].name
            if "route" in request.scope
            else None,
        )
        session.add(request_instance)
        session.commit()
