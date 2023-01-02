from fastapi import requests, responses

from data_inclusion.api.core import db
from data_inclusion.api.core.request import models


def save_request(request: requests.Request, response: responses.Response) -> None:
    with db.SessionLocal() as session:
        request_instance = models.Request(
            status_code=response.status_code,
            method=request.method,
            path=request.url.path,
            user=request.scope.get("user"),
        )
        session.add(request_instance)
        session.commit()
