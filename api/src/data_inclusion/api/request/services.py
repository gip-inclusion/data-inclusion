import fastapi

from data_inclusion.api.core import db
from data_inclusion.api.request import models


def is_trailing_slash_redirect(
    request: fastapi.Request, response: fastapi.Response
) -> bool:
    redirect_url = response.headers.get("location")
    return response.status_code == 307 and str(request.url) == f"{redirect_url}/"


def save_request(
    request: fastapi.Request,
    response: fastapi.Response,
    db_session=fastapi.Depends(db.get_session),
) -> None:
    if is_trailing_slash_redirect(request=request, response=response):
        return

    endpoint_name = None
    if (route := request.scope.get("route")) is not None:
        endpoint_name = route.name

    username = None
    if (user := request.scope.get("user")) is not None and user.is_authenticated:
        username = user.username

    request_instance = models.Request(
        status_code=response.status_code,
        method=request.method,
        path=request.url.path,
        base_url=str(request.base_url),
        user=username,
        path_params=request.path_params,
        query_params={
            key: ",".join(request.query_params.getlist(key))
            for key in request.query_params.keys()
        },
        client_host=request.client.host if request.client is not None else None,
        client_port=request.client.port if request.client is not None else None,
        endpoint_name=endpoint_name,
    )  # type: ignore

    db_session.add(request_instance)
    db_session.commit()
