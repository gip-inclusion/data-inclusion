import fastapi

from data_inclusion.api.core import db
from data_inclusion.api.core.request import models


def save_request(
    request: fastapi.Request,
    response: fastapi.Response,
    db_session=fastapi.Depends(db.get_session),
) -> None:
    # ignore trailing slash redirects
    if response.status_code == 307:
        return

    endpoint_name = None
    if (route := request.scope.get("route")) is not None:
        endpoint_name = route.name

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
        endpoint_name=endpoint_name,
    )  # type: ignore

    db_session.add(request_instance)
    db_session.flush()
