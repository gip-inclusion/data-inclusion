from starlette import requests, responses
from starlette.middleware import base

from data_inclusion.api.core.request.services import save_request


class RequestMiddleware(base.BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: requests.Request,
        call_next,
    ) -> responses.Response:
        response = await call_next(request)
        save_request(request, response)
        return response
