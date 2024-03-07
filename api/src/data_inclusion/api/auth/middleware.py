from starlette import authentication, responses
from starlette.middleware.authentication import (
    AuthenticationMiddleware as BaseAuthenticationMiddleware,
)
from starlette.requests import HTTPConnection

import fastapi
from fastapi.security import HTTPBearer

from data_inclusion.api.auth import services


def on_error(conn: HTTPConnection, exc: Exception) -> responses.Response:
    return responses.JSONResponse(content=str(exc), status_code=403)


class AuthenticationBackend(authentication.AuthenticationBackend):
    async def authenticate(self, conn):
        if "Authorization" not in conn.headers:
            return

        http_bearer_instance = HTTPBearer()

        try:
            token = await http_bearer_instance(request=conn)
        except fastapi.HTTPException as exc:
            raise authentication.AuthenticationError(exc.detail)

        payload = services.verify_token(token.credentials)

        if payload is None:
            raise authentication.AuthenticationError("Not authenticated")

        scopes = ["authenticated"]

        if payload.get("admin", False):
            scopes += ["admin"]

        return authentication.AuthCredentials(scopes=scopes), authentication.SimpleUser(
            username=payload["sub"]
        )


class AuthenticationMiddleware(BaseAuthenticationMiddleware):
    def __init__(self, app) -> None:
        super().__init__(app, backend=AuthenticationBackend(), on_error=on_error)
