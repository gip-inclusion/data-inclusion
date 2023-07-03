from starlette import authentication, responses
from starlette.middleware.authentication import (  # noqa: F401
    AuthenticationMiddleware as AuthenticationMiddleware,
)
from starlette.requests import HTTPConnection

import fastapi
from fastapi.security import HTTPBearer

from data_inclusion.api.core import jwt


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

        payload = jwt.verify_token(token.credentials)

        if payload is None:
            raise authentication.AuthenticationError("Not authenticated")

        scopes = ["authenticated"]

        if payload.get("admin", False):
            scopes += ["admin"]

        return authentication.AuthCredentials(scopes=scopes), authentication.SimpleUser(
            username=payload["sub"]
        )
