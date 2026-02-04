import sentry_sdk
from starlette.authentication import (
    AuthCredentials,
    SimpleUser,
    UnauthenticatedUser,
)

import fastapi
from fastapi import security, status

from data_inclusion.api import auth
from data_inclusion.api.config import settings

credentials_dependency = (
    fastapi.Depends(security.HTTPBearer(), use_cache=True)
    if settings.TOKEN_ENABLED
    # hide auth from openapi UI
    else fastapi.Depends(lambda: None)
)


async def authenticate(request: fastapi.Request):
    """Process authentication for the current request

    This dependency should probably not be used directly in routes.
    It DOES NOT enforce authentication permissions.

    Starting from the http header token extraction provided by fastapi,
    this dependency reproduces starlette's AuthenticationMiddleware behaviour.

    Unlike starlette's middleware, this dependency can be reused to define
    fine-grained permission at the route level, in a way that is compatible
    with fastapi's swagger generation.
    """
    request.scope["user"], request.scope["auth"] = (
        UnauthenticatedUser(),
        AuthCredentials(),
    )

    try:
        credentials = await security.HTTPBearer()(request=request)
    except fastapi.HTTPException:
        return

    payload = auth.verify_token(credentials.credentials)
    if payload is not None:
        if "created_at" not in payload or "admin" in payload:
            scopes = []
            if payload.get("admin", False):
                scopes += ["admin"]
            if payload.get("allowed_hosts") or payload.get("allowed_origins"):
                scopes += ["widget"]
            else:
                scopes += ["api"]
        else:
            scopes = payload.get("scopes", [])

        request.scope["user"], request.scope["auth"] = (
            SimpleUser(username=payload["sub"]),
            AuthCredentials(scopes=scopes),
        )


authenticate_dependency = fastapi.Depends(authenticate, use_cache=True)


def authenticated(required_scopes: list[str]):
    if not settings.TOKEN_ENABLED:
        return []

    async def _authenticated(
        request: fastapi.Request,
        _authenticate=authenticate_dependency,
        _credentials=credentials_dependency,
    ) -> None:
        if not request.user.is_authenticated:
            raise fastapi.HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)

        if len(set(request.auth.scopes) & set(required_scopes)) == 0:
            raise fastapi.HTTPException(status_code=status.HTTP_403_FORBIDDEN)

        sentry_sdk.set_user({"username": request.user.username})

    return [fastapi.Security(_authenticated, scopes=required_scopes)]
