from typing import Annotated

from starlette.authentication import AuthCredentials, SimpleUser, UnauthenticatedUser

import fastapi
from fastapi import security, status

from data_inclusion.api import auth
from data_inclusion.api.config import settings

credentials = security.HTTPBearer() if settings.TOKEN_ENABLED else None

credentials_dependency = fastapi.Depends(credentials, use_cache=True)
CredentialsDependency = Annotated[
    security.HTTPAuthorizationCredentials, credentials_dependency
]


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

    if not settings.TOKEN_ENABLED:
        return

    # extract token from header
    http_bearer_instance = security.HTTPBearer()
    try:
        credentials = await http_bearer_instance(request=request)
    except fastapi.HTTPException:
        credentials = None

    if credentials is None:
        return

    # extract payload from token
    payload = auth.verify_token(credentials.credentials)

    if payload is not None:
        scopes = ["authenticated"]
        if payload.get("admin", False):
            scopes += ["admin"]

        request.scope["user"], request.scope["auth"] = (
            SimpleUser(username=payload["sub"]),
            AuthCredentials(scopes=scopes),
        )


authenticate_dependency = fastapi.Depends(authenticate, use_cache=True)


async def authenticated(
    request: fastapi.Request,
    _authenticate=authenticate_dependency,
    _credentials=credentials_dependency,
) -> None:
    """Ensure the request is authenticated"""
    if not request.user.is_authenticated:
        raise fastapi.HTTPException(status_code=status.HTTP_403_FORBIDDEN)


authenticated_dependency = fastapi.Security(authenticated, scopes=["authenticated"])


async def admin(
    request: fastapi.Request,
    _authenticated=authenticated_dependency,
    _credentials=credentials_dependency,
) -> None:
    """Ensure the request is authenticated and has admin permissions"""
    if "admin" not in request.auth.scopes:
        raise fastapi.HTTPException(status_code=status.HTTP_403_FORBIDDEN)


admin_dependency = fastapi.Security(admin, scopes=["admin"])
