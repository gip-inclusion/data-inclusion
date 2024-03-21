from typing import Annotated

from starlette.authentication import AuthCredentials, SimpleUser, UnauthenticatedUser

import fastapi
from fastapi import security, status

from data_inclusion.api.auth import services
from data_inclusion.api.config import settings

credentials = security.HTTPBearer() if settings.TOKEN_ENABLED else None

credentials_dependency = fastapi.Depends(credentials, use_cache=True)
CredentialsDependency = Annotated[
    security.HTTPAuthorizationCredentials, credentials_dependency
]


def _authenticate(
    request: fastapi.Request,
    credentials: CredentialsDependency,
):
    """Process authentication for the current request

    This dependency should probably not be used directly in routes.
    It DOES NOT enforce authentication permissions.

    Starting from the http header token extraction provided by fastapi,
    this dependency reproduces starlette's AuthenticationMiddleware behaviour.

    Unlike starlette's middleware, this dependency can be reused to define
    fine-grained permission at the route level, in a way that is compatible
    with fastapi's swagger generation.
    """
    payload = services.verify_token(credentials.credentials)

    if payload is None:
        request.scope["user"] = UnauthenticatedUser()
        request.scope["auth"] = AuthCredentials()

    else:
        scopes = ["authenticated"]
        if payload.get("admin", False):
            scopes += ["admin"]

        request.scope["user"] = SimpleUser(username=payload["sub"])
        request.scope["auth"] = AuthCredentials(scopes=scopes)


_authenticate_dependency = fastapi.Depends(_authenticate, use_cache=True)


def authenticated(
    request: fastapi.Request,
    _authenticate=_authenticate_dependency,
) -> None:
    """Ensure the request is authenticated"""
    if not request.user.is_authenticated:
        raise fastapi.HTTPException(status_code=status.HTTP_403_FORBIDDEN)


authenticated_dependency = fastapi.Security(authenticated, scopes=["authenticated"])


def admin(
    request: fastapi.Request,
    _authenticated=authenticated_dependency,
) -> None:
    """Ensure the request is authenticated and has admin permissions"""
    if "admin" not in request.auth.scopes:
        raise fastapi.HTTPException(status_code=status.HTTP_403_FORBIDDEN)


admin_dependency = fastapi.Security(admin, scopes=["admin"])
