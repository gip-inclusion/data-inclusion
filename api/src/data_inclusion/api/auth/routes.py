from typing import Annotated

import furl
from pydantic import BaseModel

import fastapi

from data_inclusion.api import auth

router = fastapi.APIRouter(tags=["Auth"])


class TokenCreationData(BaseModel):
    email: str
    hosts: list[str] | None = None


class Token(BaseModel):
    access: str
    allowed_hosts: list[str] | None = None


def create_token(email: str) -> Token:
    return Token(access=auth.create_access_token(subject=email))


@router.post(
    "/create_token",
    response_model=Token,
    dependencies=[auth.admin_dependency],
)
def create_token_endpoint(
    token_creation_data: Annotated[TokenCreationData, fastapi.Body()],
):
    allowed_hosts = None
    if token_creation_data.hosts:
        allowed_hosts = []
        for value in token_creation_data.hosts:
            if not value:
                continue
            # allow URLs or hostnames in JSON payload
            host = furl.furl(value).host or value
            if host:
                allowed_hosts.append(host)

    return Token(
        access=auth.create_access_token(
            subject=token_creation_data.email,
            allowed_hosts=allowed_hosts,
        ),
        allowed_hosts=allowed_hosts,
    )
