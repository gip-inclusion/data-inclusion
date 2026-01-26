from typing import Annotated

from pydantic import BaseModel

import fastapi

from data_inclusion.api import auth

router = fastapi.APIRouter(tags=["Auth"])


class TokenCreationData(BaseModel):
    email: str


class Token(BaseModel):
    access: str


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
    return Token(access=auth.create_access_token(subject=token_creation_data.email))
