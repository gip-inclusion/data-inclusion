from pydantic import BaseModel
from typing_extensions import Annotated

import fastapi

from data_inclusion.api.auth import services
from data_inclusion.api.auth.dependencies import admin_dependency

router = fastapi.APIRouter(tags=["Auth"])


class TokenCreationData(BaseModel):
    email: str


class Token(BaseModel):
    access: str


def create_token(email: str) -> Token:
    return Token(access=services.create_access_token(subject=email))


@router.post(
    "/create_token",
    response_model=Token,
    dependencies=[admin_dependency],
)
def create_token_endpoint(
    token_creation_data: Annotated[TokenCreationData, fastapi.Body()],
):
    return create_token(email=token_creation_data.email)
