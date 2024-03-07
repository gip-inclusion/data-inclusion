from pydantic import BaseModel
from typing_extensions import Annotated

import fastapi

from data_inclusion.api.core import jwt

router = fastapi.APIRouter()


class TokenCreationData(BaseModel):
    email: str


class Token(BaseModel):
    access: str


def create_token(email: str) -> Token:
    return Token(access=jwt.create_access_token(subject=email))


@router.post(
    "/create_token",
    response_model=Token,
)
def create_token_endpoint(
    token_creation_data: Annotated[TokenCreationData, fastapi.Body()],
    request: fastapi.Request,
):
    if "admin" in request.auth.scopes:
        return create_token(email=token_creation_data.email)
    else:
        raise fastapi.HTTPException(
            status_code=fastapi.status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized.",
        )
