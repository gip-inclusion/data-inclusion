import jwt

from data_inclusion.api.config import settings

ALGORITHM = "HS256"


def create_access_token(
    subject,
    admin: bool | None = False,
) -> str:
    encoded_jwt = jwt.encode(
        payload={
            "sub": str(subject),
            "admin": admin,
        },
        key=settings.SECRET_KEY,
        algorithm=ALGORITHM,
    )
    return encoded_jwt


def verify_token(token: str) -> dict | None:
    try:
        payload = jwt.decode(jwt=token, key=settings.SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.InvalidTokenError:
        return None

    return payload
