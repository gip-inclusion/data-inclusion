from datetime import datetime
from typing import Any, Optional, Union

import jose
from jose import jwt

from data_inclusion.api import settings

ALGORITHM = "HS256"


def create_access_token(subject: Union[str, Any]) -> str:
    expire = datetime.utcnow() + settings.ACCESS_TOKEN_LIFETIME
    encoded_jwt = jwt.encode(
        claims={
            "exp": expire,
            "sub": str(subject),
        },
        key=settings.SECRET_KEY,
        algorithm=ALGORITHM,
    )
    return encoded_jwt


def verify_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(
            token=token, key=settings.SECRET_KEY, algorithms=[ALGORITHM]
        )
    except jose.JWTError:
        return None

    return payload
