import logging

import jwt

from data_inclusion.api.config import settings

ALGORITHM = "HS256"

logger = logging.getLogger(__name__)


def create_access_token(
    subject,
    admin: bool | None = False,
    allowed_hosts: list[str] | None = None,
) -> str:
    payload = {
        "sub": str(subject),
        "admin": admin,
    }
    if allowed_hosts is not None:
        payload["allowed_hosts"] = allowed_hosts
    encoded_jwt = jwt.encode(
        payload=payload,
        key=settings.SECRET_KEY,
        algorithm=ALGORITHM,
    )
    return encoded_jwt


def verify_token(token: str) -> dict | None:
    try:
        payload = jwt.decode(jwt=token, key=settings.SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.InvalidTokenError:
        pass
    else:
        return payload

    if settings.OLD_SECRET_KEY is None:
        logger.info("no old secret key, aborting legacy authentication")
        return None

    try:
        payload = jwt.decode(
            jwt=token, key=settings.OLD_SECRET_KEY, algorithms=[ALGORITHM]
        )
    except jwt.InvalidTokenError:
        return None

    logger.info("old (rotated) token used by sub=%s", payload["sub"])

    if payload["sub"] not in settings.OLD_USER_SUBS:
        return None

    return payload
