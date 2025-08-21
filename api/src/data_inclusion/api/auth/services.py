import logging

import jwt

ALGORITHM = "HS256"

logger = logging.getLogger(__name__)


def create_access_token(
    secret_key,
    subject,
    admin: bool | None = False,
) -> str:
    encoded_jwt = jwt.encode(
        payload={
            "sub": str(subject),
            "admin": admin,
        },
        key=secret_key,
        algorithm=ALGORITHM,
    )
    return encoded_jwt


def verify_token(token: str, settings) -> dict | None:
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
