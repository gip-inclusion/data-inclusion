from typing import Annotated

import sqlalchemy as sqla
from pydantic import AfterValidator, ValidationInfo
from pydantic_core import PydanticCustomError
from sqlalchemy import orm

from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.schema import v1


def get_commune(code_insee: str, db_session: orm.Session) -> Commune | None:
    return db_session.scalar(sqla.select(Commune).where(Commune.code == code_insee))


def code_insee_validator(value: str, info: ValidationInfo) -> str:
    db_session = None
    if info.context is not None:
        db_session = info.context.get("db_session")

    if db_session is None:
        return value

    if get_commune(value, db_session) is None:
        raise PydanticCustomError(
            "bad-code-insee",
            "Le code commune {value} n'existe pas",
            {"value": value},
        )

    return value


CodeCommune = Annotated[v1.CodeCommune | None, AfterValidator(code_insee_validator)]


class ValideurInputStructure(v1.Structure):
    code_insee: CodeCommune | None = None
    source: str | None = None


class ValideurInputService(v1.Service):
    code_insee: CodeCommune | None = None
    source: str | None = None
