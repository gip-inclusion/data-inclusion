from textwrap import dedent
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator
from pydantic.experimental.missing_sentinel import MISSING
from pydantic.json_schema import SkipJsonSchema

from data_inclusion.schema import v1 as schema


class Source(BaseModel):
    slug: str
    nom: str
    description: str | None


class Service(schema.Service):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    adresse_certifiee: Annotated[
        bool,
        Field(
            description=dedent("""\
                Indique si l'adresse  du service a été certifiée par géocodage.

                Lorsque `true`, les champs d'adresse (adresse, commune,
                code_postal, code_insee, longitude, latitude) proviennent
                du résultat de géocodage via la BAN (Base Adresse Nationale)

                Lorsque `false`, les champs d'adresse contiennent les valeurs
                originales fournies par le producteur de données.
                """),
        ),
    ]

    score_qualite: Annotated[
        float,
        Field(
            ge=0,
            le=1,
            description=dedent("""\
                Score de qualité du service, défini et calculé par data·inclusion.
                """),
        ),
    ]

    # This field is hidden from the openapi schema AND the data is not serialized
    # unless the context flag `is_extra_visible` is set to True.
    extra: Annotated[SkipJsonSchema[dict | None], Field()] = None

    @field_validator("extra")
    def show_extra(cls, v: dict | None, info: ValidationInfo) -> dict | None | MISSING:
        """Show the extra field only if the context flag is set."""

        if info.context is not None and info.context.get("is_extra_visible", False):
            return v

        return MISSING


class BaseStructure(schema.Structure):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    adresse_certifiee: Annotated[
        bool,
        Field(
            description=dedent("""\
                Indique si l'adresse  de la structure a été certifiée par géocodage.

                Lorsque `true`, les champs d'adresse (adresse, commune,
                code_postal, code_insee, longitude, latitude) proviennent
                du résultat de géocodage via la BAN (Base Adresse Nationale)

                Lorsque `false`, les champs d'adresse contiennent les valeurs
                originales fournies par le producteur de données.
                """),
        ),
    ]


class DetailedService(Service):
    structure: BaseStructure


class ServiceSearchResult(BaseModel):
    service: DetailedService
    distance: int | None = None


class Structure(BaseStructure):
    score_qualite: Annotated[
        float,
        Field(
            ge=0,
            le=1,
            description=dedent("""\
                [BETA] Score de qualité de la structure. Défini comme la moyenne
                des scores de qualité des services associés à la structure, ou 0
                si aucun service n'est associé.
                """),
        ),
    ]


class DetailedStructure(Structure):
    doublons: Annotated[
        list[schema.Structure],
        Field(description="Doublons connus de la structure"),
    ]
    services: list[Service]


class SourceIdDict(BaseModel):
    source: str
    id: str


class ListedStructure(Structure):
    doublons: Annotated[
        list[SourceIdDict],
        Field(
            description="Identifiants (source, id) des doublons connus de la structure",
        ),
    ]
