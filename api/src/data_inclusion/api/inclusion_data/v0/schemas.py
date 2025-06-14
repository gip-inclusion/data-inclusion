from textwrap import dedent
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from data_inclusion.schema import v0 as schema


class Source(BaseModel):
    slug: str
    nom: str
    description: str | None


class Service(schema.Service):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    # Dont use pydantic's `HttpUrl`, because it would likely trigger validation errors
    prise_rdv: str | None = None
    formulaire_en_ligne: str | None = None

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


class BaseStructure(schema.Structure):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    # Dont use pydantic's `HttpUrl`, because it would likely trigger validation errors
    site_web: str | None = None
    accessibilite: str | None = None


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
