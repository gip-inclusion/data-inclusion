from textwrap import dedent
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_serializer

from data_inclusion import schema


class Source(BaseModel):
    slug: str
    nom: str
    description: str | None


class Service(schema.Service):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    # Dont use pydantic's `HttpUrl`, because it would likely trigger validation errors
    prise_rdv: str | None = None
    formulaire_en_ligne: str | None = None
    lien_source: str | None = None

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


class Structure(schema.Structure):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    # Dont use pydantic's `HttpUrl`, because it would likely trigger validation errors
    site_web: str | None = None
    lien_source: str | None = None
    accessibilite: str | None = None

    cluster_id: Annotated[
        str | None,
        Field(description="ID du groupe de doublons", alias="doublons_groupe_id"),
    ]


class DetailedService(Service):
    structure: Structure


class ServiceSearchResult(BaseModel):
    service: DetailedService
    distance: int | None = None


class DetailedStructure(Structure):
    doublons: Annotated[
        list[schema.Structure],
        Field(description="Doublons connus de la structure"),
    ]
    services: list[Service]


class ListedStructure(Structure):
    doublons: Annotated[
        list[schema.Structure],
        Field(
            description="Identifiants (source, id) des doublons connus de la structure",
        ),
    ]

    @field_serializer("doublons")
    def serialize_doublons(
        self, doublons: list[schema.Structure]
    ) -> list[tuple[str, str]]:
        return [(d.source, d.id) for d in doublons]
