from textwrap import dedent
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from data_inclusion.api.v0.inclusion_schema import legacy as schema


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


class DetailedService(Service):
    structure: Structure


class ServiceSearchResult(BaseModel):
    service: DetailedService
    distance: int | None = None


class DetailedStructure(Structure):
    services: list[Service]
