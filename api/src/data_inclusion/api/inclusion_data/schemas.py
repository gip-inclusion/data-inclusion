from pydantic import BaseModel, ConfigDict, Field

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

    # TODO(vmttn): decide whether we should keep these extra fields
    di_geocodage_code_insee: schema.CodeCommune | None = Field(
        default=None, alias="_di_geocodage_code_insee"
    )
    di_geocodage_score: float | None = Field(
        default=None, ge=0, le=1, alias="_di_geocodage_score"
    )


class Structure(schema.Structure):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    # Dont use pydantic's `HttpUrl`, because it would likely trigger validation errors
    site_web: str | None = None
    lien_source: str | None = None
    accessibilite: str | None = None

    # TODO(vmttn): decide whether we should keep these extra fields
    di_geocodage_code_insee: schema.CodeCommune | None = Field(
        default=None, alias="_di_geocodage_code_insee"
    )
    di_geocodage_score: float | None = Field(
        default=None, ge=0, le=1, alias="_di_geocodage_score"
    )


class DetailedService(Service):
    structure: Structure


class ServiceSearchResult(BaseModel):
    service: DetailedService
    distance: int | None = None


class DetailedStructure(Structure):
    services: list[Service]
