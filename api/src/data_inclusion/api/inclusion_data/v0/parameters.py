from typing import Annotated, Self

import pydantic

import fastapi

from data_inclusion.api.decoupage_administratif.constants import (
    Departement,
    DepartementEnum,
    DepartementSlugEnum,
    Region,
    RegionEnum,
    RegionSlugEnum,
)
from data_inclusion.api.inclusion_data.common import filters
from data_inclusion.api.utils import pagination
from data_inclusion.schema import v0 as schema


class ListStructuresQueryParams(pydantic.BaseModel, pagination.PaginationParamsMixin):
    sources: filters.SourcesFilter = None
    id: Annotated[str | None, pydantic.Field(deprecated=True)] = None
    typologie: Annotated[schema.TypologieStructure | None, pydantic.Field()] = None
    label_national: Annotated[schema.LabelNational | None, pydantic.Field()] = None
    thematiques: filters.ThematiquesFilter[schema.Thematique] = None
    code_region: filters.CodeRegionFilter = None
    slug_region: Annotated[RegionSlugEnum | None, pydantic.Field()] = None
    code_departement: filters.CodeDepartementFilter = None
    slug_departement: Annotated[DepartementSlugEnum | None, pydantic.Field()] = None
    code_commune: filters.CodeCommuneFilter[schema.CodeCommune] = None
    exclure_doublons: filters.ExclureDoublonsStructuresFilter = False

    @pydantic.computed_field
    @property
    def departement(self) -> Departement | None:
        if self.code_departement is not None:
            return DepartementEnum[self.code_departement.name].value
        elif self.slug_departement is not None:
            return DepartementEnum[self.slug_departement.name].value

    @pydantic.computed_field
    @property
    def region(self) -> Region | None:
        if self.code_region is not None:
            return RegionEnum[self.code_region.name].value
        elif self.slug_region is not None:
            return RegionEnum[self.slug_region.name].value


class RetrieveStructurePathParams(pydantic.BaseModel):
    source: Annotated[str, pydantic.Field()]
    id: Annotated[str, pydantic.Field()]


class ListServicesQueryParams(pydantic.BaseModel, pagination.PaginationParamsMixin):
    sources: filters.SourcesFilter = None
    thematiques: filters.ThematiquesFilter[schema.Thematique] = None
    code_region: filters.CodeRegionFilter = None
    slug_region: Annotated[RegionSlugEnum | None, pydantic.Field()] = None
    code_departement: filters.CodeDepartementFilter = None
    slug_departement: Annotated[DepartementSlugEnum | None, pydantic.Field()] = None
    code_commune: filters.CodeCommuneFilter[schema.CodeCommune] = None
    frais: filters.FraisFilter[schema.Frais] = None
    profils: filters.ProfilsFilter[schema.Profil] = None
    recherche_public: filters.RecherchePublicFilter = None
    modes_accueil: filters.ModesAccueilFilter[schema.ModeAccueil] = None
    types: filters.ServiceTypesFilter[schema.TypologieService] = None
    score_qualite_minimum: filters.ScoreQualiteMinimumFilter = None
    inclure_suspendus: filters.SuspendusFilter = False

    @pydantic.computed_field
    @property
    def departement(self) -> Departement | None:
        if self.code_departement is not None:
            return DepartementEnum[self.code_departement.name].value
        elif self.slug_departement is not None:
            return DepartementEnum[self.slug_departement.name].value

    @pydantic.computed_field
    @property
    def region(self) -> Region | None:
        if self.code_region is not None:
            return RegionEnum[self.code_region.name].value
        elif self.slug_region is not None:
            return RegionEnum[self.slug_region.name].value


class RetrieveServicePathParams(pydantic.BaseModel):
    source: Annotated[str, pydantic.Field()]
    id: Annotated[str, pydantic.Field()]


class SearchServicesQueryParams(pydantic.BaseModel, pagination.PaginationParamsMixin):
    sources: filters.SourcesFilter = None
    code_commune: filters.SearchCodeCommuneFilter[schema.CodeCommune] = None
    code_insee: Annotated[
        schema.CodeCommune | None,
        pydantic.Field(
            deprecated=True,
            description="Deprecated, use `code_commune` instead.",
        ),
    ] = None
    lat: filters.SearchLatitudeFilter = None
    lon: filters.SearchLongitudeFilter = None
    thematiques: filters.ThematiquesFilter[schema.Thematique] = None
    frais: filters.FraisFilter[schema.Frais] = None
    modes_accueil: filters.ModesAccueilFilter[schema.ModeAccueil] = None
    profils: filters.ProfilsFilter[schema.Profil] = None
    recherche_public: filters.RecherchePublicFilter = None
    types: filters.ServiceTypesFilter[schema.TypologieService] = None
    score_qualite_minimum: filters.ScoreQualiteMinimumFilter = None
    inclure_suspendus: filters.SuspendusFilter = None
    exclure_doublons: filters.ExclureDoublonsServicesFilter = False

    @pydantic.model_validator(mode="after")
    def validate_lat_lon(self) -> Self:
        if (self.lat is None) != (self.lon is None):
            raise fastapi.HTTPException(
                status_code=422,
                detail="The `lat` and `lon` must be simultaneously filled.",
            )

        return self

    @pydantic.model_validator(mode="after")
    def fill_code_commune_from_code_insee(self) -> Self:
        """Fill `code_commune` from the deprecated `code_insee`."""

        if self.code_commune is None and self.code_insee is not None:
            self.code_commune = self.code_insee

        return self
