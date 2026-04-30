from typing import Annotated, Self

import pydantic

import fastapi

from data_inclusion.api.decoupage_administratif.constants import (
    Departement,
    DepartementCodeEnum,
    DepartementEnum,
    DepartementSlugEnum,
    Region,
    RegionCodeEnum,
    RegionEnum,
    RegionSlugEnum,
)
from data_inclusion.api.utils import pagination
from data_inclusion.schema import v1

CodeCommuneFilter = Annotated[
    v1.CodeCommune | None,
    pydantic.Field(description="Code insee géographique d'une commune."),
]

CodeDepartementFilter = Annotated[
    DepartementCodeEnum | None,
    pydantic.Field(description="Code insee géographique d'un département."),
]

CodeRegionFilter = Annotated[
    RegionCodeEnum | None,
    pydantic.Field(description="Code insee géographique d'une région."),
]

ThematiquesFilter = Annotated[
    list[v1.Thematique | v1.Categorie] | None,
    pydantic.Field(
        description="""Une liste de thématique.
                Chaque résultat renvoyé a (au moins) une thématique dans cette liste."""
    ),
]

FraisFilter = Annotated[
    list[v1.Frais] | None,
    pydantic.Field(
        description="""Une liste de frais.
                Chaque résultat renvoyé a (au moins) un frais dans cette liste."""
    ),
]

SourcesFilter = Annotated[
    list[str] | None,
    pydantic.Field(
        description="""Une liste d'identifiants de source.
                La liste des identifiants de source est disponible sur le endpoint
                dédié. Les résultats seront limités aux sources spécifiées.
            """,
    ),
]

ServiceTypesFilter = Annotated[
    list[v1.TypeService] | None,
    pydantic.Field(
        description="""Une liste de typologies de service.
                Chaque résultat renvoyé a (au moins) une typologie dans cette liste."""
    ),
]

ModesAccueilFilter = Annotated[
    list[v1.ModeAccueil] | None,
    pydantic.Field(
        description="""Une liste de modes d'accueil.
                Chaque résultat renvoyé a (au moins) un mode d'accueil dans cette liste.
            """
    ),
]


PublicsFilter = Annotated[
    list[v1.Public] | None,
    pydantic.Field(
        description="""Une liste de publics.
                Chaque résultat renvoyé a (au moins) un public dans cette liste.
            """
    ),
]

SuspendusFilter = Annotated[
    bool | None,
    pydantic.Field(deprecated=False),
]


ExclureDoublonsStructuresFilter = Annotated[
    bool | None,
    pydantic.Field(
        description=(
            "[BETA] Mode qui ne retourne parmi les structures en doublon, que "
            "celles ayant les services les plus qualitatifs "
            "(voir [documentation](https://gip-inclusion.notion.site/Syst-me-de-d-duplication-des-donn-es-17d5f321b60480f99f6cf65522a83c8b?pvs=4))."
        )
    ),
]

ExclureDoublonsServicesFilter = Annotated[
    bool | None,
    pydantic.Field(
        description=(
            "[BETA] Mode qui ne retourne, parmi les services attachés à des "
            "structures en doublon, que ceux attachés à la structure la plus "
            "qualitative (voir [documentation](https://gip-inclusion.notion.site/Syst-me-de-d-duplication-des-donn-es-17d5f321b60480f99f6cf65522a83c8b?pvs=4))."
        )
    ),
]

RecherchePublicFilter = Annotated[
    str | None,
    pydantic.Field(
        description="""Une recherche en texte intégral parmi toutes
              les valeurs "publics" que nous collectons chez nos producteurs de données.
            """
    ),
]

ScoreQualiteMinimumFilter = Annotated[
    float | None,
    pydantic.Field(
        description="""Score de qualité minimum.
                Les résultats renvoyés ont un score de qualité supérieur ou égal à ce
                score. (voir [documentation](https://gip-inclusion.notion.site/Conception-du-score-de-qualit-17b5f321b60480a1b79bf4a17b4567dd?pvs=4))"""
    ),
]


SearchLatitudeFilter = Annotated[
    float | None,
    pydantic.Field(
        description="""Latitude du point de recherche.
                Nécessite également de fournir `lon`.
                Les résultats sont triés par ordre de distance croissante à ce point.
            """
    ),
]

SearchLongitudeFilter = Annotated[
    float | None,
    pydantic.Field(
        description="""Longitude du point de recherche.
                Nécessite également de fournir `lat`.
                Les résultats sont triés par ordre de distance croissante à ce point.
            """
    ),
]


class ListStructuresQueryParams(pydantic.BaseModel, pagination.get_pagination_params()):
    sources: SourcesFilter = None
    reseaux_porteurs: Annotated[list[v1.ReseauPorteur] | None, pydantic.Field()] = None
    code_region: CodeRegionFilter = None
    slug_region: Annotated[RegionSlugEnum | None, pydantic.Field()] = None
    code_departement: CodeDepartementFilter = None
    slug_departement: Annotated[DepartementSlugEnum | None, pydantic.Field()] = None
    code_commune: CodeCommuneFilter = None
    exclure_doublons: ExclureDoublonsStructuresFilter = False

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
    id: Annotated[str, pydantic.Field()]


class ListServicesQueryParams(pydantic.BaseModel, pagination.get_pagination_params()):
    sources: SourcesFilter = None
    thematiques: ThematiquesFilter = None
    code_region: CodeRegionFilter = None
    slug_region: Annotated[RegionSlugEnum | None, pydantic.Field()] = None
    code_departement: CodeDepartementFilter = None
    slug_departement: Annotated[DepartementSlugEnum | None, pydantic.Field()] = None
    code_commune: CodeCommuneFilter = None
    frais: FraisFilter = None
    publics: PublicsFilter = None
    recherche_public: RecherchePublicFilter = None
    modes_accueil: ModesAccueilFilter = None
    types: ServiceTypesFilter = None
    score_qualite_minimum: ScoreQualiteMinimumFilter = None

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
    id: Annotated[str, pydantic.Field()]


class SearchServicesQueryParams(pydantic.BaseModel, pagination.get_pagination_params()):
    sources: SourcesFilter = None
    code_commune: Annotated[
        v1.CodeCommune | None,
        pydantic.Field(
            description="""Code insee de la commune considérée.
                Si fourni, les résultats inclus également les services proches de
                cette commune. Les résultats sont triés par ordre de distance
                croissante.
            """
        ),
    ] = None
    lat: SearchLatitudeFilter = None
    lon: SearchLongitudeFilter = None
    thematiques: ThematiquesFilter = None
    frais: FraisFilter = None
    modes_accueil: ModesAccueilFilter = None
    publics: PublicsFilter = None
    recherche_public: RecherchePublicFilter = None
    types: ServiceTypesFilter = None
    score_qualite_minimum: ScoreQualiteMinimumFilter = None
    exclure_doublons: ExclureDoublonsServicesFilter = False

    @pydantic.model_validator(mode="after")
    def validate_lat_lon(self) -> Self:
        if (self.lat is None) != (self.lon is None):
            raise fastapi.HTTPException(
                status_code=422,
                detail="The `lat` and `lon` must be simultaneously filled.",
            )

        return self


class SearchQueryParams(pydantic.BaseModel, pagination.get_pagination_params()):
    q: Annotated[str | None, pydantic.Field(description="Termes à rechercher.")] = None

    sources: SourcesFilter = None
    code_commune: Annotated[
        v1.CodeCommune | None,
        pydantic.Field(description="""Code insee de la commune considérée."""),
    ] = None
    code_region: CodeRegionFilter = None
    slug_region: Annotated[RegionSlugEnum | None, pydantic.Field()] = None
    code_departement: CodeDepartementFilter = None
    slug_departement: Annotated[DepartementSlugEnum | None, pydantic.Field()] = None
    lat: SearchLatitudeFilter = None
    lon: SearchLongitudeFilter = None
    thematiques: ThematiquesFilter = None
    frais: FraisFilter = None
    modes_accueil: ModesAccueilFilter = None
    publics: PublicsFilter = None
    types: ServiceTypesFilter = None
    score_qualite_minimum: ScoreQualiteMinimumFilter = None

    @pydantic.model_validator(mode="after")
    def validate_lat_lon(self) -> Self:
        if (self.lat is None) != (self.lon is None):
            raise fastapi.HTTPException(
                status_code=422,
                detail="The `lat` and `lon` must be simultaneously filled.",
            )

        return self
