from enum import Enum
from typing import Annotated, TypeVar

import pydantic

from data_inclusion.api.decoupage_administratif.constants import (
    DepartementCodeEnum,
    RegionCodeEnum,
)


class ExclureDoublonsServicesMode(str, Enum):
    STRICT = "strict"
    THEMATIQUES = "thematiques"


S = TypeVar("S")

CodeCommuneFilter = Annotated[
    S | None,
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
    list[S] | None,
    pydantic.Field(
        description="""Une liste de thématique.
                Chaque résultat renvoyé a (au moins) une thématique dans cette liste."""
    ),
]

FraisFilter = Annotated[
    list[S] | None,
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
    list[S] | None,
    pydantic.Field(
        description="""Une liste de typologies de service.
                Chaque résultat renvoyé a (au moins) une typologie dans cette liste."""
    ),
]

ModesAccueilFilter = Annotated[
    list[S] | None,
    pydantic.Field(
        description="""Une liste de modes d'accueil.
                Chaque résultat renvoyé a (au moins) un mode d'accueil dans cette liste.
            """
    ),
]

ProfilsFilter = Annotated[
    list[S] | None,
    pydantic.Field(
        description="""Une liste de profils.
                Chaque résultat renvoyé a (au moins) un profil dans cette liste.
            """
    ),
]

PublicsFilter = Annotated[
    list[S] | None,
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

ExclureDoublonsServicesFilterV1 = Annotated[
    ExclureDoublonsServicesMode | None,
    pydantic.Field(
        description=(
            "[BETA] Mode de déduplication des services. "
            "'strict' : ne retourne, parmi les services attachés à des structures "
            "en doublon, que ceux attachés à la structure la plus qualitative. "
            "'thematiques' : ne retourne qu'un service (le plus qualitatif) par "
            "cluster de structures en doublon et par thématique. "
            "Voir [documentation](https://gip-inclusion.notion.site/Syst-me-de-d-duplication-des-donn-es-17d5f321b60480f99f6cf65522a83c8b?pvs=4)."
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
        description="""[BETA] Score de qualité minimum.
                Les résultats renvoyés ont un score de qualité supérieur ou égal à ce
                score. (voir [documentation](https://gip-inclusion.notion.site/Conception-du-score-de-qualit-17b5f321b60480a1b79bf4a17b4567dd?pvs=4))"""
    ),
]


SearchCodeCommuneFilter = Annotated[
    S | None,
    pydantic.Field(
        description="""Code insee de la commune considérée.
                Si fourni, les résultats inclus également les services proches de
                cette commune. Les résultats sont triés par ordre de distance
                croissante.
            """
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
