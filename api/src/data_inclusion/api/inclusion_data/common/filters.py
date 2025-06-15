from typing import Annotated, TypeVar

from pydantic.json_schema import SkipJsonSchema

import fastapi

from data_inclusion.api.decoupage_administratif.constants import (
    DepartementCodeEnum,
    RegionCodeEnum,
)

# This ensures a dropdown is shown in the openapi doc
# for optional enum query parameters.
T = TypeVar("T")
Optional = T | SkipJsonSchema[None]

S = TypeVar("S")

CodeCommuneFilter = Annotated[
    Optional[S],
    fastapi.Query(description="Code insee géographique d'une commune."),
]

CodeDepartementFilter = Annotated[
    Optional[DepartementCodeEnum],
    fastapi.Query(description="Code insee géographique d'un département."),
]

CodeRegionFilter = Annotated[
    Optional[RegionCodeEnum],
    fastapi.Query(description="Code insee géographique d'une région."),
]

ThematiquesFilter = Annotated[
    Optional[list[S]],
    fastapi.Query(
        description="""Une liste de thématique.
                Chaque résultat renvoyé a (au moins) une thématique dans cette liste."""
    ),
]

FraisFilter = Annotated[
    Optional[list[S]],
    fastapi.Query(
        description="""Une liste de frais.
                Chaque résultat renvoyé a (au moins) un frais dans cette liste."""
    ),
]

SourcesFilter = Annotated[
    Optional[list[str]],
    fastapi.Query(
        description="""Une liste d'identifiants de source.
                La liste des identifiants de source est disponible sur le endpoint
                dédié. Les résultats seront limités aux sources spécifiées.
            """,
    ),
]

ServiceTypesFilter = Annotated[
    Optional[list[S]],
    fastapi.Query(
        description="""Une liste de typologies de service.
                Chaque résultat renvoyé a (au moins) une typologie dans cette liste."""
    ),
]

ModesAccueilFilter = Annotated[
    Optional[list[S]],
    fastapi.Query(
        description="""Une liste de modes d'accueil.
                Chaque résultat renvoyé a (au moins) un mode d'accueil dans cette liste.
            """
    ),
]

ProfilsFilter = Annotated[
    Optional[list[S]],
    fastapi.Query(
        description="""Une liste de profils.
                Chaque résultat renvoyé a (au moins) un profil dans cette liste.
            """
    ),
]

SuspendusFilter = Annotated[
    Optional[bool],
    fastapi.Query(include_in_schema=False),
]


ExclureDoublonsStructuresFilter = Annotated[
    Optional[bool],
    fastapi.Query(
        description=(
            "[BETA] Mode qui ne retourne parmi les structures en doublon, que "
            "celles ayant les services les plus qualitatifs "
            "(voir [documentation](https://gip-inclusion.notion.site/Syst-me-de-d-duplication-des-donn-es-17d5f321b60480f99f6cf65522a83c8b?pvs=4))."
        )
    ),
]

ExclureDoublonsServicesFilter = Annotated[
    Optional[bool],
    fastapi.Query(
        description=(
            "[BETA] Mode qui ne retourne, parmi les services attachés à des "
            "structures en doublon, que ceux attachés à la structure la plus "
            "qualitative (voir [documentation](https://gip-inclusion.notion.site/Syst-me-de-d-duplication-des-donn-es-17d5f321b60480f99f6cf65522a83c8b?pvs=4))."
        )
    ),
]

RecherchePublicFilter = Annotated[
    Optional[str],
    fastapi.Query(
        description="""Une recherche en texte intégral parmi toutes
              les valeurs "publics" que nous collectons chez nos producteurs de données.
            """
    ),
]

ScoreQualiteMinimumFilter = Annotated[
    Optional[float],
    fastapi.Query(
        description="""[BETA] Score de qualité minimum.
                Les résultats renvoyés ont un score de qualité supérieur ou égal à ce
                score. (voir [documentation](https://gip-inclusion.notion.site/Conception-du-score-de-qualit-17b5f321b60480a1b79bf4a17b4567dd?pvs=4))"""
    ),
]


SearchCodeCommuneFilter = Annotated[
    Optional[S],
    fastapi.Query(
        description="""Code insee de la commune considérée.
                Si fourni, les résultats inclus également les services proches de
                cette commune. Les résultats sont triés par ordre de distance
                croissante.
            """
    ),
]

SearchLatitudeFilter = Annotated[
    Optional[float],
    fastapi.Query(
        description="""Latitude du point de recherche.
                Nécessite également de fournir `lon`.
                Les résultats sont triés par ordre de distance croissante à ce point.
            """
    ),
]

SearchLongitudeFilter = Annotated[
    Optional[float],
    fastapi.Query(
        description="""Longitude du point de recherche.
                Nécessite également de fournir `lat`.
                Les résultats sont triés par ordre de distance croissante à ce point.
            """
    ),
]
