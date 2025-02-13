from pydantic import BaseModel

import fastapi

from data_inclusion.api.v0.inclusion_schema import legacy as schema

router = fastapi.APIRouter()


class FrameworkValue(BaseModel):
    """Schema for members of a domain taxonomy"""

    value: str
    label: str
    description: str | None = None


@router.get(
    "/labels-nationaux",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des labels nationaux",
)
def list_labels_nationaux_endpoint():
    return schema.LabelNational.as_dict_list()


@router.get(
    "/thematiques",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des thématiques",
)
def list_thematiques_endpoint():
    return schema.Thematique.as_dict_list()


@router.get(
    "/typologies-services",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des typologies de services",
)
def list_typologies_services_endpoint():
    return schema.TypologieService.as_dict_list()


@router.get(
    "/frais",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des frais",
)
def list_frais_endpoint():
    return schema.Frais.as_dict_list()


@router.get(
    "/profils",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des profils de publics",
)
def list_profils_endpoint():
    return schema.Profil.as_dict_list()


@router.get(
    "/typologies-structures",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des typologies de structures",
)
def list_typologies_structures_endpoint():
    return schema.Typologie.as_dict_list()


@router.get(
    "/modes-accueil",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des modes d'accueil",
)
def list_modes_accueil_endpoint():
    return schema.ModeAccueil.as_dict_list()


@router.get(
    "/modes-orientation-accompagnateur",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des modes d'orientation de l'accompagnateur",
)
def list_modes_orientation_accompagnateur_endpoint():
    return schema.ModeOrientationAccompagnateur.as_dict_list()


@router.get(
    "/modes-orientation-beneficiaire",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des modes d'orientation du bénéficiaire",
)
def list_modes_orientation_beneficiaire_endpoint():
    return schema.ModeOrientationBeneficiaire.as_dict_list()
