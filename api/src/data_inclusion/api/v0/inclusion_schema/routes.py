from pydantic import BaseModel

import fastapi

from data_inclusion.api.v0.inclusion_data import schemas

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
    return schemas.LabelNational.as_dict_list()


@router.get(
    "/thematiques",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des thématiques",
)
def list_thematiques_endpoint():
    return schemas.Thematique.as_dict_list()


@router.get(
    "/typologies-services",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des typologies de services",
)
def list_typologies_services_endpoint():
    return schemas.TypologieService.as_dict_list()


@router.get(
    "/frais",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des frais",
)
def list_frais_endpoint():
    return schemas.Frais.as_dict_list()


@router.get(
    "/profils",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des profils de publics",
)
def list_profils_endpoint():
    return schemas.Profil.as_dict_list()


@router.get(
    "/typologies-structures",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des typologies de structures",
)
def list_typologies_structures_endpoint():
    return schemas.Typologie.as_dict_list()


@router.get(
    "/modes-accueil",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des modes d'accueil",
)
def list_modes_accueil_endpoint():
    return schemas.ModeAccueil.as_dict_list()


@router.get(
    "/modes-orientation-accompagnateur",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des modes d'orientation de l'accompagnateur",
)
def list_modes_orientation_accompagnateur_endpoint():
    return schemas.ModeOrientationAccompagnateur.as_dict_list()


@router.get(
    "/modes-orientation-beneficiaire",
    response_model=list[FrameworkValue],
    summary="Récupérer le référentiel des modes d'orientation du bénéficiaire",
)
def list_modes_orientation_beneficiaire_endpoint():
    return schemas.ModeOrientationBeneficiaire.as_dict_list()
