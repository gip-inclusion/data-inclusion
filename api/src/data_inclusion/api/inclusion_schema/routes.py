from pydantic import BaseModel

import fastapi

from data_inclusion import schema

router = fastapi.APIRouter()


class EnhancedEnumMember(BaseModel):
    value: str
    label: str
    description: str | None = None


@router.get(
    "/labels-nationaux",
    response_model=list[EnhancedEnumMember],
    summary="Documente les labels nationaux",
)
def list_labels_nationaux_endpoint():
    """
    ## Documente les labels nationaux
    """
    return schema.LabelNational.as_dict_list()


@router.get(
    "/thematiques",
    response_model=list[EnhancedEnumMember],
    summary="Documente les thématiques",
)
def list_thematiques_endpoint():
    """
    ## Documente les thématiques
    """
    return schema.Thematique.as_dict_list()


@router.get(
    "/typologies-services",
    response_model=list[EnhancedEnumMember],
    summary="Documente les typologies de services",
)
def list_typologies_services_endpoint():
    """
    ## Documente les typologies de services
    """
    return schema.TypologieService.as_dict_list()


@router.get(
    "/frais",
    response_model=list[EnhancedEnumMember],
    summary="Documente les frais",
)
def list_frais_endpoint():
    """
    ## Documente les frais
    """
    return schema.Frais.as_dict_list()


@router.get(
    "/profils",
    response_model=list[EnhancedEnumMember],
    summary="Documente les profils de publics",
)
def list_profils_endpoint():
    """
    ## Documente les profils de publics
    """
    return schema.Profil.as_dict_list()


@router.get(
    "/typologies-structures",
    response_model=list[EnhancedEnumMember],
    summary="Documente les typologies de structures",
)
def list_typologies_structures_endpoint():
    """
    ## Documente les typologies de structures
    """
    return schema.Typologie.as_dict_list()


@router.get(
    "/modes-accueil",
    response_model=list[EnhancedEnumMember],
    summary="Documente les modes d'accueil",
)
def list_modes_accueil_endpoint():
    """
    ## Documente les modes d'accueil
    """
    return schema.ModeAccueil.as_dict_list()


@router.get(
    "/modes-orientation-accompagnateur",
    response_model=list[EnhancedEnumMember],
    summary="Documente les modes d'orientation de l'accompagnateur",
)
def list_modes_orientation_accompagnateur_endpoint():
    """
    ## Documente les modes d'orientation de l'accompagnateur
    """
    return schema.ModeOrientationAccompagnateur.as_dict_list()


@router.get(
    "/modes-orientation-beneficiaire",
    response_model=list[EnhancedEnumMember],
    summary="Documente les modes d'orientation du bénéficiaire",
)
def list_modes_orientation_beneficiaire_endpoint():
    """
    ## Documente les modes d'orientation du bénéficiaire
    """
    return schema.ModeOrientationBeneficiaire.as_dict_list()
