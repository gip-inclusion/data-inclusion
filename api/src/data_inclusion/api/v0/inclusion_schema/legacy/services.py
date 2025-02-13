from datetime import date, datetime
from typing import Annotated, Optional

from pydantic import EmailStr, HttpUrl, StringConstraints

from data_inclusion.schema import common
from data_inclusion.schema.base import BaseModel, Field
from data_inclusion.schema.frais import Frais
from data_inclusion.schema.modes_accueil import ModeAccueil
from data_inclusion.schema.modes_orientation import (
    ModeOrientationAccompagnateur,
    ModeOrientationBeneficiaire,
)
from data_inclusion.schema.profils import Profil
from data_inclusion.schema.thematiques import Thematique
from data_inclusion.schema.typologies_de_services import TypologieService
from data_inclusion.schema.zones_de_diffusion import ZoneDiffusionType


class Service(BaseModel):
    # fields
    id: str
    structure_id: str
    source: str
    nom: str
    presentation_resume: Optional[Annotated[str, StringConstraints(max_length=280)]] = (
        None
    )
    presentation_detail: Optional[str] = None
    types: Optional[set[TypologieService]] = None
    thematiques: Optional[set[Thematique]] = None
    prise_rdv: Optional[HttpUrl] = None
    frais: Optional[set[Frais]] = None
    frais_autres: Optional[str] = None
    profils: Optional[set[Profil]] = None
    profils_precisions: Optional[str] = None
    pre_requis: Optional[set[str]] = None
    cumulable: Optional[bool] = None
    justificatifs: Optional[set[str]] = None
    formulaire_en_ligne: Optional[HttpUrl] = None
    commune: Optional[str] = None
    code_postal: Optional[common.CodePostal] = None
    code_insee: Optional[common.CodeCommune] = None
    adresse: Optional[str] = None
    complement_adresse: Optional[str] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    recurrence: Optional[str] = None
    date_creation: Optional[date] = None
    date_suspension: Optional[date] = None
    lien_source: Optional[HttpUrl] = None
    telephone: Optional[str] = None
    courriel: Optional[EmailStr] = None
    contact_public: Optional[bool] = None
    date_maj: Optional[date | datetime] = None
    modes_accueil: Optional[set[ModeAccueil]] = None
    zone_diffusion_type: Optional[ZoneDiffusionType] = None
    zone_diffusion_code: Optional[
        common.CodeCommune
        | common.CodeEPCI
        | common.CodeDepartement
        | common.CodeRegion
    ] = None
    zone_diffusion_nom: Optional[str] = None
    contact_nom_prenom: Optional[str] = None
    page_web: Annotated[
        Optional[HttpUrl],
        Field(
            description="""
                Lien vers une page web dédiée au service sur le site web de la
                structure. Ce champ n'est pas destiné à recevoir un lien vers le site
                d'un producteur de donnée.
            """,
            examples=[
                "https://insersol.fr/biclou-atelier-reparation-participatif-solidaire/"
            ],
        ),
    ] = None
    modes_orientation_beneficiaire: Optional[set[ModeOrientationBeneficiaire]] = None
    modes_orientation_beneficiaire_autres: Optional[str] = None
    modes_orientation_accompagnateur: Optional[set[ModeOrientationAccompagnateur]] = (
        None
    )
    modes_orientation_accompagnateur_autres: Optional[str] = None
