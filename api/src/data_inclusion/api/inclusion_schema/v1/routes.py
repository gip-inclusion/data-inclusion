import fastapi

from data_inclusion.api.inclusion_schema.schemas import FrameworkValue
from data_inclusion.schema import v1

router = fastapi.APIRouter(tags=["v1 | Documentation"])

for name, model, summary in [
    ("frais", v1.Frais, "Récupérer le référentiel des frais"),
    (
        "labels-nationaux",
        v1.LabelNational,
        "Récupérer le référentiel des labels nationaux",
    ),
    ("modes-accueil", v1.ModeAccueil, "Récupérer le référentiel des modes d'accueil"),
    (
        "modes-orientation-accompagnateur",
        v1.ModeOrientationAccompagnateur,
        "Récupérer le référentiel des modes d'orientation de l'accompagnateur",
    ),
    (
        "modes-orientation-beneficiaire",
        v1.ModeOrientationBeneficiaire,
        "Récupérer le référentiel des modes d'orientation du bénéficiaire",
    ),
    ("profils", v1.Profil, "Récupérer le référentiel des profils de publics"),
    ("thematiques", v1.Thematique, "Récupérer le référentiel des thématiques"),
    (
        "typologies-services",
        v1.TypologieService,
        "Récupérer le référentiel des typologies de services",
    ),
    (
        "typologies-structures",
        v1.TypologieStructure,
        "Récupérer le référentiel des typologies de structures",
    ),
]:
    router.add_api_route(
        path=f"/{name}",
        endpoint=lambda: model.as_dict_list(),
        response_model=list[FrameworkValue],
        summary=summary,
    )
