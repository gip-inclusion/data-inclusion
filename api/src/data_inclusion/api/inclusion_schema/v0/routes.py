import fastapi

from data_inclusion.api.inclusion_schema.schemas import FrameworkValue
from data_inclusion.schema import v0

router = fastapi.APIRouter()

for name, model, summary in [
    ("frais", v0.Frais, "Récupérer le référentiel des frais"),
    (
        "labels-nationaux",
        v0.LabelNational,
        "Récupérer le référentiel des labels nationaux",
    ),
    ("modes-accueil", v0.ModeAccueil, "Récupérer le référentiel des modes d'accueil"),
    (
        "modes-orientation-accompagnateur",
        v0.ModeOrientationAccompagnateur,
        "Récupérer le référentiel des modes d'orientation de l'accompagnateur",
    ),
    (
        "modes-orientation-beneficiaire",
        v0.ModeOrientationBeneficiaire,
        "Récupérer le référentiel des modes d'orientation du bénéficiaire",
    ),
    ("profils", v0.Profil, "Récupérer le référentiel des profils de publics"),
    ("thematiques", v0.Thematique, "Récupérer le référentiel des thématiques"),
    (
        "typologies-services",
        v0.TypologieService,
        "Récupérer le référentiel des typologies de services",
    ),
    (
        "typologies-structures",
        v0.TypologieStructure,
        "Récupérer le référentiel des typologies de structures",
    ),
]:
    router.add_api_route(
        path=f"/{name}",
        endpoint=lambda: model.as_dict_list(),
        operation_id=f"list_{name.replace('-', '_')}_endpoint",
        response_model=list[FrameworkValue],
        summary=summary,
    )
