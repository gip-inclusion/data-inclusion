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
        "modes-mobilisation",
        v1.ModeMobilisation,
        "Récupérer le référentiel des modes de mobilisation",
    ),
    (
        "personnes-mobilisatrice",
        v1.PersonneMobilisatrice,
        "Récupérer le référentiel des personnes mobilisatrices",
    ),
    ("publics", v1.Public, "Récupérer le référentiel des publics"),
    ("thematiques", v1.Thematique, "Récupérer le référentiel des thématiques"),
    (
        "types-services",
        v1.TypeService,
        "Récupérer le référentiel des types de services",
    ),
    (
        "typologies-structures",
        v1.TypologieStructure,
        "Récupérer le référentiel des typologies de structures",
    ),
]:
    router.add_api_route(
        path=f"/{name}",
        endpoint=model.as_dict_list,
        response_model=list[FrameworkValue],
        summary=summary,
    )
