from datetime import date

import factory
import faker

from data_inclusion.api.inclusion_data.v1 import models
from data_inclusion.schema import v1

fake = faker.Faker("fr_FR")


class StructureFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = models.Structure
        sqlalchemy_session_persistence = "commit"
        # attributes starting with an underscore ignored by default
        # the recommended way is to use the `rename` dict
        rename = {
            "di_surrogate_id": "_di_surrogate_id",
            "is_best_duplicate": "_is_best_duplicate",
            "cluster_id": "_cluster_id",
        }

    di_surrogate_id = factory.LazyAttribute(lambda o: f"{o.source}-{o.id}")
    is_best_duplicate = None
    cluster_id = None

    id = factory.Faker("slug", locale="fr_FR")
    siret = factory.LazyFunction(lambda: fake.siret().replace(" ", ""))
    rna = factory.Faker("bothify", text="W#########")
    nom = factory.Faker("company", locale="fr_FR")
    commune = factory.Faker("city", locale="fr_FR")
    code_postal = factory.Faker("postcode")
    code_insee = "59350"
    adresse = factory.Faker("street_address", locale="fr_FR")
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")
    typologie = factory.Iterator(
        [
            v1.TypologieStructure.ACI,
            v1.TypologieStructure.MUNI,
            v1.TypologieStructure.FT,
        ],
        getter=lambda v: v.value,
    )
    telephone = "0102030405"
    courriel = factory.Faker("email", locale="fr_FR")
    site_web = factory.Faker("url", locale="fr_FR")
    presentation_resume = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    presentation_detail = factory.Faker("text", max_nb_chars=30, locale="fr_FR")
    source = factory.Iterator(["dora", "emplois-de-linclusion"])
    date_maj = factory.LazyFunction(lambda: date(2023, 1, 1))
    lien_source = factory.LazyAttribute(lambda o: f"https://{o.source}.fr/{o.id}")
    horaires_ouverture = 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off'
    accessibilite = factory.LazyAttribute(
        lambda o: f"https://acceslibre.beta.gouv.fr/app/{o.id}/"
    )
    labels_nationaux = []
    labels_autres = ["Nièvre médiation numérique"]

    score_qualite = 0.0


class ServiceFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = models.Service
        sqlalchemy_session_persistence = "commit"
        # attributes starting with an underscore ignored by default
        # the recommended way is to use the `rename` dict
        rename = {
            "di_surrogate_id": "_di_surrogate_id",
            "di_structure_surrogate_id": "_di_structure_surrogate_id",
        }

    di_surrogate_id = factory.LazyAttribute(lambda o: f"{o.source}-{o.id}")
    di_structure_surrogate_id = factory.SelfAttribute("structure._di_surrogate_id")

    structure = factory.SubFactory(
        StructureFactory,
        source=factory.SelfAttribute("..source"),
        code_insee=factory.SelfAttribute("..code_insee"),
    )
    id = factory.Faker("slug", locale="fr_FR")
    structure_id = factory.SelfAttribute("structure.id")
    source = factory.Iterator(["dora", "emplois-de-linclusion"])
    nom = factory.Faker("company", locale="fr_FR")
    presentation_resume = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    presentation_detail = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    types = factory.Iterator(
        [
            v1.TypologieService.FORMATION,
            v1.TypologieService.NUMÉRIQUE,
        ],
        getter=lambda v: [v.value],
    )
    thematiques = factory.Iterator(
        [
            v1.Thematique.CHOISIR_UN_METIER,
            v1.Thematique.CREATION_ACTIVITE,
            v1.Thematique.MOBILITE,
        ],
        getter=lambda v: [v.value],
    )
    prise_rdv = factory.Faker("url", locale="fr_FR")
    frais = factory.Iterator(
        [
            v1.Frais.GRATUIT,
        ],
        getter=lambda v: [v.value],
    )
    frais_autres = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    page_web = factory.Faker("url", locale="fr_FR")
    profils = factory.Iterator(
        [
            v1.Profil.FEMMES,
            v1.Profil.JEUNES_16_26,
            v1.Profil.SENIORS_65,
        ],
        getter=lambda v: [v.value],
    )
    profils_precisions = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    pre_requis = []
    justificatifs = []
    formulaire_en_ligne = None
    commune = factory.Faker("city", locale="fr_FR")
    code_postal = factory.Faker("postcode")
    code_insee = "59350"
    adresse = factory.Faker("street_address", locale="fr_FR")
    complement_adresse = None
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")
    recurrence = None
    lien_source = factory.LazyAttribute(lambda o: f"https://{o.source}.fr/{o.id}")
    telephone = "0102030405"
    courriel = factory.Faker("email", locale="fr_FR")
    contact_nom_prenom = factory.Faker("name", locale="fr_FR")
    date_maj = factory.LazyFunction(lambda: date(2023, 1, 1))
    modes_accueil = factory.Iterator(
        [
            v1.ModeAccueil.A_DISTANCE,
            v1.ModeAccueil.EN_PRESENTIEL,
            v1.ModeAccueil.EN_PRESENTIEL,
        ],
        getter=lambda v: [v.value],
    )
    modes_orientation_accompagnateur = factory.Iterator(
        [
            v1.ModeOrientationAccompagnateur.TELEPHONER,
            v1.ModeOrientationAccompagnateur.ENVOYER_UN_MAIL,
        ],
        getter=lambda v: [v.value],
    )
    modes_orientation_accompagnateur_autres = None
    modes_orientation_beneficiaire = factory.Iterator(
        [
            v1.ModeOrientationBeneficiaire.TELEPHONER,
            v1.ModeOrientationBeneficiaire.SE_PRESENTER,
        ],
        getter=lambda v: [v.value],
    )
    modes_orientation_beneficiaire_autres = None
    zone_diffusion_type = None
    zone_diffusion_code = None
    zone_diffusion_nom = None

    score_qualite = 0.5
