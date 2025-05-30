from datetime import date

import factory
import faker

from data_inclusion.api.inclusion_data import models
from data_inclusion.schema import v0 as di_schema

fake = faker.Faker("fr_FR")


class StructureFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = models.Structure
        sqlalchemy_session_persistence = "commit"

    _di_surrogate_id = factory.LazyAttribute(lambda o: f"{o.source}-{o.id}")

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
            di_schema.TypologieStructure.ACI,
            di_schema.TypologieStructure.MUNI,
            di_schema.TypologieStructure.FT,
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
    thematiques = factory.Iterator(
        [
            di_schema.Thematique.CHOISIR_UN_METIER,
            di_schema.Thematique.CREATION_ACTIVITE,
            di_schema.Thematique.MOBILITE,
        ],
        getter=lambda v: [v.value],
    )
    score_qualite = 0.0
    doublons = []


class ServiceFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = models.Service
        sqlalchemy_session_persistence = "commit"

    _di_surrogate_id = factory.Faker("uuid4")
    _di_structure_surrogate_id = factory.Faker("uuid4")

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
            di_schema.TypologieService.FORMATION,
            di_schema.TypologieService.NUMÉRIQUE,
        ],
        getter=lambda v: [v.value],
    )
    thematiques = factory.Iterator(
        [
            di_schema.Thematique.CHOISIR_UN_METIER,
            di_schema.Thematique.CREATION_ACTIVITE,
            di_schema.Thematique.MOBILITE,
        ],
        getter=lambda v: [v.value],
    )
    prise_rdv = factory.Faker("url", locale="fr_FR")
    frais = factory.Iterator(
        [
            di_schema.Frais.GRATUIT,
        ],
        getter=lambda v: [v.value],
    )
    frais_autres = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    page_web = factory.Faker("url", locale="fr_FR")
    profils = factory.Iterator(
        [
            di_schema.Profil.FEMMES,
            di_schema.Profil.JEUNES_16_26,
            di_schema.Profil.SENIORS_65,
        ],
        getter=lambda v: [v.value],
    )
    profils_precisions = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    pre_requis = []
    cumulable = False
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
    date_creation = factory.LazyFunction(lambda: date(2022, 1, 1))
    date_suspension = factory.LazyFunction(lambda: date(2054, 1, 1))
    lien_source = factory.LazyAttribute(lambda o: f"https://{o.source}.fr/{o.id}")
    telephone = "0102030405"
    courriel = factory.Faker("email", locale="fr_FR")
    contact_public = None
    contact_nom_prenom = factory.Faker("name", locale="fr_FR")
    date_maj = factory.LazyFunction(lambda: date(2023, 1, 1))
    modes_accueil = factory.Iterator(
        [
            di_schema.ModeAccueil.A_DISTANCE,
            di_schema.ModeAccueil.EN_PRESENTIEL,
            di_schema.ModeAccueil.EN_PRESENTIEL,
        ],
        getter=lambda v: [v.value],
    )
    modes_orientation_accompagnateur = factory.Iterator(
        [
            di_schema.ModeOrientationAccompagnateur.TELEPHONER,
            di_schema.ModeOrientationAccompagnateur.ENVOYER_UN_MAIL,
        ],
        getter=lambda v: [v.value],
    )
    modes_orientation_accompagnateur_autres = None
    modes_orientation_beneficiaire = factory.Iterator(
        [
            di_schema.ModeOrientationBeneficiaire.TELEPHONER,
            di_schema.ModeOrientationBeneficiaire.SE_PRESENTER,
        ],
        getter=lambda v: [v.value],
    )
    modes_orientation_beneficiaire_autres = None
    zone_diffusion_type = None
    zone_diffusion_code = None
    zone_diffusion_nom = None

    score_qualite = 0.5
