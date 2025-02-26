from datetime import date

import factory
import faker

from data_inclusion.api.v0.inclusion_data import models, schemas

fake = faker.Faker("fr_FR")


class StructureFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = models.Structure
        sqlalchemy_session_persistence = "commit"

    _di_surrogate_id = factory.Faker("uuid4")

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
            schemas.Typologie.ACI,
            schemas.Typologie.MUNI,
            schemas.Typologie.FT,
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
            schemas.Thematique.CHOISIR_UN_METIER,
            schemas.Thematique.CREATION_ACTIVITE,
            schemas.Thematique.MOBILITE,
        ],
        getter=lambda v: [v.value],
    )


class ServiceFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = models.Service
        sqlalchemy_session_persistence = "commit"

    _di_surrogate_id = factory.Faker("uuid4")

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
            schemas.TypologieService.FORMATION,
            schemas.TypologieService.NUMÉRIQUE,
        ],
        getter=lambda v: [v.value],
    )
    thematiques = factory.Iterator(
        [
            schemas.Thematique.CHOISIR_UN_METIER,
            schemas.Thematique.CREATION_ACTIVITE,
            schemas.Thematique.MOBILITE,
        ],
        getter=lambda v: [v.value],
    )
    prise_rdv = factory.Faker("url", locale="fr_FR")
    frais = factory.Iterator(
        [
            schemas.Frais.GRATUIT,
        ],
        getter=lambda v: [v.value],
    )
    frais_autres = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    page_web = factory.Faker("url", locale="fr_FR")
    profils = factory.Iterator(
        [
            schemas.Profil.FEMMES,
            schemas.Profil.JEUNES_16_26,
            schemas.Profil.SENIORS_65,
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
            schemas.ModeAccueil.A_DISTANCE,
            schemas.ModeAccueil.EN_PRESENTIEL,
            schemas.ModeAccueil.EN_PRESENTIEL,
        ],
        getter=lambda v: [v.value],
    )
    modes_orientation_accompagnateur = factory.Iterator(
        [
            schemas.ModeOrientationAccompagnateur.TELEPHONER,
            schemas.ModeOrientationAccompagnateur.ENVOYER_UN_MAIL,
        ],
        getter=lambda v: [v.value],
    )
    modes_orientation_accompagnateur_autres = None
    modes_orientation_beneficiaire = factory.Iterator(
        [
            schemas.ModeOrientationBeneficiaire.TELEPHONER,
            schemas.ModeOrientationBeneficiaire.SE_PRESENTER,
        ],
        getter=lambda v: [v.value],
    )
    modes_orientation_beneficiaire_autres = None
    zone_diffusion_type = None
    zone_diffusion_code = None
    zone_diffusion_nom = None

    score_qualite = 0.5
