from datetime import date
from itertools import tee

import factory
import faker

from data_inclusion.api import models, schema

fake = faker.Faker("fr_FR")


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


class CommuneFactory(factory.Factory):
    class Meta:
        model = models.Commune


class StructureFactory(factory.Factory):
    class Meta:
        model = models.Structure

    _di_surrogate_id = factory.Faker("uuid4")
    _di_geocodage_code_insee = factory.Faker("postcode")
    _di_geocodage_score = factory.Faker(
        "pyfloat", right_digits=2, positive=True, max_value=1
    )

    id = factory.Faker("slug", locale="fr_FR")
    siret = factory.LazyFunction(lambda: fake.siret().replace(" ", ""))
    rna = factory.Faker("bothify", text="W#########")
    nom = factory.Faker("company", locale="fr_FR")
    commune = factory.Faker("city", locale="fr_FR")
    code_postal = factory.Faker("postcode")
    code_insee = factory.Faker("postcode")
    adresse = factory.Faker("street_address", locale="fr_FR")
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")
    typologie = factory.Iterator(
        [
            schema.Typologie.ACI,
            schema.Typologie.MUNI,
            schema.Typologie.PE,
        ],
        getter=lambda t: t.value,
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
    labels_autres = ["SudLabs", "Nièvre médiation numérique"]
    thematiques = factory.Iterator(
        pairwise(
            [
                schema.Thematique.CHOISIR_UN_METIER,
                schema.Thematique.CREATION_ACTIVITE,
                schema.Thematique.MOBILITE,
            ]
        ),
        getter=lambda l: list(map(lambda t: t.value, l)),
    )


class ServiceFactory(factory.Factory):
    class Meta:
        model = models.Service

    _di_surrogate_id = factory.Faker("uuid4")
    _di_geocodage_code_insee = factory.Faker("postcode")
    _di_geocodage_score = factory.Faker(
        "pyfloat", right_digits=2, positive=True, max_value=1
    )

    structure = factory.SubFactory(StructureFactory)
    id = factory.Faker("slug", locale="fr_FR")
    structure_id = factory.SelfAttribute("structure.id")
    source = factory.SelfAttribute("structure.source")
    nom = factory.Faker("company", locale="fr_FR")
    presentation_resume = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    presentation_detail = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    types = factory.Iterator(
        pairwise(
            [
                schema.TypologieService.FORMATION,
                schema.TypologieService.NUMÉRIQUE,
            ]
        ),
        getter=lambda l: list(map(lambda t: t.value, l)),
    )
    thematiques = factory.Iterator(
        pairwise(
            [
                schema.Thematique.CHOISIR_UN_METIER,
                schema.Thematique.CREATION_ACTIVITE,
                schema.Thematique.MOBILITE,
            ]
        ),
        getter=lambda l: list(map(lambda t: t.value, l)),
    )
    prise_rdv = factory.Faker("url", locale="fr_FR")
    frais = factory.Iterator(
        pairwise(schema.Frais),
        getter=lambda l: list(map(lambda t: t.value, l)),
    )
    frais_autres = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    profils = factory.Iterator(
        pairwise(
            [
                schema.Profil.FEMMES,
                schema.Profil.JEUNES_16_26,
                schema.Profil.SENIORS_65,
            ]
        ),
        getter=lambda l: list(map(lambda t: t.value, l)),
    )
    pre_requis = []
    cumulable = False
    justificatifs = []
    formulaire_en_ligne = None
    commune = factory.Faker("city", locale="fr_FR")
    code_postal = factory.Faker("postcode")
    code_insee = factory.Faker("postcode")
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
            [schema.ModeAccueil.A_DISTANCE.value],
            [schema.ModeAccueil.EN_PRESENTIEL.value],
            [
                schema.ModeAccueil.A_DISTANCE.value,
                schema.ModeAccueil.EN_PRESENTIEL.value,
            ],
        ]
    )
    modes_orientation_accompagnateur = factory.Iterator(
        [
            [schema.ModeOrientationAccompagnateur.TELEPHONER.value],
            [schema.ModeOrientationAccompagnateur.ENVOYER_UN_MAIL.value],
        ]
    )
    modes_orientation_accompagnateur_autres = None
    modes_orientation_beneficiaire = factory.Iterator(
        [
            [schema.ModeOrientationBeneficiaire.TELEPHONER.value],
            [schema.ModeOrientationBeneficiaire.SE_PRESENTER.value],
        ]
    )
    modes_orientation_beneficiaire_autres = None
    zone_diffusion_type = None
    zone_diffusion_code = None
    zone_diffusion_nom = None
