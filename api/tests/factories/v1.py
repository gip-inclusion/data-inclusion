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
            "cluster_id": "_cluster_id",
        }

    cluster_id = None

    id = factory.LazyAttribute(lambda o: f"{o.source}--{fake.slug()}")
    siret = factory.LazyFunction(lambda: fake.siret().replace(" ", ""))
    nom = factory.LazyFunction(lambda: fake.company().rstrip("."))
    commune = factory.Faker("city", locale="fr_FR")
    code_postal = factory.Faker("postcode")
    code_insee = "59350"
    adresse = factory.Faker("street_address", locale="fr_FR")
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")
    telephone = "0102030405"
    courriel = factory.Faker("email", locale="fr_FR")
    site_web = factory.Faker("url", locale="fr_FR")
    lien_source = factory.LazyAttribute(lambda o: f"https://{o.source}.fr/{o.id}")
    description = factory.Faker("sentence", nb_words=100, locale="fr_FR")
    source = factory.Iterator(["dora", "emplois-de-linclusion"])
    date_maj = factory.LazyFunction(lambda: date(2023, 1, 1))
    horaires_accueil = "Mo-Fr 10:00-20:00"
    accessibilite_lieu = factory.LazyAttribute(
        lambda o: f"https://acceslibre.beta.gouv.fr/app/{o.id}/"
    )
    reseaux_porteurs = []

    score_qualite = 0.0


class ServiceFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = models.Service
        sqlalchemy_session_persistence = "commit"

    structure = factory.SubFactory(
        StructureFactory,
        source=factory.SelfAttribute("..source"),
        code_insee=factory.SelfAttribute("..code_insee"),
    )
    id = factory.LazyAttribute(lambda o: f"{o.source}--{fake.slug()}")
    structure_id = factory.SelfAttribute("structure.id")
    source = factory.Iterator(["dora", "emplois-de-linclusion"])
    nom = factory.LazyFunction(lambda: fake.company().rstrip("."))
    description = factory.Faker("sentence", nb_words=100, locale="fr_FR")
    type = factory.Iterator(
        [
            v1.TypeService.FORMATION,
            v1.TypeService.ACCOMPAGNEMENT,
            v1.TypeService.INFORMATION,
        ],
    )
    thematiques = [v1.Thematique.FAMILLE__GARDE_DENFANTS.value]
    frais = factory.Iterator([v1.Frais.GRATUIT, v1.Frais.PAYANT])
    frais_precisions = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    horaires_accueil = "Mo-Fr 10:00-20:00"
    publics = factory.Iterator(
        [
            v1.Public.FEMMES,
            v1.Public.JEUNES,
            v1.Public.SENIORS,
        ],
        getter=lambda v: [v.value],
    )
    publics_precisions = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    commune = factory.Faker("city", locale="fr_FR")
    code_postal = factory.Faker("postcode")
    code_insee = "59350"
    conditions_acces = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    adresse = factory.Faker("street_address", locale="fr_FR")
    complement_adresse = None
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")
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
    lien_mobilisation = None
    lien_source = factory.LazyAttribute(lambda o: f"https://{o.source}.fr/{o.id}")
    modes_mobilisation = None
    mobilisation_precisions = None
    mobilisable_par = None
    zone_eligibilite = None
    nombre_semaines = 1
    volume_horaire_hebdomadaire = 1
    score_qualite = 0.5
    extra = None
