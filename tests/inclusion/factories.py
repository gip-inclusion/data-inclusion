from datetime import datetime
from itertools import tee

import factory
import faker
import pytz

from data_inclusion.api import models, schema

fake = faker.Faker("fr_FR")


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


class StructureFactory(factory.Factory):
    class Meta:
        model = models.Structure

    id = factory.Faker("slug", locale="fr_FR")
    siret = factory.LazyFunction(lambda: fake.siret().replace(" ", ""))
    rna = factory.Faker("bothify", text="W#########")
    nom = factory.Faker("company", locale="fr_FR")
    commune = factory.Faker("city", locale="fr_FR")
    code_postal = factory.Faker("postcode", locale="fr_FR")
    code_insee = factory.Faker("postcode", locale="fr_FR")
    adresse = factory.Faker("street_address", locale="fr_FR")
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")
    typologie = factory.Iterator(schema.Typologie)
    telephone = "0102030405"
    courriel = factory.Faker("email", locale="fr_FR")
    site_web = factory.Faker("url", locale="fr_FR")
    presentation_resume = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    presentation_detail = factory.Faker("text", max_nb_chars=30, locale="fr_FR")
    source = factory.Iterator(["dora", "itou"])
    date_maj = factory.LazyFunction(lambda: datetime.now(tz=pytz.UTC))
    lien_source = factory.LazyAttribute(lambda o: f"https://{o.source}.fr/{o.id}")
    horaires_ouverture = 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off'
    accessibilite = factory.LazyAttribute(
        lambda o: f"https://acceslibre.beta.gouv.fr/app/{o.id}/"
    )
    labels_nationaux = []
    labels_autres = ["SudLabs", "Nièvre médiation numérique"]
    thematiques = factory.Iterator(
        pairwise(schema.Thematique),
        getter=lambda l: list(map(lambda t: t.value, l)),
    )
