import factory
import faker

from django.conf import settings
from django.utils import timezone

fake = faker.Faker("fr_FR")


class UserFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = settings.AUTH_USER_MODEL

    id = factory.Sequence(lambda n: n)


class StructureReportFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = "inclusion.StructureReport"

    id_in_source = factory.Faker("slug", locale="fr_FR")

    siret = factory.LazyFunction(lambda: fake.siret().replace(" ", ""))
    rna = factory.Faker("bothify", text="W#########")

    nom = factory.Faker("company", locale="fr_FR")
    presentation_resume = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    site_web = factory.Faker("url", locale="fr_FR")
    presentation_detail = factory.Faker("text", max_nb_chars=30, locale="fr_FR")
    telephone = "0102030405"
    courriel = factory.Faker("email", locale="fr_FR")
    code_postal = factory.Faker("postcode", locale="fr_FR")
    code_insee = factory.Faker("postcode", locale="fr_FR")
    commune = factory.Faker("city", locale="fr_FR")
    adresse = factory.Faker("street_address", locale="fr_FR")
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")
    score_geocodage = 0.5
    date_maj = factory.LazyFunction(timezone.now)
    source = factory.Iterator(["dora", "itou"])
    lien_source = factory.LazyAttribute(lambda o: f"https://{o.source}.fr/{o.id_in_source}")
