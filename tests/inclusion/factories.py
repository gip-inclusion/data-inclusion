import factory
import faker

from django.conf import settings
from django.utils import timezone

fake = faker.Faker("fr_FR")


class StructureFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = "inclusion.Structure"

    siret = factory.LazyFunction(lambda: fake.siret().replace(" ", ""))
    name = factory.Faker("company", locale="fr_FR")
    ape = "84.11Z"
    address1 = factory.Faker("street_address", locale="fr_FR")
    city = factory.Faker("city", locale="fr_FR")
    city_code = factory.Faker("postcode", locale="fr_FR")
    postal_code = factory.Faker("postcode", locale="fr_FR")
    is_siege = True
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")


class UserFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = settings.AUTH_USER_MODEL

    id = factory.Sequence(lambda n: n)


class StructureReportFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = "inclusion.StructureReport"

    structure = factory.SubFactory(StructureFactory)
    reporter = factory.SubFactory(UserFactory)

    nom = factory.Faker("company", locale="fr_FR")
    presentation_resume = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    site_web = factory.Faker("url", locale="fr_FR")
    presentation_detail = factory.Faker("text", max_nb_chars=30, locale="fr_FR")
    telephone = "0102030405"
    courriel = factory.Faker("email", locale="fr_FR")
    code_postal = factory.Faker("postcode", locale="fr_FR")
    code_insee = factory.Faker("postcode", locale="fr_FR")
    commune = factory.Faker("city", locale="fr_FR")
    departement = factory.Faker("department_number", locale="fr_FR")
    adresse = factory.Faker("street_address", locale="fr_FR")
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")
    score_geocodage = 0.5
    date_maj = factory.LazyFunction(timezone.now)
