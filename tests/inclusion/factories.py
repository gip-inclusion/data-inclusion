import factory
import faker

from django.conf import settings

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

    slug = factory.Faker("slug", locale="fr_FR")
    name = factory.Faker("company", locale="fr_FR")
    short_desc = factory.Faker("text", max_nb_chars=20, locale="fr_FR")
    url = factory.Faker("url", locale="fr_FR")
    full_desc = factory.Faker("text", max_nb_chars=30, locale="fr_FR")
    phone = "0102030405"
    email = factory.Faker("email", locale="fr_FR")
    postal_code = factory.Faker("postcode", locale="fr_FR")
    city_code = factory.Faker("postcode", locale="fr_FR")
    city = factory.Faker("city", locale="fr_FR")
    department = factory.Faker("department_number", locale="fr_FR")
    address1 = factory.Faker("street_address", locale="fr_FR")
    code_ape = "84.11Z"
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")
    geocoding_score = 0.5
