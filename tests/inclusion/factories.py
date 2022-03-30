import factory


class StructureFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = "inclusion.Structure"

    id = factory.Sequence(lambda n: f"structure_id_{n:04d}")
    siret = factory.Faker("siret", locale="fr_FR")
    name = factory.Faker("company", locale="fr_FR")
    ape = "84.11Z"
    address1 = factory.Faker("street_address", locale="fr_FR")
    city = factory.Faker("city", locale="fr_FR")
    city_code = factory.Faker("postcode", locale="fr_FR")
    postal_code = factory.Faker("postcode", locale="fr_FR")
    is_siege = True
    longitude = factory.Faker("longitude")
    latitude = factory.Faker("latitude")
