import factory
import faker

from annotation import models

fake = faker.Faker("fr_FR")


class GroupFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = models.Group

    name = factory.Faker("slug", locale="fr_FR")


class DatasetFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = models.Dataset

    organization = factory.SubFactory(GroupFactory)
