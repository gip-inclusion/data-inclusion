import uuid

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


class DatasetRowFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = models.DatasetRow

    dataset = factory.SubFactory(DatasetFactory)
    data = factory.LazyFunction(lambda: {"id": str(uuid.uuid4())})


class AnnotationFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = models.Annotation

    siret = factory.LazyFunction(lambda: fake.siret().replace(" ", ""))
    row = factory.SubFactory(DatasetRowFactory)
