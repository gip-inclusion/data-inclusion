import factory
import faker

from django.conf import settings
from django.utils import timezone

from inclusion import models

fake = faker.Faker("fr_FR")


class UserFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = settings.AUTH_USER_MODEL

    id = factory.Sequence(lambda n: n)


class StructureReportFactory(factory.django.DjangoModelFactory):
    class Meta:
        model = models.StructureReport

    id_in_source = factory.Faker("slug", locale="fr_FR")
    typologie = factory.Iterator(models.StructureTypology.objects.all())

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
    horaires_ouverture = 'Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off'
    accessibilite = factory.LazyAttribute(lambda o: f"https://acceslibre.beta.gouv.fr/app/{o.id_in_source}/")
    labels_autres = ["SudLabs", "Nièvre médiation numérique"]

    @factory.post_generation
    def labels_nationaux(self, create, extracted, **kwargs):
        if not create:
            return

        if extracted is not None:
            self.labels_nationaux.add(*extracted)
        else:
            self.labels_nationaux.add(models.StructureLabel.objects.order_by("value").first())
