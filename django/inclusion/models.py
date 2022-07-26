from django.contrib.postgres.fields import ArrayField
from django.core import exceptions
from django.db import models, transaction
from django.db.models import (
    BooleanField,
    CharField,
    DateTimeField,
    EmailField,
    FloatField,
    ForeignKey,
    JSONField,
    ManyToManyField,
    TextField,
    URLField,
)

from common.models import BaseModel
from inclusion import managers


class StructureTypology(BaseModel):
    value = CharField(max_length=255, unique=True, db_index=True)
    label = CharField(max_length=255)

    def __str__(self) -> str:
        return self.label


class StructureLabel(BaseModel):
    value = CharField(max_length=255, unique=True, db_index=True)
    label = CharField(max_length=255)


class StructureReport(BaseModel):
    # autre rapport dans lequel le rapport considéré a été déclaré
    # c'est le cas notamment pour documentés des antennes qui ne sont pas directement
    # rattachables à une structure (pivots inexistants)
    parent_report = ForeignKey(
        "StructureReport",
        blank=True,
        null=True,
        on_delete=models.CASCADE,
        related_name="sub_reports",
    )

    # identifiant de la structure dans la source
    # utilisé notamment pour y rattacher des données d'antennes
    id_in_source = TextField(blank=True, null=True)

    # champs définis dans le schéma standard
    typologie = ForeignKey(
        StructureTypology,
        blank=True,
        null=True,
        on_delete=models.PROTECT,
        related_name="reports",
    )
    nom = CharField(blank=True, default="", max_length=255)
    siret = CharField(blank=True, null=True, max_length=14)
    rna = CharField(blank=True, null=True, max_length=10)
    presentation_resume = CharField(blank=True, default="", max_length=280)
    site_web = URLField(blank=True, default="")
    presentation_detail = TextField(blank=True, default="")
    telephone = TextField(blank=True, default="")
    courriel = EmailField(blank=True, default="")
    code_postal = CharField(blank=True, default="", max_length=5)
    code_insee = CharField(blank=True, default="", max_length=5)
    commune = CharField(blank=True, default="", max_length=255)
    adresse = CharField(blank=True, default="", max_length=255)
    complement_adresse = CharField(blank=True, default="", max_length=255)
    longitude = FloatField(blank=True, null=True)
    latitude = FloatField(blank=True, null=True)
    source = TextField(blank=True, default="")
    date_maj = DateTimeField(blank=True, null=True)
    lien_source = URLField(blank=True, default="")
    horaires_ouverture = TextField(blank=True, default="")
    accessibilite = TextField(blank=True, default="")
    labels_nationaux = ManyToManyField(StructureLabel, related_name="reports")
    labels_autres = ArrayField(TextField(), default=list)

    # valeur indiquant la pertinence des valeurs lat/lon issues d'un géocodage
    # valeur allant de 0 (pas pertinent) à 1 (pertinent)
    score_geocodage = FloatField(blank=True, null=True)
    extra = JSONField(blank=True, default=dict)

    # champs dénormalisé pour accélerer la récupération des dernières données remontées
    is_latest = BooleanField(default=False)

    class Meta:
        constraints = [
            models.CheckConstraint(
                name="not_orphaned",
                check=~models.Q(
                    siret__isnull=True,
                    rna__isnull=True,
                    parent_report__isnull=True,
                ),
            )
        ]

    def __str__(self) -> str:
        return f"{self.id}"

    objects = managers.StructureReportManager()

    def clean(self):
        super().clean()
        if self.siret is None and self.rna is None and self.parent_report is None:
            raise exceptions.ValidationError(
                "la donnée ne peut ni être rattachée à une structure, ni inclue comme une antenne"
            )

    def save(self, *args, **kwargs):
        if self._state.adding:
            with transaction.atomic():
                # mise à jour du champs is_latest

                latest_report_instance = StructureReport.objects.filter(
                    source=self.source,
                    id_in_source=self.id_in_source,
                    is_latest=True,
                ).first()

                if latest_report_instance is None:
                    # 1er signalement pour ce couple source/structure
                    self.is_latest = True
                elif latest_report_instance.date_maj <= self.date_maj:
                    # cette donnée est la plus récente
                    latest_report_instance.is_latest = False
                    latest_report_instance.save()
                    self.is_latest = True
                else:
                    # cette donnée n'est pas la plus récente
                    self.is_latest = False

        super().save(*args, **kwargs)
