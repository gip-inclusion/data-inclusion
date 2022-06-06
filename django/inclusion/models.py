from django.core import exceptions
from django.db import models
from django.db.models import (
    CharField,
    DateTimeField,
    EmailField,
    FloatField,
    ForeignKey,
    JSONField,
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
    telephone = CharField(blank=True, default="", max_length=10)
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

    # valeur indiquant la pertinence des valeurs lat/lon issues d'un géocodage
    # valeur allant de 0 (pas pertinent) à 1 (pertinent)
    score_geocodage = FloatField(blank=True, null=True)
    extra = JSONField(blank=True, default=dict)

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
