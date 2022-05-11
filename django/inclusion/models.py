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


class Structure(BaseModel):
    siret = CharField(max_length=14, blank=True, null=True, unique=True)
    rna = CharField(max_length=10, blank=True, null=True, unique=True)

    class Meta:
        constraints = [
            models.CheckConstraint(
                name="has_pivot",
                check=~models.Q(siret__isnull=True, rna__isnull=True),
            )
        ]

    def __str__(self) -> str:
        return self.siret

    objects = managers.StructureManager()

    def clean(self):
        super().clean()
        # Ensure at least one pivot has been set
        if self.siret is None and self.rna is None:
            raise exceptions.ValidationError("Précisez au moins un pivot (siret ou rna)")


class StructureTypology(BaseModel):
    value = CharField(max_length=255, unique=True, db_index=True)
    label = CharField(max_length=255)

    def __str__(self) -> str:
        return self.label


class StructureReport(BaseModel):
    # métadonnées

    structure = ForeignKey(Structure, on_delete=models.CASCADE, related_name="reports")

    # données de structure, selon le schéma de données normalisé

    id_antenne = CharField(max_length=5, blank=True, default="")
    structure_mere = ForeignKey("self", on_delete=models.CASCADE, blank=True, null=True, related_name="branches")
    typologie = ForeignKey(StructureTypology, null=True, blank=True, on_delete=models.PROTECT, related_name="reports")
    nom = CharField(blank=True, default="", max_length=255)
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
    # valeur indiquant la pertinence des valeurs lat/lon issues d'un géocodage
    # valeur allant de 0 (pas pertinent) à 1 (pertinent)
    score_geocodage = FloatField(blank=True, null=True)
    source = TextField(blank=True, default="")
    date_maj = DateTimeField(blank=True, null=True)
    extra = JSONField(blank=True, default=dict)

    def __str__(self) -> str:
        return f"{self.id}"

    objects = managers.StructureReportManager()
