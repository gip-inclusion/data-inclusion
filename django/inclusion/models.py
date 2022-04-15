from django.conf import settings
from django.db import models
from django.db.models import (
    BooleanField,
    CharField,
    DateTimeField,
    EmailField,
    FloatField,
    ForeignKey,
    JSONField,
    SlugField,
    TextField,
    URLField,
)

from common.models import BaseModel
from inclusion import managers


class Structure(BaseModel):
    siret = CharField(max_length=14, unique=True)
    code_safir_pe = CharField(max_length=5, null=True, blank=True, db_index=True)
    name = CharField(max_length=255)
    ape = CharField(max_length=6)
    address1 = CharField(max_length=255)
    address2 = CharField(max_length=255, blank=True, default="")
    city = CharField(max_length=255)
    city_code = CharField(max_length=5, db_index=True)
    postal_code = CharField(max_length=5)
    is_siege = BooleanField()
    longitude = FloatField(blank=True, null=True)
    latitude = FloatField(blank=True, null=True)

    def __str__(self) -> str:
        return self.siret

    objects = managers.StructureManager()


class StructureTypology(BaseModel):
    value = CharField(max_length=255, unique=True, db_index=True)
    label = CharField(max_length=255)

    def __str__(self) -> str:
        return self.label


class StructureReport(BaseModel):
    # métadonnées

    structure = ForeignKey(Structure, on_delete=models.CASCADE, related_name="reports")
    reporter = ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="reports")

    # données de structure, selon le schéma de données normalisé

    branch_id = CharField(max_length=5, blank=True, default="")
    parent = ForeignKey("self", on_delete=models.CASCADE, blank=True, null=True, related_name="branches")
    typology = ForeignKey(StructureTypology, null=True, blank=True, on_delete=models.PROTECT, related_name="reports")
    slug = SlugField(blank=True, default="")
    name = CharField(blank=True, default="", max_length=255)
    short_desc = CharField(blank=True, default="", max_length=280)
    url = URLField(blank=True, default="")
    full_desc = TextField(blank=True, default="")
    phone = TextField(blank=True, default="", max_length=10)
    email = EmailField(blank=True, default="")
    postal_code = CharField(blank=True, default="", max_length=5)
    city_code = CharField(blank=True, default="", max_length=5)
    city = CharField(blank=True, default="", max_length=255)
    department = CharField(blank=True, default="", max_length=3)
    address1 = CharField(blank=True, default="", max_length=255)
    address2 = CharField(blank=True, default="", max_length=255)
    code_ape = CharField(blank=True, default="", max_length=6)
    longitude = FloatField(blank=True, null=True)
    latitude = FloatField(blank=True, null=True)
    # valeur indiquant la pertinence des valeurs lat/lon issues d'un géocodage
    # valeur allant de 0 (pas pertinent) à 1 (pertinent)
    geocoding_score = FloatField(blank=True, null=True)
    source = TextField(blank=True, default="")
    creation_date = DateTimeField(blank=True, null=True)
    modification_date = DateTimeField(blank=True, null=True)
    extra = JSONField(blank=True, default=dict)

    def __str__(self) -> str:
        return f"{self.id}"

    objects = managers.StructureReportManager()
