import enum

from django.contrib.postgres.indexes import GinIndex
from django.db import models


class Establishment(models.Model):
    siret = models.CharField(verbose_name="Siret", max_length=14, primary_key=True)
    siren = models.CharField(verbose_name="Siren", max_length=9)
    name = models.CharField(verbose_name="Nom", max_length=255)

    ape = models.ForeignKey("CodeNAF", on_delete=models.PROTECT, related_name="establishments", null=True)
    address1 = models.CharField(max_length=255, blank=True)
    address2 = models.CharField(max_length=255)
    city = models.CharField(max_length=255)
    city_code = models.CharField(max_length=5, db_index=True)
    postal_code = models.CharField(max_length=5)
    is_siege = models.BooleanField()
    longitude = models.FloatField(blank=True, null=True)
    latitude = models.FloatField(blank=True, null=True)

    class Meta:
        indexes = [
            # https://www.postgresql.org/docs/current/pgtrgm.html#id-1.11.7.40.8
            GinIndex(
                name="full_text_name_trgm_idx",
                fields=("name",),
                opclasses=("gin_trgm_ops",),
            ),
            GinIndex(
                name="full_text_address_trgm_idx",
                fields=("address1",),
                opclasses=("gin_trgm_ops",),
            ),
        ]


class CodeNAF(models.Model):
    class Level(enum.Enum):
        SECTION = "section"
        DIVISION = "division"
        GROUP = "group"
        CLASS = "class"
        SUBCLASS = "subclass"

    code = models.TextField(max_length=6, primary_key=True)
    label = models.TextField()

    section_code = models.TextField()
    section_label = models.TextField()
    division_code = models.TextField()
    division_label = models.TextField()
    group_code = models.TextField()
    group_label = models.TextField()
    class_code = models.TextField()
    class_label = models.TextField()
