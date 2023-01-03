from django.contrib.gis.db.models import PointField
from django.contrib.postgres.indexes import GinIndex
from django.db import models


class Permanence(models.Model):
    id = models.TextField(primary_key=True)
    siret = models.TextField(db_index=True, null=True)
    name = models.TextField()
    address = models.TextField()
    city = models.TextField()
    postal_code = models.TextField()
    position = PointField()

    class Meta:
        indexes = [
            # https://www.postgresql.org/docs/current/pgtrgm.html#id-1.11.7.40.8
            GinIndex(
                name="cnfs_name_trgm_idx",
                fields=("name",),
                opclasses=("gin_trgm_ops",),
            ),
            GinIndex(
                name="cnfs_address_trgm_idx",
                fields=("address",),
                opclasses=("gin_trgm_ops",),
            ),
        ]
