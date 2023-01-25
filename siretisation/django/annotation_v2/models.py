from django.db import models


class Datalake(models.Model):
    class Meta:
        # TODO: faire en sorte que la table se mette à jour tous les jours
        db_table = "datalake_2023_01_23"
        managed = False

    id = models.UUIDField(primary_key=True)
    created_at = models.DateTimeField()
    logical_date = models.DateField()
    batch_id = models.TextField()
    src_alias = models.TextField()
    src_url = models.TextField()
    file = models.TextField()
    data = models.JSONField()
    data_normalized = models.JSONField()
