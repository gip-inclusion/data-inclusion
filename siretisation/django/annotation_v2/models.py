from django.db import models


class Datalake(models.Model):
    class Meta:
        db_table = "datalake_latest"
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
