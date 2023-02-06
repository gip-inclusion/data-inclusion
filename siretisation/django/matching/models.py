from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.utils import text

from common.models import BaseModel
from users.models import User


class Datalake(models.Model):
    class Meta:
        db_table = "datalake"
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


class Datasource(BaseModel):
    # should match src_alias
    name = models.TextField(unique=True)
    slug = models.SlugField(blank=True, default="")

    def save(self, *args, **kwargs):
        self.slug = text.slugify(self.name)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return self.name


class Stream(BaseModel):
    datasource = models.ForeignKey(Datasource, on_delete=models.PROTECT)
    name = models.TextField()
    slug = models.SlugField(blank=True, default="")
    selected_columns = ArrayField(models.TextField(), blank=True, default=list)

    def save(self, *args, **kwargs):
        self.slug = text.slugify(self.name)
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        return f"{self.datasource.name} | {self.name}"


class Matching(BaseModel):
    left_stream = models.ForeignKey(Stream, on_delete=models.PROTECT, related_name="left_matchings")
    right_stream = models.ForeignKey(Stream, on_delete=models.PROTECT, related_name="right_matchings")

    logical_date = models.DateField()

    left_row_natural_id = models.TextField()
    left_row_data = models.JSONField()

    right_row_natural_id = models.TextField(null=True)
    right_row_data = models.JSONField(null=True)

    skipped = models.BooleanField(default=False)
    no_matching_row = models.BooleanField(default=False)

    created_by = models.ForeignKey(User, on_delete=models.PROTECT)
