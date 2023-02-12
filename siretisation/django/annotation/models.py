from django.contrib.auth.models import Group
from django.db import models
from django.utils import text

from common.models import BaseModel
from users.models import User


class Dataset(BaseModel):
    label = models.CharField(max_length=100)
    source = models.CharField(max_length=50, default="")
    slug = models.SlugField(blank=True, default="")
    organization = models.ForeignKey(Group, on_delete=models.PROTECT, related_name="datasets")
    priority_settings = models.JSONField(blank=True, default=dict)
    show_nearby_cnfs_permanences = models.BooleanField(default=False)

    def __str__(self) -> str:
        return self.label

    def save(self, *args, **kwargs):
        self.slug = text.slugify(f"{self.organization.name} {self.label}")
        super().save(*args, **kwargs)


class Annotation(BaseModel):
    dataset = models.ForeignKey(Dataset, on_delete=models.PROTECT, related_name="annotations")
    di_surrogate_id = models.TextField(db_index=True)
    siret = models.CharField(max_length=14, blank=True, default="")

    skipped = models.BooleanField(default=False)
    closed = models.BooleanField(default=False)
    irrelevant = models.BooleanField(default=False)
    is_parent = models.BooleanField(default=False)

    created_by = models.ForeignKey(User, on_delete=models.PROTECT, null=True, blank=True)
