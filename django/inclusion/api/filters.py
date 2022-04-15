from django_filters import rest_framework as filters

from inclusion import models


class StructureFilterSet(filters.FilterSet):
    class Meta:
        model = models.Structure
        fields = ["siret"]
