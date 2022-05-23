from django_filters import rest_framework as filters

from inclusion import models


class StructureReportFilterSet(filters.FilterSet):
    is_antenne = filters.BooleanFilter(field_name="parent_report", lookup_expr="isnull", label="Is antenne")

    class Meta:
        model = models.StructureReport
        fields = ["siret", "rna", "source", "typologie", "is_antenne"]
