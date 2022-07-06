from django_filters import rest_framework as filters

from inclusion import models


class StructureReportFilterSet(filters.FilterSet):
    is_antenne = filters.BooleanFilter(
        field_name="parent_report",
        lookup_expr="isnull",
        label="Is antenne",
    )
    typologie = filters.ModelMultipleChoiceFilter(
        field_name="typologie__value",
        to_field_name="value",
        queryset=models.StructureTypology.objects.all(),
    )

    class Meta:
        model = models.StructureReport
        fields = ["siret", "rna", "source", "is_antenne", "typologie"]
