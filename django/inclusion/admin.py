from django.contrib import admin
from django.utils.translation import gettext_lazy as _

from inclusion import models


class LatestsFilter(admin.SimpleListFilter):
    title = _("latests by source")
    parameter_name = "latests"

    def lookups(self, request, mode_admin):
        return [
            (True, _("Yes")),
        ]

    def queryset(self, request, queryset):
        if self.value():
            return queryset.filter(is_latest=True)
        return queryset


@admin.register(models.StructureReport)
class StructureReport(admin.ModelAdmin):
    list_display = ["id", "source", "id_in_source", "siret", "date_maj", "nom"]
    list_filter = [LatestsFilter, "source", "typologie"]
    search_fields = ["siret", "nom"]

    def get_queryset(self, request):
        return self.model.objects.with_is_latest_by_source()
