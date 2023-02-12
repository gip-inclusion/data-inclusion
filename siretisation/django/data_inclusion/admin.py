from django.contrib import admin

from data_inclusion import models


@admin.register(models.Structure)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ["di_surrogate_id", "nom", "siret"]
