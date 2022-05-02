from django.contrib import admin

from inclusion import models


@admin.register(models.Structure)
class Structure(admin.ModelAdmin):
    list_display = ("id", "siret")


@admin.register(models.StructureReport)
class StructureReport(admin.ModelAdmin):
    list_display = ("id", "nom", "reporter")
