from furl import furl

from django.contrib import admin
from django.urls import reverse
from django.utils import html, text

from matching import models


@admin.register(models.Datasource)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ["id", "name"]


@admin.register(models.Stream)
class StreamAdmin(admin.ModelAdmin):
    list_display = ["id", "name"]


@admin.register(models.Matching)
class MatchingAdmin(admin.ModelAdmin):
    list_display = ["id"]
