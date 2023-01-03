from furl import furl

from django.contrib import admin
from django.urls import reverse
from django.utils import html, text

from annotation import models


@admin.register(models.Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ["id", "organization_name", "label", "slug", "link"]

    def organization_name(self, obj):
        return obj.organization.name

    def link(self, obj):
        url = furl(reverse("index"))
        url = url.set({"dataset": obj.slug})
        return html.format_html(
            "<a target='_blank' href='{url}'>Open 🔗</a>",
            url=url,
            slug=obj.slug,
        )


@admin.register(models.DatasetRow)
class DatasetRowAdmin(admin.ModelAdmin):
    list_display = ["id", "name"]
    list_filter = ["dataset"]
    search_fields = ["data__nom"]
    ordering = ["-created_at"]

    def name(self, obj):
        return text.Truncator(obj.data["nom"]).chars(80)


@admin.register(models.Annotation)
class AnnotationAdmin(admin.ModelAdmin):
    list_display = ["id", "name", "siret", "created_at", "is_parent", "closed", "skipped", "created_by"]
    list_filter = ["is_parent", "closed", "skipped", "created_by", "row__dataset"]
    search_fields = ["siret", "row__data__nom"]
    ordering = ["-created_at"]

    def name(self, obj):
        return text.Truncator(obj.row.data["nom"]).chars(80)
