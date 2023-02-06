from django.contrib import admin

from matching import models


@admin.register(models.Datasource)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ["id", "name"]


@admin.register(models.Stream)
class StreamAdmin(admin.ModelAdmin):
    list_display = ["id", "name"]


@admin.register(models.Matching)
class MatchingAdmin(admin.ModelAdmin):
    list_display = [
        "id",
        "left_stream",
        "left_row_natural_id",
        "right_stream",
        "right_row_natural_id",
        "skipped",
        "created_by",
    ]
    list_filter = ["left_stream", "right_stream", "created_by", "skipped"]
    search_fields = ["left_row_data", "right_row_data"]
    ordering = ["-created_at"]
