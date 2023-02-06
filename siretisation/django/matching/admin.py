from matching import models

from django.contrib import admin


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
        "no_matching_row",
        "created_by",
    ]
    list_filter = ["left_stream", "right_stream", "created_by", "skipped", "no_matching_row"]
    search_fields = ["left_row_data", "right_row_data"]
    ordering = ["-created_at"]
