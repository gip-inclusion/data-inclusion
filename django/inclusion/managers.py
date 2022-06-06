from django.db import models


class StructureReportQuerySet(models.QuerySet):
    def latests_by_source(self):
        return self.filter(is_latest=True)

    def antennes(self):
        return self.filter(parent_report__isnull=False)

    def sieges(self):
        return self.filter(parent_report__isnull=True)


class StructureReportManager(models.Manager):
    def get_queryset(self):
        return StructureReportQuerySet(self.model)

    def latests_by_source(self):
        return self.get_queryset().latests_by_source()

    def antennes(self):
        return self.get_queryset().antennes()

    def sieges(self):
        return self.get_queryset().sieges()
