from django.db import models
from django.db.models import F, Max, OuterRef, Subquery
from django.db.models.lookups import Exact


class StructureReportQuerySet(models.QuerySet):
    def with_is_latest_by_source(self):
        return self.alias(
            latest=Subquery(
                self.values("source", "id_in_source")
                .annotate(latest=Max("created_at"))
                .filter(latest=OuterRef("created_at"))
                .values("latest")
            )
        ).annotate(is_latest=Exact(F("latest"), F("created_at")))

    def latests_by_source(self):
        return self.alias(
            latest=Subquery(
                self.values("source", "id_in_source")
                .annotate(latest=Max("created_at"))
                .filter(latest=OuterRef("created_at"))
                .values("latest")
            )
        ).filter(created_at=F("latest"))

    def antennes(self):
        return self.filter(parent_report__isnull=False)

    def sieges(self):
        return self.filter(parent_report__isnull=True)


class StructureReportManager(models.Manager):
    def get_queryset(self):
        return StructureReportQuerySet(self.model)

    def with_is_latest_by_source(self):
        return self.get_queryset().with_is_latest_by_source()

    def latests_by_source(self):
        return self.get_queryset().latests_by_source()

    def antennes(self):
        return self.get_queryset().antennes()

    def sieges(self):
        return self.get_queryset().sieges()
