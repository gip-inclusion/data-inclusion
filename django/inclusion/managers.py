from django.db import models
from django.db.models import F, Max, OuterRef, Prefetch, Subquery


class StructureManager(models.Manager):
    def with_latest_reports(self):
        from inclusion.models import StructureReport

        return self.prefetch_related(
            Prefetch(
                "reports",
                queryset=StructureReport.objects.latest_by_reporter(),
                to_attr="latest_reports",
            )
        )


class StructureReportManager(models.Manager):
    def latest_by_reporter(self):
        return self.alias(
            latest=Subquery(
                self.values("reporter")
                .annotate(latest=Max("created_at"))
                .filter(latest=OuterRef("created_at"))
                .values("latest")
            )
        ).filter(created_at=F("latest"))
