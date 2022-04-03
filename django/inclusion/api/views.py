from rest_framework import mixins, viewsets

from inclusion import models
from inclusion.api import filters, serializers


class StructureViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = models.Structure.objects.with_latest_reports()
    serializer_class = serializers.StructureSerializer
    ordering_fields = ["created_at"]
    ordering = ["created_at"]
    filterset_class = filters.StructureFilterSet


class StructureReportViewSet(
    mixins.CreateModelMixin,
    viewsets.GenericViewSet,
):
    queryset = models.StructureReport.objects.all()
    serializer_class = serializers.CreateStructureReportSerializer
