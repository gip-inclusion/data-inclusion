from rest_framework import viewsets

from inclusion import models
from inclusion.api import filters, serializers


class StructureViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = models.Structure.objects.with_latest_reports()
    serializer_class = serializers.StructureSerializer
    ordering_fields = ["created_at"]
    ordering = ["created_at"]
    filterset_class = filters.StructureFilterSet
