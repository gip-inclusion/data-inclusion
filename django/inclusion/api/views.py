from rest_framework import mixins, pagination, viewsets

from inclusion import models
from inclusion.api import filters, serializers


class StructureReportViewSet(
    mixins.CreateModelMixin,
    mixins.RetrieveModelMixin,
    mixins.ListModelMixin,
    viewsets.GenericViewSet,
):
    ordering_fields = ["created_at"]
    ordering = ["created_at"]
    filterset_class = filters.StructureReportFilterSet
    pagination_class = pagination.LimitOffsetPagination

    def get_queryset(self):
        if self.action == "create":
            return models.StructureReport.objects.all()
        elif self.action in ["list", "retrieve"]:
            return models.StructureReport.objects.latests_by_source()

    def get_serializer_class(self):
        if self.action == "create":
            return serializers.CreateStructureReportSerializer
        elif self.action in ["list", "retrieve"]:
            return serializers.StructureReportSerializer

    def create(self, request, *args, **kwargs):
        return super().create(request, *args, **kwargs)

    def list(self, request, *args, **kwargs):
        return super().list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        return super().retrieve(request, *args, **kwargs)
