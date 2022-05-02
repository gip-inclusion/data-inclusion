from rest_framework import mixins, pagination, status, viewsets
from rest_framework.response import Response

from inclusion import models
from inclusion.api import filters, serializers


class StructureViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = models.Structure.objects.with_latest_reports()
    serializer_class = serializers.StructureSerializer
    ordering_fields = ["created_at"]
    ordering = ["created_at"]
    filterset_class = filters.StructureFilterSet
    pagination_class = pagination.LimitOffsetPagination


class StructureReportViewSet(
    mixins.CreateModelMixin,
    viewsets.GenericViewSet,
):
    queryset = models.StructureReport.objects.all()
    serializer_class = serializers.CreateStructureReportSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        self.perform_create(serializer)
        headers = self.get_success_headers(serializer.data)

        # l'identifiant de la structure est utilisé dans ses représentations
        data = serializer.data
        data["id"] = serializer.instance.structure.id

        return Response(data, status=status.HTTP_201_CREATED, headers=headers)
