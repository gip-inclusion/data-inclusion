from rest_framework import serializers

from inclusion import models


class StructureReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.StructureReport
        fields = "__all__"


class StructureSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Structure
        fields = "__all__"

    latest_reports = StructureReportSerializer(many=True, default=[])
