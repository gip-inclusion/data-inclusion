from rest_framework import serializers

from inclusion import models


class StructureDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.StructureReport
        exclude = ["reporter", "created_at", "updated_at", "structure"]

    id = serializers.SerializerMethodField()
    typology = serializers.SlugRelatedField(slug_field="value", read_only=True)

    def get_id(self, obj):
        return obj.structure.id


class StructureReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.StructureReport
        fields = ["id", "data", "reporter", "created_at", "updated_at"]

    reporter = serializers.SlugRelatedField(slug_field="username", read_only=True)
    data = StructureDataSerializer(source="*")


class StructureSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.Structure
        fields = "__all__"

    latest_reports = StructureReportSerializer(many=True, default=[])


class CreateStructureReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.StructureReport
        fields = "__all__"

    typology = serializers.SlugRelatedField(
        slug_field="value", required=False, allow_null=True, queryset=models.StructureTypology.objects.all()
    )
