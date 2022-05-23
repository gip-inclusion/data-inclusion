from rest_framework import serializers

from inclusion.models import StructureReport, StructureTypology


class AntenneDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        exclude = ["created_at", "updated_at", "parent_report", "id_in_source"]

    id = serializers.CharField(source="id_in_source")
    typologie = serializers.SlugRelatedField(slug_field="value", read_only=True)
    structure_parente = serializers.UUIDField(source="parent_report.id_in_source")


class StructureDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        exclude = ["created_at", "updated_at", "parent_report", "id_in_source"]

    id = serializers.CharField(source="id_in_source")
    typologie = serializers.SlugRelatedField(slug_field="value", read_only=True)
    structure_parente = serializers.SerializerMethodField()

    def get_structure_parente(self, instance):
        return None


class StructureReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        fields = ["id", "data", "created_at", "updated_at", "antennes_data"]

    data = StructureDataSerializer(source="*")
    antennes_data = AntenneDataSerializer(source="sub_reports", many=True, required=False, default=[])


class CreateStructureReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        exclude = ["id_in_source", "parent_report"]

    id = serializers.CharField(source="id_in_source", required=False)
    siret = serializers.CharField(
        allow_null=True,
        allow_blank=True,
        max_length=14,
        required=False,
    )
    rna = serializers.CharField(
        allow_null=True,
        allow_blank=True,
        max_length=10,
        required=False,
    )
    typologie = serializers.SlugRelatedField(
        slug_field="value",
        required=False,
        allow_null=True,
        queryset=StructureTypology.objects.all(),
    )
    structure_parente = serializers.CharField(
        allow_null=True,
        allow_blank=True,
        required=False,
    )

    def create(self, data):
        # Retrouve la structure à partir des données
        siret = data.get("siret", None)
        rna = data.get("rna", None)
        source = data.get("source", None)
        structure_parente = data.pop("structure_parente", None)

        if structure_parente is not None and source is None:
            raise serializers.ValidationError("veuillez préciser la source")
        elif structure_parente is not None and source is not None:
            parent_report = StructureReport.objects.filter(
                id_in_source=structure_parente,
                source=source,
            ).latest("created_at")
            if parent_report is None:
                raise serializers.ValidationError("veuillez d'abord référencer la structure parente")
            report = StructureReport.objects.create(**data, parent_report=parent_report)
        elif siret is not None or rna is not None:
            report = StructureReport.objects.create(**data)
        else:
            raise serializers.ValidationError("veuillez préciser un pivot ou référencer une structure parente")

        return report
