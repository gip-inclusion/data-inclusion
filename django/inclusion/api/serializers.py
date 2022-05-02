from rest_framework import serializers

from inclusion.models import Structure, StructureReport, StructureTypology


class StructureDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        exclude = ["reporter", "created_at", "updated_at", "structure"]

    id = serializers.UUIDField(source="structure.id")
    siret = serializers.CharField(source="structure.siret")
    rna = serializers.CharField(source="structure.rna")
    typologie = serializers.SlugRelatedField(slug_field="value", read_only=True)


class StructureReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        fields = ["id", "data", "reporter", "created_at", "updated_at"]

    reporter = serializers.SlugRelatedField(slug_field="username", read_only=True)
    data = StructureDataSerializer(source="*")


class StructureSerializer(serializers.ModelSerializer):
    class Meta:
        model = Structure
        fields = "__all__"

    latest_reports = StructureReportSerializer(many=True, default=[])


class CreateStructureReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        exclude = ["structure"]

    siret = serializers.CharField(source="structure.siret", allow_null=True)
    rna = serializers.CharField(source="structure.rna", allow_null=True)
    typologie = serializers.SlugRelatedField(
        slug_field="value", required=False, allow_null=True, queryset=StructureTypology.objects.all()
    )

    def create(self, data):
        # Retrouve la structure à partir des données pivots
        siret = data["structure"]["siret"]
        rna = data["structure"]["rna"]

        if siret is None and rna is None:
            raise serializers.ValidationError("pivot(s) manquant(s): siret, rna, etc.")

        structure = Structure.objects.get_from_pivots(siret=siret, rna=rna)

        if structure is None:
            structure = Structure.objects.create(siret=siret, rna=rna)

        data["structure"] = structure
        report = StructureReport.objects.create(**data)
        return report
