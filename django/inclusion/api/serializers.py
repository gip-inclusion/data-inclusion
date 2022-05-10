from rest_framework import serializers

from inclusion.models import Structure, StructureReport, StructureTypology


class StructureDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        exclude = ["created_at", "updated_at", "structure"]

    id = serializers.UUIDField(source="structure.id")
    siret = serializers.CharField(source="structure.siret")
    rna = serializers.CharField(source="structure.rna")
    typologie = serializers.SlugRelatedField(slug_field="value", read_only=True)


class StructureReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        fields = ["id", "data", "created_at", "updated_at"]

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

    siret = serializers.CharField(
        source="structure.siret",
        allow_null=True,
        allow_blank=True,
        max_length=14,
        required=False,
    )
    rna = serializers.CharField(
        source="structure.rna",
        allow_null=True,
        allow_blank=True,
        max_length=10,
        required=False,
    )
    typologie = serializers.SlugRelatedField(
        slug_field="value", required=False, allow_null=True, queryset=StructureTypology.objects.all()
    )

    def create(self, data):
        # Retrouve la structure à partir des données
        if "structure" not in data:
            raise serializers.ValidationError("pivot(s) manquant(s): siret, rna, etc.")
        else:
            siret = data["structure"].get("siret", None)
            rna = data["structure"].get("rna", None)

            if siret is None and rna is None:
                raise serializers.ValidationError("pivot(s) manquant(s): siret, rna, etc.")

        structure = Structure.objects.get_from_pivots(siret=siret, rna=rna)

        if structure is None:
            structure = Structure.objects.create(siret=siret, rna=rna)

        data["structure"] = structure
        report = StructureReport.objects.create(**data)
        return report
