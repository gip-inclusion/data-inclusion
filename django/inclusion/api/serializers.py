from rest_framework import serializers

from inclusion.models import StructureLabel, StructureReport, StructureTypology


class AntenneDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        exclude = ["created_at", "updated_at", "parent_report", "id_in_source", "is_latest"]

    id = serializers.CharField(source="id_in_source")
    typologie = serializers.SlugRelatedField(slug_field="value", read_only=True)
    structure_parente = serializers.UUIDField(source="parent_report.id_in_source")
    labels_nationaux = serializers.SlugRelatedField(slug_field="value", read_only=True, many=True)


class StructureDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        exclude = ["created_at", "updated_at", "parent_report", "id_in_source", "is_latest"]

    id = serializers.CharField(source="id_in_source")
    typologie = serializers.SlugRelatedField(slug_field="value", read_only=True)
    structure_parente = serializers.SerializerMethodField()
    labels_nationaux = serializers.SlugRelatedField(slug_field="value", read_only=True, many=True)

    def get_structure_parente(self, instance):
        return instance.parent_report.id_in_source if instance.parent_report is not None else None


class StructureReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        fields = ["id", "data", "created_at", "updated_at", "antennes_data"]

    data = StructureDataSerializer(source="*")
    antennes_data = AntenneDataSerializer(source="sub_reports", many=True, required=False, default=[])


class CreateStructureReportSerializer(serializers.ModelSerializer):
    class Meta:
        model = StructureReport
        exclude = ["id_in_source", "parent_report", "is_latest"]

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
    lien_source = serializers.CharField(
        allow_null=True,
        allow_blank=True,
        required=False,
    )
    horaires_ouverture = serializers.CharField(
        allow_null=True,
        allow_blank=True,
        required=False,
    )
    labels_nationaux = serializers.SlugRelatedField(
        allow_null=True,
        allow_empty=True,
        many=True,
        slug_field="value",
        queryset=StructureLabel.objects.all(),
        required=False,
    )

    def create(self, data):
        # Overriden in order to deal with antenna reports
        # Antenna report can have no siret or rna provided, and can reference only its
        # parent structure. As such, antenna reports will be attached to their parent
        # structure report.

        siret = data.get("siret", None)
        rna = data.get("rna", None)
        source = data.get("source", None)
        structure_parente = data.pop("structure_parente", None)
        labels_nationaux = data.pop("labels_nationaux", None)

        if structure_parente is not None and source is None:
            raise serializers.ValidationError("veuillez préciser la source")
        elif structure_parente is not None and source is not None:
            parent_report = StructureReport.objects.filter(
                id_in_source=structure_parente,
                source=source,
                is_latest=True,
            ).first()
            if parent_report is None:
                raise serializers.ValidationError("veuillez d'abord référencer la structure parente")
            report = StructureReport.objects.create(**data, parent_report=parent_report)
        elif siret is not None or rna is not None:
            report = StructureReport.objects.create(**data)
        else:
            raise serializers.ValidationError("veuillez préciser un pivot ou référencer une structure parente")

        if labels_nationaux is not None:
            report.labels_nationaux.add(*labels_nationaux)

        return report
