import pandas as pd

from django.db import migrations


def import_naf(apps, _) -> None:
    CodeNAF = apps.get_model("sirene", "CodeNAF")

    series_by_level_index = {
        level_index: pd.read_excel(
            f"https://www.insee.fr/fr/statistiques/fichier/2120875/naf2008_liste_n{level_index}.xls",
            header=2,
            dtype=str,
        )
        .set_index("Code")
        .squeeze("columns")
        for level_index in range(1, 6)
    }

    df = pd.read_excel(
        "https://www.insee.fr/fr/statistiques/fichier/2120875/naf2008_5_niveaux.xls",
        dtype=str,
    )

    df = df.assign(
        **{
            f"LABEL{level_index}": df.apply(lambda row: series[row[f"NIV{level_index}"]], axis="columns")
            for level_index, series in series_by_level_index.items()
        }
    )

    df = df.rename(
        columns={
            "NIV1": "section_code",
            "NIV2": "division_code",
            "NIV3": "group_code",
            "NIV4": "class_code",
            "NIV5": "code",
            "LABEL1": "section_label",
            "LABEL2": "division_label",
            "LABEL3": "group_label",
            "LABEL4": "class_label",
            "LABEL5": "label",
        }
    )

    CodeNAF.objects.bulk_create(
        list(df.apply(lambda row: CodeNAF(**row.to_dict()), axis="columns")),
        update_conflicts=True,
        update_fields=[
            "label",
            "section_code",
            "section_label",
            "division_code",
            "division_label",
            "group_code",
            "group_label",
            "class_code",
            "class_label",
        ],
        unique_fields=["code"],
    )

    # special `00.00Z` for establishment not yet categorized
    # https://www.sirene.fr/sirene/public/variable/activitePrincipaleEtablissement
    CodeNAF.objects.get_or_create(
        code="00.00Z",
        label="",
        section_code="",
        section_label="",
        division_code="",
        division_label="",
        group_code="",
        group_label="",
        class_code="",
        class_label="",
    )


class Migration(migrations.Migration):
    dependencies = [
        ("sirene", "0004_add_codenaf_table"),
    ]

    operations = [
        migrations.RunPython(import_naf, reverse_code=migrations.RunPython.noop),
    ]
