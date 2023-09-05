# Generated by Django 4.1.3 on 2022-11-18 14:31

import textwrap

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("sirene", "0005_import_codenaf"),
    ]

    operations = [
        migrations.AlterField(
            model_name="establishment",
            name="ape",
            field=models.CharField(max_length=6, null=True),
        ),
        migrations.RunSQL(
            sql=textwrap.dedent(
                """
                UPDATE sirene_establishment
                SET ape = NULL
                WHERE ape NOT IN (SELECT code FROM sirene_codenaf);
                """
            ),
            reverse_sql=textwrap.dedent(
                """
                UPDATE sirene_establishment
                SET ape = ''
                WHERE ape IS NULL;
                """
            ),
        ),
        migrations.AlterField(
            model_name="establishment",
            name="ape",
            field=models.ForeignKey(
                null=True,
                on_delete=models.deletion.PROTECT,
                related_name="establishments",
                to="sirene.codenaf",
            ),
        ),
    ]
