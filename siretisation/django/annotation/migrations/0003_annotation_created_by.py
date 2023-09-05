# Generated by Django 4.0.6 on 2022-07-27 10:23

import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("annotation", "0002_annotation_closed_annotation_irrelevant_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="annotation",
            name="created_by",
            field=models.ForeignKey(
                blank=True, null=True, on_delete=django.db.models.deletion.PROTECT, to=settings.AUTH_USER_MODEL
            ),
        ),
    ]
