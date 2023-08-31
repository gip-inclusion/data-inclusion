from django.contrib.postgres.operations import CreateExtension
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("sirene", "0002_remove_establishment_full_text_trgm_idx_and_more"),
    ]

    operations = [
        CreateExtension("postgis"),
    ]
