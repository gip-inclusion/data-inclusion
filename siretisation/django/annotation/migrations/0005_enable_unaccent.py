from django.contrib.postgres import operations
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("annotation", "0004_dataset_slug"),
    ]

    operations = [
        operations.UnaccentExtension(),
    ]
