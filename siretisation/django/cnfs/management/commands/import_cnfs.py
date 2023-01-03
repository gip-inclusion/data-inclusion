import numpy as np
import pandas as pd
from cnfs import models

from django.contrib.gis.geos import Point
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    def handle(self, *args, **options):
        input_df = pd.read_json(
            "https://api.conseiller-numerique.gouv.fr/permanences",
            dtype=False,
        )

        input_df = input_df.replace([np.nan, ""], None)

        output_df = pd.DataFrame()

        output_df = output_df.assign(id=input_df["id"])
        output_df = output_df.assign(siret=input_df["pivot"].replace(r"^(?!\d{14}).*$", lambda _: None))
        output_df = output_df.assign(name=input_df["nom"])
        output_df = output_df.assign(address=input_df["adresse"])
        output_df = output_df.assign(city=input_df["commune"])
        output_df = output_df.assign(postal_code=input_df["code_postal"])
        output_df = output_df.assign(
            position=input_df.apply(lambda row: Point(row.longitude, row.latitude, srid=4326), axis="columns")
        )

        models.Permanence.objects.all().delete()

        models.Permanence.objects.bulk_create(
            list(output_df.apply(lambda row: models.Permanence(**row.to_dict()), axis="columns")),
            update_conflicts=True,
            update_fields=[
                "siret",
                "name",
                "address",
                "city",
                "postal_code",
                "position",
            ],
            unique_fields=["id"],
        )
