import logging
from pathlib import Path

import numpy as np
import pandas as pd

from django.core.management.base import BaseCommand, CommandParser

from annotation import models, services

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("input_path", type=Path)
        parser.add_argument("dataset_label", type=str)
        parser.add_argument(
            "organization_name",
            choices=[g.name for g in models.Group.objects.all()],
            type=str,
        )

    def handle(self, *args, **options):
        input_path = options["input_path"]

        if input_path.suffix == ".csv":
            df = pd.read_csv(input_path, dtype=str)
        elif input_path.suffix == ".json":
            df = pd.read_json(input_path, dtype=False)
        else:
            logger.error("Allowed file extensions : csv, json.")
            raise SystemExit()

        df = df.replace({np.nan: None})

        services.import_dataset(
            dataset_rows_list=df.to_dict(orient="records"),
            dataset_label=options["dataset_label"],
            organization_id=models.Group.objects.get(name=options["organization_name"]).pk,
        )
