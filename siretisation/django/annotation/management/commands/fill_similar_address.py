import logging

import tqdm

from django.core.management.base import BaseCommand, CommandParser

from annotation import models, services

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("dataset_slug", choices=[d.slug for d in models.Dataset.objects.all()], type=str)

    def handle(self, *args, **options):
        dataset_instance = models.Dataset.objects.get(slug=options["dataset_slug"])

        for row_instance in tqdm.tqdm(
            dataset_instance.rows.iterator(),
            total=dataset_instance.rows.count(),
        ):
            similar_address = services.search_similar_address(
                address=row_instance.data["adresse"],
                postal_code=row_instance.data["code_postal"],
            )
            if similar_address is not None:
                row_instance.similar_address = similar_address
                row_instance.save()
