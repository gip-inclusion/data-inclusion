import logging
import tempfile
from datetime import date
from typing import Optional

import pandas as pd
import requests

from django.conf import settings
from django.core.management.base import BaseCommand, CommandParser
from rest_framework import serializers

from inclusion import models

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StructureDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = models.StructureReport
        exclude = [
            "created_at",
            "updated_at",
            "parent_report",
            "id_in_source",
            "is_latest",
            "extra",
            "score_geocodage",
        ]

    id = serializers.CharField(source="id_in_source")
    typologie = serializers.SlugRelatedField(slug_field="value", read_only=True)
    structure_parente = serializers.SerializerMethodField()

    def get_structure_parente(self, instance):
        return instance.parent_report.id_in_source if instance.parent_report is not None else None


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        breakpoint()
        logger.error(resp.json())
        raise err


class DataGouvClient:
    def __init__(
        self,
        base_url: str,
        dataset_id: str,
        api_key: Optional[str] = None,
    ):
        self.base_url = base_url + "/1"
        self.dataset_id = dataset_id
        self.session = requests.Session()
        self.session.hooks["response"] = [log_and_raise]
        if api_key is not None:
            self.session.headers.update({"X-API-KEY": api_key})

    def upload_dataset_resource(self, resource_id: str, filepath_str: str):
        self.session.post(
            f"{self.base_url}/datasets/{self.dataset_id}/resources/{resource_id}/upload/",
            files={"file": open(filepath_str, "rb")},
        )


class Command(BaseCommand):
    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("-n", "--dry-run", action="store_true")

    def handle(self, *args, **options):
        if not options["dry_run"] and any(
            [
                setting is None
                for setting in [
                    settings.DATAGOUV_API_URL,
                    settings.DATAGOUV_DI_DATASET_ID,
                    settings.DATAGOUV_API_KEY,
                ]
            ]
        ):
            logger.error("DATAGOUV_API_URL, DATAGOUV_DI_DATASET_ID, DATAGOUV_API_KEY must be set")
            raise SystemExit()

        serializer = StructureDataSerializer(
            models.StructureReport.objects.filter(is_latest=True),
            many=True,
        )

        base_filename_str = f"structures-inclusion-{date.today().strftime('%Y%m%d')}"
        output_dir_path_str = "." if options["dry_run"] else tempfile.TemporaryDirectory()
        base_filepath_str = f"{output_dir_path_str}/{base_filename_str}"

        df = pd.DataFrame.from_records(data=serializer.data)
        df.to_json(f"{base_filepath_str}.json", orient="records")
        df.set_index("id").to_csv(f"{base_filepath_str}.csv")
        df.set_index("id").to_excel(f"{base_filepath_str}.xlsx")

        if not options["dry_run"]:
            datagouv_client = DataGouvClient(
                base_url=settings.DATAGOUV_API_URL,
                dataset_id=settings.DATAGOUV_DI_DATASET_ID,
                api_key=settings.DATAGOUV_API_KEY,
            )
            if settings.DATAGOUV_DI_JSON_STRUCTURE_RESOURCE_ID is not None:
                datagouv_client.upload_dataset_resource(
                    resource_id=settings.DATAGOUV_DI_JSON_STRUCTURE_RESOURCE_ID,
                    filepath_str=f"{base_filepath_str}.json",
                )
            if settings.DATAGOUV_DI_CSV_STRUCTURE_RESOURCE_ID is not None:
                datagouv_client.upload_dataset_resource(
                    resource_id=settings.DATAGOUV_DI_CSV_STRUCTURE_RESOURCE_ID,
                    filepath_str=f"{base_filepath_str}.csv",
                )
            if settings.DATAGOUV_DI_XLSX_STRUCTURE_RESOURCE_ID is not None:
                datagouv_client.upload_dataset_resource(
                    resource_id=settings.DATAGOUV_DI_XLSX_STRUCTURE_RESOURCE_ID,
                    filepath_str=f"{base_filepath_str}.xlsx",
                )
        else:
            logger.info(f"Files generated in directory : {output_dir_path_str}")
