import logging
import re

from . import utils

logger = logging.getLogger(__name__)


class GristClient(utils.BaseApiClient):
    def __init__(self, base_url: str, token: str):
        super().__init__(base_url)
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def _create_document(self, workspace_id: str, document_name: str) -> str:
        return self.session.post(
            self.base_url + f"/workspaces/{workspace_id}/docs",
            json={"name": document_name},
        ).json()

    def create_document(self, workspace_id: str, document_name: str) -> str:
        workspace_dict = self.describe_workspace(workspace_id=workspace_id)

        existing_document_dict = next(
            (
                document_dict
                for document_dict in workspace_dict["docs"]
                if document_dict["name"] == document_name
            ),
            None,
        )

        if existing_document_dict is not None:
            logger.warning(
                f"A document with name '{document_name}' already exists in workspace."
            )
            return existing_document_dict["id"]

        return self._create_document(
            workspace_id=workspace_id, document_name=document_name
        )

    def _create_table(self, document_id: str, table_name: str, columns: list) -> str:
        return self.session.post(
            self.base_url + f"/docs/{document_id}/tables",
            json={"tables": [{"id": table_name, "columns": columns}]},
        ).json()["tables"][0]["id"]

    def list_tables(self, document_id: str) -> list:
        return self.session.get(
            self.base_url + f"/docs/{document_id}/tables",
        ).json()["tables"]

    def create_table(self, document_id: str, table_name: str, columns: list) -> str:
        tables_list = self.list_tables(document_id=document_id)

        existing_table_dict = next(
            (
                table_dict
                for table_dict in tables_list
                if table_dict["id"] == table_name
            ),
            None,
        )

        if existing_table_dict is not None:
            logger.warning(
                f"A table with name '{table_name}' already exists in document."
            )
            return existing_table_dict["id"]

        return self._create_table(
            document_id=document_id, table_name=table_name, columns=columns
        )

    def describe_workspace(self, workspace_id: str):
        # https://support.getgrist.com/api/#tag/workspaces/paths/~1workspaces~1%7BworkspaceId%7D/get
        return self.session.get(self.base_url + f"/workspaces/{workspace_id}").json()

    def download_table_content_as_csv(self, document_id: str, table_id: str) -> bytes:
        # https://support.getgrist.com/api/#tag/docs/paths/~1docs~1%7BdocId%7D~1download~1csv/get
        return self.session.get(
            self.base_url + f"/docs/{document_id}/download/csv",
            params={"tableId": table_id},
        ).content

    def add_records(self, document_id: str, table_id: str, records: list):
        # https://support.getgrist.com/api/#tag/records/paths/~1docs~1%7BdocId%7D~1tables~1%7BtableId%7D~1records/post
        return self.session.post(
            self.base_url + f"/docs/{document_id}/tables/{table_id}/records",
            json={"records": records},
        )


def extract(url: str, token: str, **kwargs) -> bytes:
    match = re.search(
        r"(?P<base_url>.+)/docs/(?P<document_id>\w+)/download/csv\?.*tableId=(?P<table_id>\w+)",  # noqa: E501
        url,
    )

    if match is None:
        raise Exception("Invalid url")

    base_url, document_id, table_id = match.groups()

    grist_client = GristClient(base_url=base_url, token=token)

    return grist_client.download_table_content_as_csv(
        document_id=document_id, table_id=table_id
    )
