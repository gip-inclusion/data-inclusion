import logging

import requests

logger = logging.getLogger(__name__)


def log_and_raise(resp: requests.Response, *args, **kwargs):
    try:
        resp.raise_for_status()
    except requests.HTTPError as err:
        logger.error(resp.json())
        raise err


class GristClient:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.hooks["response"] = [log_and_raise]
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
        ).json()

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


def _normalize_table_name(name: str) -> str:
    # Grist normalizes table names
    return name.capitalize().replace("-", "_")


def extract(
    url: str,
    workspace_id: str,
    token: str,
    id: str,
    source_dict: dict,
    **kwargs,
) -> bytes:
    grist_client = GristClient(base_url=url, token=token)

    # 1. Get documents in workspace
    workspace_dict = grist_client.describe_workspace(workspace_id=workspace_id)

    # 2. Found document with name matching the current source id
    target_document_dict = next(
        (
            document_dict
            for document_dict in workspace_dict["docs"]
            if document_dict["name"] == source_dict["id"]
        ),
        None,
    )

    if target_document_dict is None:
        raise Exception(f"No document with name '{source_dict['id']}' in workspace.")

    # 3. Download the target ressource
    return grist_client.download_table_content_as_csv(
        document_id=target_document_dict["id"],
        table_id=_normalize_table_name(id),
    )
