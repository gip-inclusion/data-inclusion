import os
from collections.abc import Generator
from typing import Any

import sib_api_v3_sdk


class BrevoClient:
    CONTACT_LIST_PAGE_SIZE = 500  # the maximum

    def __init__(self, token: str) -> None:
        self.token = token
        self._client = None

    @property
    def client(self) -> sib_api_v3_sdk.ApiClient:
        if self._client is None:
            configuration = sib_api_v3_sdk.Configuration()
            configuration.api_key["api-key"] = self.token
            self._client = sib_api_v3_sdk.ApiClient(configuration)
        return self._client

    @property
    def contacts_api(self) -> sib_api_v3_sdk.ContactsApi:
        return sib_api_v3_sdk.ContactsApi(self.client)

    def list_contacts(
        self, list_id
    ) -> Generator[sib_api_v3_sdk.GetContactDetails, None, None]:
        index = 0
        while True:
            response = self.contacts_api.get_contacts_from_list(
                list_id=list_id,
                limit=self.CONTACT_LIST_PAGE_SIZE,
                offset=index,
            )
            if len(response.contacts) == 0:
                break
            yield from response.contacts
            index += self.CONTACT_LIST_PAGE_SIZE


def brevo_import_contacts() -> list[dict[str, Any]]:
    BREVO_API_KEY = os.getenv("BREVO_API_KEY")
    if not BREVO_API_KEY:
        print("BREVO_API_KEY is not set, skipping")
        return []

    brevo_client = BrevoClient(token=BREVO_API_KEY)
    contacts = list(brevo_client.list_contacts(6))  # list ID for all contacts

    return [
        {
            "courriel": contact["email"],
            "has_hardbounced": contact["emailBlacklisted"],
            "was_objected_to": "DATE_DI_RGPD_OPPOSITION" in contact["attributes"],
        }
        for contact in contacts
    ]
