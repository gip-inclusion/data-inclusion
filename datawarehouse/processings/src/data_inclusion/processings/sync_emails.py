import os
from collections.abc import Generator
from typing import Any

import sib_api_v3_sdk

from .log import setup_logging

logger = setup_logging(__name__)


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

    def empty_list(self, list_id) -> None:
        """Simplified version of empty_list, for tests. Does not handle large lists."""
        all_emails = [c["email"] for c in self.list_contacts(list_id=list_id)]
        if not all_emails:
            return
        contact_emails = sib_api_v3_sdk.RemoveContactFromList()
        contact_emails.emails = all_emails
        self.contacts_api.remove_contact_from_list(
            list_id=list_id,
            contact_emails=contact_emails,
        )

    def import_to_list(self, list_id, emails):
        import_params = sib_api_v3_sdk.RequestContactImport()
        # Ensure we have no invalid emails in the list, it would
        # make the whole import fail
        import_params.json_body = [{"email": email} for email in emails if email]
        import_params.list_ids = [int(list_id)]
        return self.contacts_api.import_contacts(request_contact_import=import_params)


def sync_emails(
    emails: list[str],
    all_contacts_list_id,
    current_contacts_list_id=None,
) -> list[dict[str, Any]]:
    BREVO_API_KEY = os.getenv("BREVO_API_KEY")
    if not BREVO_API_KEY:
        logger.info("BREVO_API_KEY is not set, skipping")
        return []

    brevo_client = BrevoClient(token=BREVO_API_KEY)
    contacts = list(brevo_client.list_contacts(all_contacts_list_id))

    known_contacts = [
        {
            "courriel": contact["email"],
            "has_hardbounced": contact["emailBlacklisted"],
            "was_objected_to": "DATE_DI_RGPD_OPPOSITION" in contact["attributes"],
        }
        for contact in contacts
    ]

    if current_contacts_list_id:  # avoid updating the lists by mistake
        logger.info(f"Importing contacts to {all_contacts_list_id=}")
        brevo_client.import_to_list(all_contacts_list_id, emails)
        logger.info(f"Importing contacts to {current_contacts_list_id=}")
        brevo_client.import_to_list(current_contacts_list_id, emails)
    else:
        logger.info("'current_contacts_list_id' is not set, skipping import")

    return known_contacts
