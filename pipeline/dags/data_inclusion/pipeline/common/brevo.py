import logging
from collections.abc import Generator

logger = logging.getLogger(__name__)


def _get_client():
    import time

    import pendulum
    import sib_api_v3_sdk

    class BrevoClient:
        CONTACT_LIST_PAGE_SIZE = 500  # the maximum
        IMPORT_BATCH_SIZE = 2000

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

        def list_contacts(self, list_id) -> Generator[sib_api_v3_sdk.GetContactDetails]:
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
            all_emails = [c["email"] for c in self.list_contacts(list_id=list_id)]
            responses = []
            for i in range(0, len(all_emails), 150):
                contact_emails = sib_api_v3_sdk.RemoveContactFromList()
                contact_emails.emails = all_emails[i : i + 150]
                responses.append(
                    self.contacts_api.remove_contact_from_list(
                        list_id=list_id,
                        contact_emails=contact_emails,
                    )
                )
            return responses

        def import_to_list(self, list_id, contacts: list[dict]):
            # Ensure we have no invalid emails in the list, it would
            # make the whole import fail
            contacts = [c for c in contacts if c["email"]]
            # keep request bodies small, brevo rejects oversized ones
            for i in range(0, len(contacts), self.IMPORT_BATCH_SIZE):
                import_params = sib_api_v3_sdk.RequestContactImport()
                import_params.json_body = contacts[i : i + self.IMPORT_BATCH_SIZE]
                import_params.list_ids = [int(list_id)]
                response = self.contacts_api.import_contacts(
                    request_contact_import=import_params
                )
                self.wait_for_process(process_id=response.process_id)

        def wait_for_process(self, process_id, timeout=300) -> None:
            # Brevo processes are queued: their duration depends on brevo's load,
            # not only on the import size. Timing out does not cancel the process,
            # it only stops waiting for it.
            processes_api = sib_api_v3_sdk.ProcessApi(self.client)
            deadline = time.monotonic() + timeout
            while True:
                status = processes_api.get_process(process_id=process_id).status
                if status == "completed":
                    return
                if time.monotonic() > deadline:
                    raise TimeoutError(f"process {process_id} is still {status}")
                time.sleep(10)

        # brevo only returns statistics for the last 90 days by default, and
        # possibly limits date ranges to 90 days : walk back in time by windows.
        STATS_WINDOW_DAYS = 90
        # date of the first data·inclusion campaign
        STATS_START_DATE = pendulum.date(2023, 11, 1)

        def get_blocklist_motif(self, email) -> str:
            # most recent windows first : the latest event is the relevant one
            end_date = pendulum.today().date()
            while end_date >= self.STATS_START_DATE:
                start_date = max(
                    end_date.subtract(days=self.STATS_WINDOW_DAYS - 1),
                    self.STATS_START_DATE,
                )
                stats = self.contacts_api.get_contact_info(
                    identifier=email,
                    start_date=start_date.to_date_string(),
                    end_date=end_date.to_date_string(),
                ).statistics
                if stats.hard_bounces:
                    return "hardbounce"
                if stats.complaints:
                    return "spam"
                if stats.unsubscriptions and stats.unsubscriptions.user_unsubscription:
                    return "desinscription"
                end_date = start_date.subtract(days=1)
            return "autre"

        def set_blocklist_motif(self, email, motif: str | None) -> None:
            self.contacts_api.update_contact(
                identifier=email,
                update_contact=sib_api_v3_sdk.UpdateContact(
                    attributes={"MOTIF_BLOCKLIST": motif}
                ),
            )

        def classify_blocklisted_contacts(self, list_id) -> None:
            """Store the blocklisting reason in the MOTIF_BLOCKLIST attribute.

            Brevo only exposes a boolean `emailBlacklisted`, which mixes hardbounces,
            unsubscriptions, spam complaints and admin blocklisting.
            """
            for contact in self.list_contacts(list_id=list_id):
                motif = contact["attributes"].get("MOTIF_BLOCKLIST")

                if not contact["emailBlacklisted"]:
                    # the contact left the blocklist : the motif is outdated, and
                    # would be wrong if the contact is blocklisted again later.
                    if motif:
                        logger.info("clearing outdated blocklist motif")
                        self.set_blocklist_motif(email=contact["email"], motif=None)
                    continue

                # `autre` means no signal was found yet : it can still change
                if motif and motif != "autre":
                    continue

                new_motif = self.get_blocklist_motif(email=contact["email"])
                if new_motif != motif:
                    self.set_blocklist_motif(email=contact["email"], motif=new_motif)

        def create_and_send_email_campaign(
            self,
            subject,
            template_id,
            to_list_id,
            from_email,
            from_name,
            reply_to,
            tag,
        ):
            email_api = sib_api_v3_sdk.EmailCampaignsApi(self.client)
            cp_info = sib_api_v3_sdk.CreateEmailCampaign(
                template_id=template_id,
                subject=subject,
                tag=tag,
                sender=sib_api_v3_sdk.CreateEmailCampaignSender(
                    email=from_email, name=from_name
                ),
                name=f"{subject} {pendulum.now().to_date_string()}",
                reply_to=reply_to,
                recipients=sib_api_v3_sdk.CreateEmailCampaignRecipients(
                    list_ids=[to_list_id]
                ),
            )
            response = email_api.create_email_campaign(cp_info)
            return email_api.send_email_campaign_now(response.id)

    return BrevoClient


def __getattr__(name):
    if name == "BrevoClient":
        return _get_client()

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
