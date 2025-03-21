import logging

logger = logging.getLogger(__name__)


def _get_client():
    import pendulum
    import sib_api_v3_sdk

    class BrevoClient:
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
