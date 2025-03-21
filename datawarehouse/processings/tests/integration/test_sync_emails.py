import os

import pytest

from data_inclusion.processings import sync_emails
from data_inclusion.processings.sync_emails import BrevoClient


@pytest.fixture(autouse=True)
def prepare_brevo_tests(monkeypatch):
    monkeypatch.setenv("BREVO_CURRENT_CONTACTS_LIST_ID", "4")
    monkeypatch.setenv("BREVO_ALL_CONTACTS_LIST_ID", "2")
    client = BrevoClient(token=os.environ["AIRFLOW_VAR_BREVO_API_KEY"])
    client.empty_list(list_id=4)
    client.empty_list(list_id=2)


pytestmark = pytest.mark.skipif(
    "AIRFLOW_VAR_BREVO_API_KEY" not in os.environ,
    reason="AIRFLOW_VAR_BREVO_API_KEY environment variable is required",
)

def test_sync_emails():
    result = sync_emails(emails=["a@foo.com", "b@foo.com"])
    assert result == []
    result = sync_emails(emails=["a@foo.com", "b@foo.com"])
    assert result == [
        {
            'courriel': 'b@foo.com',
            'has_hardbounced': False,
            'was_objected_to': False,
        },
        {
            'courriel': 'a@foo.com',
            'has_hardbounced': False,
            'was_objected_to': False,
        },
    ]
    result = sync_emails(emails=["a@foo.com", "c@foo.com"])
    assert result == [
        {
            'courriel': 'b@foo.com',
            'has_hardbounced': False,
            'was_objected_to': False,
        },
        {
            'courriel': 'a@foo.com',
            'has_hardbounced': False,
            'was_objected_to': False,
        },
    ]
    result = sync_emails(emails=["a@foo.com", "c@foo.com"])
    assert result == [
        {
            'courriel': 'c@foo.com',
            'has_hardbounced': False,
            'was_objected_to': False,
        },
        {
            'courriel': 'b@foo.com',
            'has_hardbounced': False,
            'was_objected_to': False,
        },
        {
            'courriel': 'a@foo.com',
            'has_hardbounced': False,
            'was_objected_to': False,
        },
    ]
