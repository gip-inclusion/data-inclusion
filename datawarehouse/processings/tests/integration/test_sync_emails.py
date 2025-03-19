import os

import pytest

from data_inclusion.processings import sync_emails
from data_inclusion.processings.sync_emails import BrevoClient


@pytest.fixture(autouse=True)
def prepare_brevo_tests(monkeypatch):
    client = BrevoClient(token=os.environ["BREVO_API_KEY"])
    client.empty_list(list_id=4)
    client.empty_list(list_id=2)


pytestmark = pytest.mark.skipif(
    "BREVO_API_KEY" not in os.environ,
    reason="BREVO_API_KEY environment variable is required",
)


def test_sync_emails():
    ALL_CONTACTS_TEST_LIST_ID = 2
    CURRENT_CONTACTS_TEST_LIST_ID = 4
    result = sync_emails(
        ["a@foo.com", "b@foo.com"],
        ALL_CONTACTS_TEST_LIST_ID,
        CURRENT_CONTACTS_TEST_LIST_ID,
    )
    assert result == []
    result = sync_emails(
        ["a@foo.com", "b@foo.com"],
        ALL_CONTACTS_TEST_LIST_ID,
        CURRENT_CONTACTS_TEST_LIST_ID,
    )
    assert result == [
        {
            "courriel": "b@foo.com",
            "has_hardbounced": False,
            "was_objected_to": False,
        },
        {
            "courriel": "a@foo.com",
            "has_hardbounced": False,
            "was_objected_to": False,
        },
    ]
    result = sync_emails(
        ["a@foo.com", "c@foo.com"],
        ALL_CONTACTS_TEST_LIST_ID,
        CURRENT_CONTACTS_TEST_LIST_ID,
    )
    assert result == [
        {
            "courriel": "b@foo.com",
            "has_hardbounced": False,
            "was_objected_to": False,
        },
        {
            "courriel": "a@foo.com",
            "has_hardbounced": False,
            "was_objected_to": False,
        },
    ]
    result = sync_emails(
        ["a@foo.com", "c@foo.com"],
        ALL_CONTACTS_TEST_LIST_ID,
        CURRENT_CONTACTS_TEST_LIST_ID,
    )
    assert result == [
        {
            "courriel": "c@foo.com",
            "has_hardbounced": False,
            "was_objected_to": False,
        },
        {
            "courriel": "b@foo.com",
            "has_hardbounced": False,
            "was_objected_to": False,
        },
        {
            "courriel": "a@foo.com",
            "has_hardbounced": False,
            "was_objected_to": False,
        },
    ]
