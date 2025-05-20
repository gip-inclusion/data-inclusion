import pytest

from data_inclusion.api.analytics.services import is_bot


@pytest.mark.parametrize(
    ("user_agent", "expected"),
    [
        (None, False),
        ("", False),
        ("python-requests/2.28.1", False),
        ("curl/7.68.0", False),
        # Bot identifiers found in Datadog logs
        ("bingbot/2.0; +http://www.bing.com/bingbot.htm)", True),
        (
            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
            True,
        ),
        (
            "meta-externalagent/1.1 "
            "(+https://developers.facebook.com/docs/sharing/webmasters/crawler)",
            True,
        ),
        # Normal user agents
        (
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:138.0) "
            "Gecko/20100101 Firefox/138.0",
            False,
        ),
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 "
            "Safari/604.1",
            False,
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
            "(KHTML, like Gecko) Version/15.0 Safari/605.1.15",
            False,
        ),
    ],
)
def test_is_bot(user_agent, expected):
    assert is_bot(user_agent) == expected
