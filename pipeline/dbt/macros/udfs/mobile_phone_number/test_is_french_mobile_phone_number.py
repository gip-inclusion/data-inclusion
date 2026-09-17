import pytest
from is_french_mobile_phone_number import is_french_mobile_phone_number


@pytest.mark.parametrize(
    ("phone_number", "expected"),
    [
        (None, False),
        ("0687278992", True),
        ("+33687278992", True),
        ("07 56 78 90 12", True),
        ("+33756789012", True),
        ("0123456789", False),
        ("+590690000000", False),
        ("invalid", False),
    ],
)
def test_is_french_mobile_phone_number(phone_number, expected):
    assert is_french_mobile_phone_number(phone_number) is expected
