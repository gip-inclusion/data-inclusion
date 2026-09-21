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
        ("+590690001234", True),
        ("+594694201234", True),
        ("+596696201234", True),
        ("+262692123456", True),
        ("+262639012345", True),
        ("+687751234", True),
        ("+68987123456", True),
        ("+681821234", True),
        ("+681721234", True),
        ("+508551234", True),
        ("+447911123456", False),
        ("invalid", False),
    ],
)
def test_is_french_mobile_phone_number(phone_number, expected):
    assert is_french_mobile_phone_number(phone_number) is expected
