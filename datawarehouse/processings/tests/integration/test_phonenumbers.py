import pytest

from data_inclusion.processings.phonenumbers import format_phone_number


@pytest.mark.parametrize(
    ("phone_number", "expected"),
    [
        ("+33 1 23 45 67 89", "01 23 45 67 89"),
        ("+33 1 23 45 67", None),
        ("01 23 45 67 89", "01 23 45 67 89"),
        ("01 23 45 67", None),
        ("+33123456789", "01 23 45 67 89"),
        ("0123456789", "01 23 45 67 89"),
        ("+33 123456789", "01 23 45 67 89"),
        ("+355 23 45 67 89", "+355 23 456 789"),
        ("+355 23 6 67 89", None),
        ("04 67 87 83 87 / 06 31 47 16 85", None),
    ],
)
def test_format_phone_number(phone_number: str, expected: bool):
    assert format_phone_number(phone_number) == expected
