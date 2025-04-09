import pytest

from data_inclusion.processings.phonenumbers import format_phone_number


@pytest.mark.parametrize(
    ("phone_number", "expected"),
    [
        ("+33 1 23 45 67 89", "+33123456789"),
        ("+33 1 23 45 67", None),
        ("01 23 45 67 89", "+33123456789"),
        ("01 23 45 67", None),
        ("+33123456789", "+33123456789"),
        ("0123456789", "+33123456789"),
        ("+33 123456789", "+33123456789"),
        ("+355 23 45 67 89", "+35523456789"),
        ("+355 23 6 67 89", None),
        ("04 67 87 83 87 / 06 31 47 16 85", None),
        ("3333", "3333"),
        ("33  33", "3333"),
        ("116 000", "116000"),
        # Guadeloupe, Saint-Barthélemy et Saint-Martin
        pytest.param("05 90 00 00 00", "+590590000000", id="guadeloupe-fixed-0590"),
        pytest.param("06 90 00 00 00", "+590690000000", id="guadeloupe-mobile-0690"),
        pytest.param("06 91 00 00 00", "+590691000000", id="guadeloupe-mobile-0691"),
        # Guyane
        pytest.param("05 94 00 00 00", "+594594000000", id="guyane-fixed-0594"),
        pytest.param("06 94 00 00 00", "+594694000000", id="guyane-mobile-0694"),
        # Mayotte
        pytest.param("02 69 00 00 00", "+262269000000", id="mayotte-fixed-0269"),
        pytest.param("06 39 00 00 00", "+262639000000", id="mayotte-mobile-0639"),
        # Martinique
        pytest.param("05 96 00 00 00", "+596596000000", id="martinique-fixed-0596"),
        pytest.param("06 96 00 00 00", "+596696000000", id="martinique-mobile-0696"),
        pytest.param("06 97 00 00 00", "+596697000000", id="martinique-mobile-0697"),
        # Réunion
        pytest.param("02 62 00 00 00", "+262262000000", id="reunion-fixed-0262"),
        pytest.param("02 63 00 00 00", "+262263000000", id="reunion-fixed-0263"),
        pytest.param("06 92 00 00 00", "+262692000000", id="reunion-mobile-0692"),
        pytest.param("06 93 00 00 00", "+262693000000", id="reunion-mobile-0693"),
    ],
)
def test_format_phone_number(phone_number: str, expected: bool):
    assert format_phone_number(phone_number) == expected
