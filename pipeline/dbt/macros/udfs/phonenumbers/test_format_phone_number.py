import pytest
from format_phone_number import format_phone_number


@pytest.mark.parametrize(
    ("phone_number", "expected"),
    [
        (None, None),
        ("+33 1 23 45 67 89", "+33123456789"),
        ("+33 1 23 45 67", None),
        (" 01 23 45 67 89", "+33123456789"),
        ("01.23.45.67.89", "+33123456789"),
        ("01-23-45-67-89", "+33123456789"),
        ("01 23 45 67", None),
        ("+33123456789", "+33123456789"),
        ("0123456789", "+33123456789"),
        ("+33 123456789", "+33123456789"),
        ("+355 23 45 67 89", "+35523456789"),
        ("+355 23 6 67 89", None),
        ("04 67 87 83 87 / 06 31 47 16 85", None),
        ("3333", "3333"),
        ("33-33", "3333"),
        ("33.33", "3333"),
        ("33  33", "3333"),
        ("116 000", "116000"),
        ("06 87 27 89 92 ou 03 89 20 80 80", None),
        ("15", "15"),
        ("112", "112"),
        ("0687278992", "+33687278992"),
        # Numéros courts SVA (Services à Valeur Ajoutée)
        pytest.param("3631", "3631", id="sva-la-poste"),
        pytest.param("3939", "3939", id="sva-service-public"),
        # Guadeloupe, Saint-Barthélemy et Saint-Martin
        pytest.param("05 90 00 00 00", "+590590000000", id="guadeloupe-fixed-0590"),
        pytest.param("06 90 00 00 00", "+590690000000", id="guadeloupe-mobile-0690"),
        pytest.param("06 91 29 00 00", "+590691290000", id="guadeloupe-mobile-0691"),
        # Guyane
        pytest.param("05 94 00 00 00", "+594594000000", id="guyane-fixed-0594"),
        pytest.param("06 94 00 00 00", "+594694000000", id="guyane-mobile-0694"),
        # Mayotte
        pytest.param("02 62 00 00 00", "+262262000000", id="mayotte-fixed-0262"),
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
        # DOM-TOM numbers using the +33 country code
        pytest.param("+33590000000", "+590590000000", id="guadeloupe-fixed-+33"),
        pytest.param("+33262000000", "+262262000000", id="guyane-fixed-+33"),
        pytest.param("+33262000000", "+262262000000", id="mayotte-fixed-+33"),
        pytest.param("+33269000000", "+262269000000", id="mayotte-fixed-+33-bis"),
        # Numéros spéciaux français (08/09)
        pytest.param("08 00 01 31 40", "+33800013140", id="france-toll-free-0800"),
        pytest.param("09 12 34 56 78", "+33912345678", id="france-voip-09"),
        # Saint-Pierre-et-Miquelon
        pytest.param("41 10 50", None, id="spm-without-areacode"),
        pytest.param("+5 08 41 10 50", "+508411050", id="spm-with-areacode"),
        pytest.param("05 08 41 10 50", "+508411050", id="spm-bad"),
        # Wallis-et-Futuna
        pytest.param("72 25 73", None, id="wallis-without-areacode"),
        pytest.param("+681 72 25 73", "+681722573", id="wallis-with-areacode"),
        pytest.param("06 81 72 25 73", "+33681722573", id="not-wallis"),
        # Polynésie française
        pytest.param("40 41 57 00", "+68940415700", id="polynesie-without-areacode"),
        pytest.param("+689 40 41 57 00", "+68940415700", id="polynesie-with-areacode"),
        pytest.param("06 89 00 00 00", "+33689000000", id="not-polynesie"),
        # Nouvelle-Calédonie
        pytest.param("27 31 15", None, id="nouvelle-cal-without-areacode"),
        pytest.param("+687 27 31 15", "+687273115", id="nouvelle-cal-with-areacode"),
        pytest.param("06 87 27 31 15", "+33687273115", id="not-nouvelle-cal"),
    ],
)
def test_format_phone_number(phone_number: str, expected: str | None):
    formatted = format_phone_number(phone_number)
    assert formatted == expected

    # test idempotency
    assert formatted == format_phone_number(formatted)
