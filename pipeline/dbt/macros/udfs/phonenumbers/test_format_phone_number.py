import pytest
from format_phone_number import format_phone_number


@pytest.mark.parametrize(
    ("phone_number", "expected"),
    [
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
        # Numéros courts SVA (Services à Valeur Ajoutée)
        pytest.param("3631", "3631", id="sva-la-poste"),
        pytest.param("3939", "3939", id="sva-service-public"),
        # Guadeloupe, Saint-Barthélemy et Saint-Martin
        pytest.param("05 90 00 00 00", "+590590000000", id="guadeloupe-fixed-0590"),
        pytest.param("06 90 00 00 00", "+590690000000", id="guadeloupe-mobile-0690"),
        pytest.param(
            "06 91 00 00 00",
            "+590691000000",
            id="guadeloupe-mobile-0691",
            marks=pytest.mark.xfail,
        ),
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
        # DOM-TOM numbers using the +33 prefix
        pytest.param(
            "+33 5 90 00 00 00", "+590590000000", id="guadeloupe-fixed-+33-prefix"
        ),
        pytest.param(
            "+33 2 62 00 00 00", "+262262000000", id="guyane-fixed-+33-prefix"
        ),
        pytest.param(
            "+33 2 62 00 00 00", "+262262000000", id="mayotte-fixed-+33-prefix"
        ),
        # Numéros spéciaux français (08/09) - doivent rester en +33
        pytest.param(" 08 00 01 31 40", "+33800013140", id="france-toll-free-0800"),
        pytest.param("08 00 01 31 40", "+33800013140", id="france-toll-free-0800-2"),
        pytest.param("09 12 34 56 78", "+33912345678", id="france-voip-09"),
        # Saint-Pierre-et-Miquelon
        pytest.param("05 08 41 10 50", "+508508411050", id="spm-fixed-0508"),
        # Wallis-et-Futuna
        pytest.param("06 81 00 00 00", "+681681000000", id="wallis-0681"),
        # Polynésie française
        pytest.param("06 89 00 00 00", "+689689000000", id="polynesie-0689"),
        # Nouvelle-Calédonie
        pytest.param("06 87 00 00 00", "+687687000000", id="nouvelle-caledonie-0687"),
        # None input
        (None, None),
    ],
)
def test_format_phone_number(phone_number: str, expected: str | None):
    assert format_phone_number(phone_number) == expected
