import pytest

from data_inclusion.api.valideur import translations, utils


@pytest.mark.parametrize(
    ("items", "expected"),
    [
        ([], ""),
        ([1], "1"),
        ([1, 2, 3], "1-3"),
        ([1, 3, 4, 5, 7], "1, 3-5, 7"),
    ],
)
def test_display_ranges(items, expected):
    assert utils.display_ranges(items) == expected


@pytest.mark.parametrize(
    ("msg", "expected"),
    [
        ("Field required", "Champ requis"),
        (
            "String should have at least 6 characters",
            "La chaîne doit contenir au moins 6 caractères",
        ),
    ],
)
def test_tr(msg, expected):
    assert translations.tr(msg) == expected
