import pytest

from data_inclusion.pipeline.dags.extract_publics import extraction


@pytest.mark.integration
@pytest.mark.parametrize(
    ("input", "expected_output"),
    [
        (
            "Possibilite d entree en formation jusqu a 30 ans",
            {"resultat": [{"age": {"max": 30}}]},
        ),
        (
            "Beneficiaire du RSA ou + 30 ans",
            {"resultat": [{"allocation": "rsa"}, {"age": {"min": 30}}]},
        ),
        (
            "Pour les personnes rattachées au territoire communal de Bouillargues",
            {"resultat": [{"lieu_residence": "Bouillargues"}]},
        ),
    ],
)
def test_extraction(input, expected_output):
    assert extraction.extract(input) == expected_output
