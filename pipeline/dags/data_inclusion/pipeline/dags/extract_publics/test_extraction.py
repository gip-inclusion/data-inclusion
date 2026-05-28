import pytest

from data_inclusion.pipeline.dags.extract_publics.extraction import (
    Extraction,
    Profil,
    Profiler,
    TrancheAge,
)


@pytest.mark.integration
@pytest.mark.parametrize(
    ("input", "expected_output"),
    [
        (
            "Possibilite d entree en formation jusqu a 30 ans",
            Extraction(profils=[Profil(age=TrancheAge(max=30))]),
        ),
        (
            "Beneficiaire du RSA ou + 30 ans",
            Extraction(
                profils=[
                    Profil(allocation="rsa"),
                    Profil(age=TrancheAge(min=30)),
                ]
            ),
        ),
        (
            "Pour les personnes rattachées au territoire communal de Bouillargues",
            Extraction(
                profils=[
                    Profil(lieu_residence="Bouillargues"),
                ]
            ),
        ),
    ],
)
def test_extraction(
    input: str,
    expected_output: Extraction,
):
    profiler = Profiler()
    assert profiler.extract(input) == expected_output
