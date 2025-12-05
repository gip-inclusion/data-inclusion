import opening_hours
import pytest


@pytest.mark.parametrize(
    ("horaires", "expected"),
    [
        ([], "PH off"),
        (
            [
                {
                    "jour": 1,
                    "horaireFerme": "N",
                    "horaireEnContinu": "O",
                    "ouvertureMatin": "08:00",
                    "fermetureApresMidi": "18:00",
                }
            ],
            'Mo 08:00-18:00 open "Sans rendez-vous"; PH off',
        ),
        (
            [
                {
                    "jour": 2,
                    "horaireFerme": "N",
                    "horaireEnContinu": "N",
                    "ouvertureMatin": "09:00",
                    "fermetureMatin": "12:00",
                }
            ],
            'Tu 09:00-12:00 open "Sans rendez-vous"; PH off',
        ),
        (
            [
                {
                    "jour": 1,
                    "horaireFerme": "N",
                    "horaireEnContinu": "O",
                    "ouvertureMatin": "08:00",
                    "fermetureApresMidi": "18:00",
                },
                {
                    "jour": 2,
                    "horaireFerme": "N",
                    "horaireEnContinu": "N",
                    "ouvertureMatin": "09:00",
                    "fermetureMatin": "12:00",
                },
            ],
            (
                'Mo 08:00-18:00 open "Sans rendez-vous";'
                ' Tu 09:00-12:00 open "Sans rendez-vous";'
                " PH off"
            ),
        ),
        (
            [
                {
                    "jour": 3,
                    "horaireFerme": "N",
                    "horaireEnContinu": "N",
                    "ouvertureApresMidi": "14:00",
                    "fermetureApresMidi": "17:00",
                }
            ],
            'We 14:00-17:00 open "Sans rendez-vous"; PH off',
        ),
        (
            [
                {
                    "jour": 4,
                    "horaireFerme": "N",
                    "horaireEnContinu": "N",
                    "ouvertureMatin": "09:00",
                    "fermetureMatin": "12:00",
                    "ouvertureApresMidi": "14:00",
                    "fermetureApresMidi": "17:00",
                }
            ],
            'Th 09:00-12:00,14:00-17:00 open "Sans rendez-vous"; PH off',
        ),
        ([{"jour": 4, "horaireEnContinu": "N", "horaireFerme": "O"}], "PH off"),
        (
            [
                {"jour": 4, "horaireEnContinu": "N", "horaireFerme": "O"},
                {
                    "jour": 5,
                    "horaireFerme": "N",
                    "horaireEnContinu": "N",
                    "ouvertureMatin": "09:00",
                    "fermetureMatin": "12:00",
                },
            ],
            'Fr 09:00-12:00 open "Sans rendez-vous"; PH off',
        ),
        (
            [
                {
                    "jour": 1,
                    "horaireFerme": "N",
                    "horaireEnContinu": "O",
                    "ouvertureMatin": "08:00",
                    "fermetureApresMidi": "18:00",
                },
                {
                    "jour": 2,
                    "horaireFerme": "N",
                    "horaireEnContinu": "O",
                    "ouvertureMatin": "08:00",
                    "fermetureApresMidi": "18:00",
                },
                {
                    "jour": 3,
                    "horaireFerme": "N",
                    "horaireEnContinu": "O",
                    "ouvertureMatin": "08:00",
                    "fermetureApresMidi": "18:00",
                },
                {
                    "jour": 4,
                    "horaireFerme": "N",
                    "horaireEnContinu": "O",
                    "ouvertureMatin": "08:00",
                    "fermetureApresMidi": "18:00",
                },
                {
                    "jour": 5,
                    "horaireFerme": "N",
                    "horaireEnContinu": "O",
                    "ouvertureMatin": "08:00",
                    "fermetureApresMidi": "18:00",
                },
            ],
            'Mo-Fr 08:00-18:00 open "Sans rendez-vous"; PH off',
        ),
    ],
)
def test_to_osm(horaires, expected):
    assert opening_hours.to_osm(horaires) == expected
