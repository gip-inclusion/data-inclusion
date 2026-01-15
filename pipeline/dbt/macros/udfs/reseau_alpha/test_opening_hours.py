import opening_hours
import pytest


@pytest.mark.parametrize(
    ("horaires", "expected"),
    [
        ([], None),
        (
            [
                {
                    "jour": "lundi",
                    "dateFin": None,
                    "dateDebut": "1970-01-01 09:30",
                },
            ],
            "Mo open",
        ),
        (
            [
                {
                    "jour": "lundi",
                    "dateFin": "1970-01-01 11:30",
                    "dateDebut": "1970-01-01 09:30",
                },
                {
                    "jour": "jeudi",
                    "dateFin": "1970-01-01 12:30",
                    "dateDebut": "1970-01-01 09:30",
                },
            ],
            "Mo 09:30-11:30 open; Th 09:30-12:30 open",
        ),
        pytest.param(
            [
                {
                    "jour": "lundi",
                    "dateFin": "1970-01-01 11:30",
                    "dateDebut": "1970-01-01 09:30",
                },
                {
                    "jour": "lundi",
                    "dateFin": "1970-01-01 17:30",
                    "dateDebut": "1970-01-01 12:30",
                },
            ],
            "Mo 09:30-11:30,12:30-17:30 open",
            # TODO: normalize this using a package like
            # https://github.com/remi-dupre/opening-hours-rs/tree/master/opening-hours-py
            marks=pytest.mark.xfail(reason="Currently not supported"),
        ),
        (
            [
                {
                    "jour": "lundi",
                    "dateFin": "1970-01-01 21:00",
                    "dateDebut": "1970-01-01 19:30",
                },
                {
                    "jour": "jeudi",
                    "dateFin": "1970-01-01 21:00",
                    "dateDebut": "1970-01-01 19:30",
                },
                {
                    "jour": "vendredi",
                    "dateFin": "1970-01-01 21:00",
                    "dateDebut": "1970-01-01 19:30",
                },
            ],
            "Mo,Th-Fr 19:30-21:00 open",
        ),
    ],
)
def test_to_osm(horaires, expected):
    assert opening_hours.to_osm(horaires) == expected
