import opening_hours
import pytest


@pytest.mark.parametrize(
    ("horaires", "expected"),
    [
        (
            {
                "friday": {"open": False, "timeslot": []},
                "monday": {"open": False, "timeslot": []},
                "sunday": {"open": False, "timeslot": []},
                "tuesday": {"open": False, "timeslot": []},
                "saturday": {"open": False, "timeslot": []},
                "thursday": {"open": False, "timeslot": []},
                "wednesday": {"open": False, "timeslot": []},
                "description": "",
                "closedHolidays": "UNKNOWN",
            },
            "Mo closed, Tu closed, We closed, Th closed, Fr closed, Sa closed, Su closed",  # noqa: E501
        ),
        (
            {
                "friday": {
                    "day": [],
                    "open": True,
                    "timeslot": [{"end": 1545, "start": 830}],
                },
                "monday": {
                    "day": [],
                    "open": True,
                    "timeslot": [{"end": 1700, "start": 830}],
                },
                "sunday": {"day": [], "open": False, "timeslot": []},
                "tuesday": {
                    "day": [],
                    "open": True,
                    "timeslot": [{"end": 1700, "start": 830}],
                },
                "saturday": {"day": [], "open": False, "timeslot": []},
                "thursday": {
                    "day": [],
                    "open": True,
                    "timeslot": [{"end": 1700, "start": 830}],
                },
                "wednesday": {
                    "day": [],
                    "open": True,
                    "timeslot": [{"end": 1700, "start": 830}],
                },
                "closedHolidays": "UNKNOWN",
            },
            "Mo 08:30-17:00 open, Tu 08:30-17:00 open, We 08:30-17:00 open, Th 08:30-17:00 open, Fr 08:30-15:45 open, Sa closed, Su closed",  #  noqa: E501
        ),
    ],
)
def test_to_osm(horaires, expected):
    assert opening_hours.to_osm(horaires) == expected
