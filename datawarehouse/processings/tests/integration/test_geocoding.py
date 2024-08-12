from unittest.mock import ANY

import pytest

from data_inclusion.processings import geocode

pytestmark = pytest.mark.ban_api


@pytest.mark.parametrize(
    ("adresse", "expected"),
    [
        (
            {
                "id": "1",
                "adresse": "17 rue Malus",
                "code_postal": "59000",
                "code_insee": "59350",
                "commune": "Lille",
            },
            {
                "id": "1",
                "adresse": "17 rue Malus",
                "code_postal": "59000",
                "code_insee": "59350",
                "commune": "Lille",
                "latitude": "50.627078",
                "longitude": "3.067372",
                "result_label": "17 Rue Malus 59000 Lille",
                "result_score": ANY,
                "result_score_next": None,
                "result_type": "housenumber",
                "result_id": "59350_5835_00017",
                "result_housenumber": "17",
                "result_name": "17 Rue Malus",
                "result_street": "Rue Malus",
                "result_postcode": "59000",
                "result_city": "Lille",
                "result_context": "59, Nord, Hauts-de-France",
                "result_citycode": "59350",
                "result_oldcitycode": "59350",
                "result_oldcity": "Lille",
                "result_district": None,
                "result_status": "ok",
            },
        ),
        (
            {
                "id": "2",
                "adresse": None,
                "code_postal": None,
                "code_insee": None,
                "commune": None,
            },
            None,
        ),
    ],
)
def test_ban_geocode(adresse: dict, expected: dict):
    result_df = geocode(data=adresse)

    assert result_df == ([expected] if expected is not None else [])
