import pytest
from dirty_equals import IsFloat
from inline_snapshot import snapshot

from data_inclusion.pipeline.models.intermediate.enrichments.geocodages import (
    geoplateforme,
)


@pytest.mark.integration
@pytest.mark.parametrize(
    ("input_data", "expected"),
    [
        (
            {
                "id": "1",
                "adresse": "17 rue Malus",
                "code_postal": "59000",
                "code_insee": "59350",
                "commune": "Lille",
            },
            snapshot({
                "result_status": "ok",
                "id": "1",
                "latitude": IsFloat(),
                "longitude": IsFloat(),
                "result_label": "17 Rue Malus 59000 Lille",
                "result_score": IsFloat(),
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
            }),
        ),
        (
            {
                "id": "2",
                "adresse": None,
                "code_postal": None,
                "code_insee": None,
                "commune": None,
            },
            snapshot({
                "id": "2",
                "result_status": "skipped",
            }),
        ),
        (
            {
                "id": "1",
                "adresse": "bla",
                "code_postal": None,
                "code_insee": 38009,
                "commune": "Anjou",
            },
            snapshot({
                "id": "1",
                "result_status": "ok",
                "latitude": IsFloat(),
                "longitude": IsFloat(),
                "result_label": "Anjou",
                "result_score": IsFloat(),
                "result_type": "municipality",
                "result_id": "38009",
                "result_housenumber": None,
                "result_name": "Anjou",
                "result_street": None,
                "result_postcode": "38150",
                "result_city": "Anjou",
                "result_context": "38, Isère, Auvergne-Rhône-Alpes",
                "result_citycode": "38009",
                "result_oldcitycode": None,
                "result_oldcity": None,
                "result_district": None,
            }),
        ),
        (
            {
                "id": "1",
                "adresse": "",
                "code_postal": None,
                "code_insee": 38009,
                "commune": "Anjou",
            },
            snapshot({
                "result_status": "ok",
                "id": "1",
                "latitude": IsFloat(),
                "longitude": IsFloat(),
                "result_label": "Anjou",
                "result_score": IsFloat(),
                "result_type": "municipality",
                "result_id": "38009",
                "result_housenumber": None,
                "result_name": "Anjou",
                "result_street": None,
                "result_postcode": "38150",
                "result_city": "Anjou",
                "result_context": "38, Isère, Auvergne-Rhône-Alpes",
                "result_citycode": "38009",
                "result_oldcitycode": None,
                "result_oldcity": None,
                "result_district": None,
            }),
        ),
        (
            {
                "id": "1",
                "adresse": "bourg",
                "code_postal": None,
                "code_insee": 38009,
                "commune": "Anjou",
            },
            snapshot({
                "result_status": "ok",
                "id": "1",
                "latitude": IsFloat(),
                "longitude": IsFloat(),
                "result_label": "Anjou",
                "result_score": IsFloat(),
                "result_type": "municipality",
                "result_id": "38009",
                "result_housenumber": None,
                "result_name": "Anjou",
                "result_street": None,
                "result_postcode": "38150",
                "result_city": "Anjou",
                "result_context": "38, Isère, Auvergne-Rhône-Alpes",
                "result_citycode": "38009",
                "result_oldcitycode": None,
                "result_oldcity": None,
                "result_district": None,
            }),
        ),
    ],
)
def test_geoplateforme_geocoder(input_data: dict, expected: dict):
    results = geoplateforme.geocode(
        data=[input_data],
        columns=["adresse", "code_postal", "commune"],
        filters={"citycode": "code_insee"},
    )

    assert len(results) == 1
    assert results[0].model_dump() == expected
