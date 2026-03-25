import pendulum
import polars as pl
import pytest
from dirty_equals import IsDatetime, IsPartialDict

from data_inclusion.pipeline.models.intermediate.enrichments.geocodages import (
    data_tests,
    geoplateforme,
    model,
)

communes_df = pl.DataFrame(
    [
        {"nom": "Lille", "code": "59350", "codes_postaux": ["59000"]},
        {"nom": "Paris", "code": "75056", "codes_postaux": ["75010"]},
    ],
)


def geocode(data: list[dict], **_) -> list[geoplateforme.GeocodeCsvResponseRow]:
    placeholder = "lorem ipsum"
    return [
        geoplateforme.OkRow(
            result_status="ok",
            id=adresse["id"],
            latitude=50.0,
            longitude=3.0,
            result_label=adresse["adresse"],
            result_score=1.0,
            result_type="housenumber",
            result_id="59350_5835_00017",
            result_housenumber=placeholder,
            result_name=placeholder,
            result_street=placeholder,
            result_postcode=placeholder,
            result_city=adresse["commune"],
            result_context=placeholder,
            result_citycode=adresse["code_insee"],
            result_oldcitycode=None,
            result_oldcity=None,
            result_district=None,
        )
        for adresse in data
    ]


@pytest.mark.parametrize(
    ("adresse", "expected"),
    [
        ("17 rue Malus", "17 rue Malus"),
        ("6 bd St Denis", "6 boulevard St Denis"),
        ("3 Rte de Cobonne", "3 route de Cobonne"),
        ("Avenue de l'U.R.S.S.", "Avenue de l'URSS"),
        ("Ville-Nouvelle", "Ville Nouvelle"),
        ("CS 23123 3 route de Cobonne", "3 route de Cobonne"),
        ("3 route de Cobonne bp 7872387", "3 route de Cobonne"),
        ("3 route de Cobonne CS 23123", "3 route de Cobonne"),
    ],
)
def test_preprocess_adresse(adresse, expected):
    assert model.preprocess_adresse(adresse) == expected


@pytest.mark.parametrize(
    ("commune", "expected"),
    [
        ("Lille cedex 12345", "Lille"),
    ],
)
def test_preprocess_commune(commune, expected):
    assert model.preprocess_commune(commune) == expected


@pytest.mark.parametrize(
    ("existing", "adresse", "expected"),
    [
        pytest.param(
            {
                "adresse_id": "1",
                "input_adresse": "17 rue Malus",
                "input_code_postal": "59000",
                "input_code_insee": "59350",
                "input_commune": "Lille",
                "type": "housenumber",
                "geocoded_at": pendulum.now() - pendulum.duration(days=10),
                "latitude": 50.0,
                "longitude": 3.0,
                "code_commune": "59350",
                "commune": "Lille",
                "adresse": "17 rue Malus",
                "code_postal": "59000",
                "code_arrondissement": None,
                "score": 1.0,
            },
            {
                "id": "1",
                "adresse": "17 rue Malus",
                "code_postal": "59000",
                "code_insee": "59350",
                "commune": "Lille",
            },
            IsPartialDict({
                "geocoded_at": IsDatetime(
                    approx=pendulum.now() - pendulum.duration(days=10),
                    iso_string=True,
                )
            }),
            id="unchanged row should not be re-geocoded",
        ),
        pytest.param(
            {
                "adresse_id": "1",
                "input_adresse": "18 rue Malus",
                "input_code_postal": "59000",
                "input_code_insee": "59350",
                "input_commune": "Lille",
                "type": "housenumber",
                "geocoded_at": pendulum.now().start_of("year"),
                "latitude": 50.0,
                "longitude": 3.0,
                "code_commune": "59350",
                "commune": "Lille",
                "adresse": "17 rue Malus",
                "code_postal": "59000",
                "code_arrondissement": None,
                "score": 1.0,
            },
            {
                "id": "1",
                "adresse": "17 rue Malus",
                "code_postal": "59000",
                "code_insee": "59350",
                "commune": "Lille",
            },
            IsPartialDict({
                "geocoded_at": IsDatetime(
                    approx=pendulum.now(),
                    iso_string=True,
                    delta=pendulum.duration(seconds=10),
                )
            }),
            id="changed row should be re-geocoded",
        ),
    ],
)
def test_int__geocodages_incremental(existing: dict, adresse: dict, expected: dict):
    existing_df = pl.DataFrame([existing])
    adresses_df = pl.DataFrame([adresse])

    results_df = model.int__geocodages(
        adresses_df=adresses_df,
        communes_df=communes_df,
        existing_df=existing_df,
        geocode=geocode,
    )

    assert results_df.to_dicts()[0] == expected

    data_tests.int__geocodages(adresses_df=adresses_df, geocodages_df=results_df)


@pytest.mark.parametrize(
    ("adresse", "expected"),
    [
        pytest.param(
            {
                "id": "1",
                "adresse": "17 rue Malus BP 12345",
                "code_postal": "59000",
                "code_insee": "59350",
                "commune": "Lille",
            },
            IsPartialDict({"input_adresse": "17 rue Malus"}),
            id="boite postale should be removed from adresse before geocoding",
        ),
        pytest.param(
            {
                "id": "1",
                "adresse": "17 rue Malus",
                "code_postal": "59000",
                "code_insee": "59350",
                "commune": "Lille cedex 12345",
            },
            IsPartialDict({"input_commune": "Lille"}),
            id="cedex should be removed from commune before geocoding",
        ),
        pytest.param(
            {
                "id": "1",
                "adresse": "17 rue Malus",
                "code_postal": "99999",
                "code_insee": "59350",
                "commune": "Lille",
            },
            IsPartialDict({"input_code_postal": None}),
            id="invalid postal code should be removed before geocoding",
        ),
        pytest.param(
            {
                "id": "1",
                "adresse": "6 bvd St Denis",
                "code_postal": "75010",
                "code_insee": "75056",
                "commune": "Paris",
            },
            IsPartialDict({"input_adresse": "6 boulevard St Denis"}),
            id="abbreviated street type should not be removed before geocoding",
        ),
        pytest.param(
            {
                "id": "1",
                "adresse": "6 boulevard Saint-Denis",
                "code_postal": "75010",
                "code_insee": "75056",
                "commune": "Paris",
            },
            IsPartialDict({"input_adresse": "6 boulevard Saint Denis"}),
            id="punctuation should be removed from street name before geocoding",
        ),
        pytest.param(
            {
                "id": "1",
                "adresse": "6 boulevard St-Denis",
                "code_postal": None,
                "code_insee": None,
                "commune": "Paris",
            },
            None,
            id="almost empty rows should be removed from street name before geocoding",
        ),
    ],
)
def test_int__geocodages_preprocess(adresse, expected):
    adresses_df = pl.DataFrame([adresse])

    results_df = model.int__geocodages(
        adresses_df=adresses_df,
        communes_df=communes_df,
        geocode=geocode,
    )

    if expected is None:
        assert len(results_df) == 0
    else:
        assert results_df.to_dicts()[0] == expected


@pytest.mark.parametrize(
    ("geocodage", "expected"),
    [
        pytest.param(
            geoplateforme.OkRow(
                result_status="ok",
                id="1",
                latitude=50.0,
                longitude=3.0,
                result_label="6 boulevard St Denis",
                result_score=1.0,
                result_type="housenumber",
                result_id="59350_5835_00017",
                result_housenumber="lorem",
                result_name="lorem",
                result_street="lorem",
                result_postcode="lorem",
                result_city="Paris",
                result_context="lorem",
                result_citycode="75110",
                result_oldcitycode=None,
                result_oldcity=None,
                result_district=None,
            ),
            IsPartialDict({"code_commune": "75056"}),
            id="geocodage should be post-processed to match output schema",
        ),
        pytest.param(
            geoplateforme.SkippedRow(
                result_status="skipped",
                id="1",
            ),
            None,
            id="skipped row should be filtered out",
        ),
        pytest.param(
            geoplateforme.ErrorRow(
                result_status="error",
                id="1",
            ),
            None,
            id="row in error should be filtered out",
        ),
    ],
)
def test_int__geocodages_postprocess(geocodage, expected):
    adresses_df = pl.DataFrame([
        {
            "id": "1",
            "adresse": "6 boulevard St Denis",
            "code_postal": "75010",
            "code_insee": "75056",
            "commune": "Paris",
        }
    ])

    def geocode(*args, **kwargs):
        return [geocodage]

    results_df = model.int__geocodages(
        adresses_df=adresses_df,
        communes_df=communes_df,
        geocode=geocode,
    )

    if expected is None:
        assert len(results_df) == 0
    else:
        assert results_df.to_dicts()[0] == expected
