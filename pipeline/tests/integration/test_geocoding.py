import pandas as pd
import pytest

from data_inclusion.scripts.tasks import geocoding

pytestmark = pytest.mark.ban_api


@pytest.fixture
def ban_backend():
    return geocoding.BaseAdresseNationaleBackend(
        base_url="https://api-adresse.data.gouv.fr"
    )


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame.from_records(
        data=[
            {
                "source": "dora",
                "id": "1",
                "adresse": "17 rue Malus",
                "code_postal": "59000",
                "commune": "Lille",
            },
            {
                "source": "dora",
                "id": "2",
                "adresse": None,
                "code_postal": None,
                "commune": None,
            },
        ]
    )


def test_ban_geocode(
    ban_backend: geocoding.BaseAdresseNationaleBackend,
    sample_df: pd.DataFrame,
):
    assert ban_backend.geocode(sample_df).to_dict(orient="records") == [
        {
            "id": "1",
            "source": "dora",
            "adresse": "17 rue Malus",
            "code_postal": "59000",
            "commune": "Lille",
            "latitude": "50.627078",
            "longitude": "3.067372",
            "result_label": "17 Rue Malus 59000 Lille",
            "result_score": "0.97",
            "result_type": "housenumber",
            "result_id": "59350_5835_00017",
            "result_housenumber": "17",
            "result_name": "Rue Malus",
            "result_street": None,
            "result_postcode": "59000",
            "result_city": "Lille",
            "result_context": "59, Nord, Hauts-de-France",
            "result_citycode": "59350",
            "result_oldcitycode": "59350",
            "result_oldcity": "Lille",
            "result_district": None,
            "result_status": "ok",
        }
    ]
