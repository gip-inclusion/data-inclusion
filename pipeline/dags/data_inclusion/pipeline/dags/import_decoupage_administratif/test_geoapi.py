import pytest

from data_inclusion.pipeline.dags.import_decoupage_administratif import geoapi


@pytest.mark.parametrize(
    "resource",
    geoapi.GeoApiClient.Resource,
)
@pytest.mark.integration
def test_geoapi_client(resource):
    client = geoapi.GeoApiClient()
    client.list_resource(resource=resource)
