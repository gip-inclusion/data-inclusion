from datetime import timedelta

import pytest

from data_inclusion.api import services
from data_inclusion.api.utils import timezone

from . import factories


class FakeSoliguideClient(services.SoliguideAPIClient):
    def __init__(self):
        self.retrieved_ids = []

    def retrieve_place(self, place_id: str):
        self.retrieved_ids.append(place_id)


@pytest.mark.parametrize(
    ("resource_type", "created_at", "expected"),
    [
        (
            "service",
            timezone.now(),
            ["soliguide-structure-id"],
        ),
        (
            "structure",
            timezone.now(),
            ["soliguide-structure-id"],
        ),
        (
            "service",
            timezone.now() - timedelta(hours=25),
            [],
        ),
    ],
)
def test_batch_forward_requests_to_soliguide(
    resource_type, created_at, expected, test_session
):
    factories.StructureFactory(source="soliguide", id="soliguide-structure-id")
    factories.ServiceFactory(
        source="soliguide",
        id="soliguide-service-id",
        structure_id="soliguide-structure-id",
    )

    factories.RequestFactory(
        endpoint_name=f"retrieve_{resource_type}_endpoint",
        path_params={"source": "soliguide", "id": f"soliguide-{resource_type}-id"},
        created_at=created_at,
    )

    soliguide_client = FakeSoliguideClient()

    services.batch_forward_requests_to_soliguide(
        db_session=test_session,
        soliguide_api_client=soliguide_client,
    )

    assert soliguide_client.retrieved_ids == expected
