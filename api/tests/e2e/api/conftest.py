import pytest

from ... import factories


@pytest.fixture
def factory(endpoint):
    if "structures" in endpoint.url:
        return factories.StructureFactory
    elif "services" in endpoint.url:
        return factories.ServiceFactory
