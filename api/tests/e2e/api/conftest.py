import pytest

from ... import factories


@pytest.fixture
def url(schema_version, path):
    return f"/api/{schema_version}{path}"


@pytest.fixture
def factory(path):
    if "services" in path:
        return factories.ServiceFactory
    elif "structures" in path:
        return factories.StructureFactory
