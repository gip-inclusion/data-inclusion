import pytest

from ... import factories


@pytest.fixture
def url(schema_version, path):
    return f"/api/{schema_version}{path}"


@pytest.fixture
def structure_factory(schema_version):
    return (
        factories.v0.StructureFactory
        if schema_version == "v0"
        else factories.v1.StructureFactory
    )


@pytest.fixture
def service_factory(schema_version):
    return (
        factories.v0.ServiceFactory
        if schema_version == "v0"
        else factories.v1.ServiceFactory
    )


@pytest.fixture
def factory(path, structure_factory, service_factory):
    if "structures" in path:
        return structure_factory
    elif "services" in path:
        return service_factory
