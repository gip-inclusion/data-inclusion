import pytest

from ... import factories


@pytest.fixture
def url(schema_version, path):
    return f"/api/{schema_version}{path}"


@pytest.fixture
def structure_factory(schema_version):
    def _factory(**kwargs):
        if schema_version == "v0":
            factory = factories.v0.StructureFactory

        elif schema_version == "v1":
            factory = factories.v1.StructureFactory

            if "id" in kwargs:
                if "--" not in kwargs["id"]:
                    raise ValueError("Not a valid v1 id")
                if "source" in kwargs and not kwargs["id"].startswith(
                    kwargs["source"] + "--"
                ):
                    raise ValueError("Not a valid v1 id")

        return factory(**kwargs)

    return _factory


@pytest.fixture
def service_factory(schema_version):
    def _factory(**kwargs):
        if schema_version == "v0":
            factory = factories.v0.ServiceFactory

        elif schema_version == "v1":
            factory = factories.v1.ServiceFactory

            if "id" in kwargs:
                if "--" not in kwargs["id"]:
                    raise ValueError("Not a valid v1 id")
                if "source" in kwargs and not kwargs["id"].startswith(
                    kwargs["source"] + "--"
                ):
                    raise ValueError("Not a valid v1 id")

            if "structure_id" in kwargs:
                if "--" not in kwargs["structure_id"]:
                    raise ValueError("Not a valid v1 structure_id")
                if "source" in kwargs and not kwargs["structure_id"].startswith(
                    kwargs["source"] + "--"
                ):
                    raise ValueError("Not a valid v1 structure_id")

        return factory(**kwargs)

    return _factory


@pytest.fixture
def factory(path, structure_factory, service_factory):
    if "structures" in path:
        return structure_factory
    elif "services" in path:
        return service_factory
