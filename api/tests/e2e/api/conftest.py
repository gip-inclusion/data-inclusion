import pytest


@pytest.fixture
def url(schema_version, path):
    return f"/api/{schema_version}{path}"
