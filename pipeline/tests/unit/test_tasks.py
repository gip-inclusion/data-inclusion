import pytest
from dags.data_inclusion.pipeline.sources import utils
from requests import HTTPError


def test_extract_http_content(requests_mock):
    requests_mock.get("http://some.url/endpoint", status_code=404)
    with pytest.raises(HTTPError):
        utils.extract_http_content("http://some.url/endpoint")

    requests_mock.get("http://some.url/endpoint", content=b"some content")
    assert utils.extract_http_content("http://some.url/endpoint") == b"some content"
