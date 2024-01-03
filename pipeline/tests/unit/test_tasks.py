import pytest
from requests import HTTPError

from dags.dag_utils.sources import utils as task_utils


def test_extract_http_content(requests_mock):
    requests_mock.get("http://some.url/endpoint", status_code=404)
    with pytest.raises(HTTPError):
        task_utils.extract_http_content("http://some.url/endpoint")

    requests_mock.get("http://some.url/endpoint", content=b"some content")
    assert (
        task_utils.extract_http_content("http://some.url/endpoint") == b"some content"
    )
