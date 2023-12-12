from data_inclusion.scripts.tasks import utils


def extract(id: str, url: str, **kwargs) -> bytes:
    return utils.extract_http_content(utils.safe_urljoin(url, id))
