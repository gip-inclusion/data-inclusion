import requests


def get_resources_url_from_dataset_url(dataset_url: str) -> dict[str, str]:
    """Identify data.inclusion resource urls in a data.gouv dataset given its url"""

    dataset_id = dataset_url.rstrip("/").split("/")[-1]

    response = requests.get(f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}")
    dataset_data = response.json()

    # filter resources based on the data.inclusion schema
    data_inclusion_resources = [
        resource_data
        for resource_data in dataset_data["resources"]
        if resource_data["schema"]["name"] == "gip-inclusion/data-inclusion-schema"
    ]

    # identify urls based on resource titles
    return {
        "structures"
        if "structures" in resource_data["title"]
        else "services": resource_data["latest"]
        for resource_data in data_inclusion_resources
    }


def extract(id: str, url: str, **kwargs) -> bytes:
    urls = get_resources_url_from_dataset_url(dataset_url=url)
    return requests.get(urls[id]).content
