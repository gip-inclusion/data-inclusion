import json


def extract(url: str, token: str, **kwargs) -> bytes:
    """Extract from airtable table.

    Args:
        url (str): URL with format https://airtable.com/app***/tbl***
    """
    from pyairtable import api as pyairtable

    base_id = url.split("/")[-2]
    table_name = url.split("/")[-1]

    api = pyairtable.Api(api_key=token)
    table = api.table(base_id=base_id, table_name=table_name)
    data = table.all()
    return json.dumps(data).encode()
