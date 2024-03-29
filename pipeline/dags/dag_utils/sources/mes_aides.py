import json


def extract(url: str, token: str, **kwargs) -> bytes:
    from pyairtable import api as pyairtable

    # extract airtable's base_id and table_name from url
    # cf https://support.airtable.com/docs/understanding-airtable-ids
    base_id = url.split("/")[-3]
    table_name = url.split("/")[-2]

    api = pyairtable.Api(api_key=token)
    table = api.table(base_id=base_id, table_name=table_name)
    data = table.all()
    return json.dumps(data).encode()
