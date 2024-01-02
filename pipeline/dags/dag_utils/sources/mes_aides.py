import io
import json

from pyairtable import api as pyairtable


def extract(url: str, token: str, **kwargs) -> bytes:
    # extract airtable's base_id and table_name from url
    # cf https://support.airtable.com/docs/understanding-airtable-ids
    base_id = url.split("/")[-3]
    table_name = url.split("/")[-2]

    api = pyairtable.Api(api_key=token)
    table = api.table(base_id=base_id, table_name=table_name)
    data = table.all()

    with io.StringIO() as buf:
        json.dump(data, buf)
        return buf.getvalue().encode()
