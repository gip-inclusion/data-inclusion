import json

ARCGIS_PAGE_SIZE = 1000


def extract(url: str, **kwargs) -> bytes:
    import requests

    all_records = []
    offset = 0

    while True:
        response = requests.get(
            url + "/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "orderByFields": "ObjectId",
                "resultRecordCount": ARCGIS_PAGE_SIZE,
                "resultOffset": offset,
            },
        )
        response.raise_for_status()
        data = response.json()

        all_records.extend(f["attributes"] for f in data["features"])

        if not data.get("exceededTransferLimit", False):
            break

        offset += ARCGIS_PAGE_SIZE

    return json.dumps(all_records).encode()


def read(path):
    import pandas as pd

    with open(path) as f:
        records = json.load(f)

    df = pd.DataFrame.from_records(records)
    df = df.astype(str)
    df = df.replace({"nan": None, "None": None})
    return df
