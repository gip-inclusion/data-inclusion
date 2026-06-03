def extract(url: str, token: str, **kwargs) -> bytes:
    """Download a Grist table as CSV.

    `url` is the full Grist REST "download as csv" endpoint, e.g.
    https://grist.numerique.gouv.fr/o/docs/api/docs/<docId>/download/csv?tableId=<tableId>

    cf: https://support.getgrist.com/api/#tag/docs/operation/downloadCSV
    """
    import requests

    response = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    response.raise_for_status()
    return response.content
