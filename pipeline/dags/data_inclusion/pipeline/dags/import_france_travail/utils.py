import json
from pathlib import Path
from urllib.parse import urljoin


def extract(url, token, id=None):
    import requests

    client_id, client_secret = token.split(":")

    token = requests.post(
        url="https://entreprise.francetravail.fr/connexion/oauth2/access_token",
        params={
            "realm": "/partenaire",
        },
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": f"application_{client_id} api_referentielagencesv1 organisationpe",
        },
    ).json()["access_token"]

    response = requests.get(
        url=urljoin(url, "partenaire/referentielagences/v1/agences?horaire=true"),
        params={},
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )

    response.raise_for_status()

    return response.content


def read(path: Path):
    """utils.df_from_json is enough but parse horaires to horaires_open_street_map
    cf: https://francetravail.io/data/api/referentiel-agences?tabgroup-api=documentation&doc-section=api-doc-section-caracteristiques
    cf: https://wiki.openstreetmap.org/wiki/Key:opening_hours
    """
    import pandas as pd

    from data_inclusion.pipeline.common import utils

    with path.open() as file:
        data = json.load(file)

    df = pd.DataFrame.from_records(data)
    return utils.df_clear_nan(df)
