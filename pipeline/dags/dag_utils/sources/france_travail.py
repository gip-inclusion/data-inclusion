from urllib.parse import urljoin


def extract(url, token, id=None):
    import requests

    client_id, client_secret = token.split(":")

    token = requests.post(
        url="https://entreprise.pole-emploi.fr/connexion/oauth2/access_token",
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
