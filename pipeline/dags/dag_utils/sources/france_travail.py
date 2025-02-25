import json
from pathlib import Path
from typing import Any, Optional
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


def same_values_every_day(formated_horaires: dict[str, str]) -> bool:
    first_value = next(iter(formated_horaires.values()))
    return all(value == first_value for value in formated_horaires.values())


def format_date_ft_to_open_street_map(
    horaires: Optional[list[dict[str, Any]]],
) -> Optional[str]:
    """Format horaires from France Travail to Open Street Map format
    cf: https://francetravail.io/data/api/referentiel-agences?tabgroup-api=documentation&doc-section=api-doc-section-caracteristiques
    cf: https://wiki.openstreetmap.org/wiki/Key:opening_hours
    The object horaires also contains information on openning hours for appointment
    only.
    We could added it in this format for example:
    "Mo 12:00-14:00 open "Sans RDv", Mo 14:00-16:00 open "Sur RDV seulement"; PH off"
    Because of the complexity of the format, we will not implement it for now.
    """
    if not horaires:
        return None

    mapping_days = {
        1: "Mo",
        2: "Tu",
        3: "We",
        4: "Th",
        5: "Fr",
    }

    horaires = sorted(horaires, key=lambda x: x["jour"])

    formated_horaires = {}

    for horaire in horaires:
        if horaire["horaireFerme"] == "O":
            continue
        day = mapping_days[horaire["jour"]]
        if horaire["horaireEnContinu"] == "O":
            formated_horaires[day] = (
                f"{horaire['ouvertureMatin']}-{horaire['fermetureApresMidi']};"
            )
        elif "ouvertureApresMidi" not in horaire:
            formated_horaires[day] = (
                f"{horaire['ouvertureMatin']}-{horaire['fermetureMatin']};"
            )
        elif "ouvertureMatin" not in horaire:
            formated_horaires[day] = (
                f"{horaire['ouvertureApresMidi']}-{horaire['fermetureApresMidi']};"
            )
        else:
            formated_horaires[day] = (
                f"{horaire['ouvertureMatin']}-{horaire['fermetureMatin']},{horaire['ouvertureApresMidi']}-{horaire['fermetureApresMidi']};"
            )

    if formated_horaires == {}:
        return None

    # Simplify format for 70% of the dataset
    if len(formated_horaires) == 5 and same_values_every_day(formated_horaires):
        return f"Mo-Fr {next(iter(formated_horaires.values()))} PH off"

    open_street_map_horaires = ""
    for day, formated_horaire in formated_horaires.items():
        open_street_map_horaires += f"{day} {formated_horaire}"

    return f"{open_street_map_horaires} PH off"


def read(path: Path):
    """utils.read_json is enough but parse horaires to horaires_open_street_map
    cf: https://francetravail.io/data/api/referentiel-agences?tabgroup-api=documentation&doc-section=api-doc-section-caracteristiques
    cf: https://wiki.openstreetmap.org/wiki/Key:opening_hours
    """
    import pandas as pd

    from . import utils

    with path.open() as file:
        data = json.load(file)

    for agence in data:
        agence["horaires_open_street_map"] = (
            format_date_ft_to_open_street_map(agence["horaires"])
            if "horaires" in agence
            else None
        )

    df = pd.DataFrame.from_records(data)
    return utils.df_clear_nan(df)
