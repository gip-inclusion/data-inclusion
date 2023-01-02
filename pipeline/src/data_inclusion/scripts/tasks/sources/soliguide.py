import io
import json
import logging
import time
from copy import deepcopy
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests
from dateutil.parser import parse as dateutil_parse
from tqdm import tqdm

from data_inclusion.schema import models
from data_inclusion.scripts.tasks import utils

SOLIGUIDE_SOURCE_STR = "soliguide"

logger = logging.getLogger(__name__)

categories = {
    0: "Unknowed",
    100: "Santé",
    101: "Addiction",
    102: "Dépistage",
    103: "Psychologie",
    104: "Soins enfants",
    105: "Généraliste",
    106: "Dentaire",
    107: "Suivi grossesse",
    109: "Vaccination",
    110: "Infirmerie",
    111: "Vétérinaire",
    200: "Formation et emploi",
    201: "Atelier numérique",
    202: "Formation français",
    203: "Accompagnement à l'emploi",
    205: "Insertion par l'activité économique",
    206: "Soutien scolaire",
    300: "Hygiène et bien-être",
    301: "Douche",
    302: "Laverie",
    303: "Bien-être",
    304: "Toilettes",
    305: "Protections périodiques",
    306: "Masques",
    400: "Conseil",
    401: "Conseil logement",
    402: "Permanence juridique",
    403: "Domiciliation",
    404: "Accompagnement social",
    405: "Ecrivain public",
    406: "Conseil handicap",
    407: "Conseil administratif",
    408: "Conseil aux parents",
    500: "Technologie",
    501: "Ordinateur",
    502: "Wifi",
    503: "Prise",
    504: "Téléphones",
    505: "Coffre-fort numérique",
    600: "Alimentation",
    601: "Distribution de repas",
    602: "Restauration assise",
    603: "Colis Alimentaire",
    604: "Épicerie Sociale et Solidaire",
    605: "Fontaine à eau",
    700: "Accueil",
    701: "Accueil de jour",
    702: "Hébergement d'urgence",
    703: "Hébergement à long terme",
    705: "Espaces de repos",
    706: "Halte de nuit",
    707: "Garde d'enfants",
    708: "Espace famille",
    709: "Point d'information",
    710: "Hébergement citoyen",
    800: "Activités",
    801: "Sport",
    802: "Musée",
    803: "Bibliothèque",
    804: "Activités",
    805: "Animations et loisirs",
    900: "Matériel",
    901: "Bagagerie",
    902: "Magasin solidaire",
    903: "Vêtements",
    904: "Animaux",
    1100: "Spécialistes",
    1101: "Allergologie",
    1102: "Cardiologie",
    1103: "Dermatologie",
    1104: "Echographie",
    1105: "Endocrinologie",
    1106: "Gastro-entérologie",
    1107: "Gynécologie",
    1108: "Kinésithérapie",
    1109: "Mammographie",
    1110: "Ophtalmologie",
    1111: "Oto-rhino-laryngologie",
    1112: "Nutrition",
    1113: "Pédicure",
    1114: "Phlébologie",
    1115: "Pneumologie",
    1116: "Radiologie",
    1117: "Rhumatologie",
    1118: "Urologie",
    1119: "Orthophonie",
    1120: "Stomatologie",
    1121: "Osthéopathie",
    1122: "Accupuncture",
    1200: "Transport & mobilité",
    1201: "Co-voiturage",
    1202: "Mise à disposition de véhicule",
    1203: "Transport avec chauffeur",
    1204: "Aide à la mobilité",
}

big_cat_dict_nb = {
    "Santé": ["101", "102", "103", "104", "105", "106", "107", "109", "110", "111"],
    "Formation": ["201", "202", "203", "205", "206"],
    "Hygiène": ["301", "302", "303", "304", "305", "306"],
    "Conseil": ["401", "402", "403", "404", "405", "406", "407", "408"],
    "Technologie": [
        "501",
        "502",
        "503",
        "504",
        "505",
    ],
    "Alimentation": ["601", "602", "603", "604", "605"],
    "Accueil": ["701", "702", "703", "704", "705", "706", "707", "708", "709", "710"],
    "Activités": ["801", "802", "803", "804", "805"],
    "Matériel": ["901", "902", "903", "904"],
    "Spécialistes": [
        "1101",
        "1102",
        "1103",
        "1104",
        "1105",
        "1106",
        "1107",
        "1108",
        "1109",
        "1110",
        "1111",
        "1112",
        "1113",
        "1114",
        "1115",
        "1116",
        "1117",
        "1118",
        "1119",
        "1120",
        "1121",
        "1122",
    ],
    "Transport & Mobilité": ["1201", "1202", "1203", "1204"],
}

categories_by_subcategories = dict(
    [(int(sub), k) for k, v in big_cat_dict_nb.items() for sub in v]
)


class APIClient:
    # Documentation on the soliguide API is available here:
    # https://apisolidarite.soliguide.fr/Documentation-technique-de-l-API-Solidarit-ecaf8198f0e9400d93140b8043c9f2ce

    def __init__(self, base_url: str, token: str, user_agent: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"JWT {token}",
                "User-Agent": user_agent,
            }
        )

    def search(
        self,
        location_geo_type: str,
        location_geo_value: Optional[str] = None,
    ) -> dict:
        default_data = {
            "location": {
                "geoType": location_geo_type,
            },
            "options": {
                "limit": 15000,
            },
        }
        if location_geo_value is not None:
            default_data["location"]["geoValue"] = location_geo_value

        places_data = []
        page_number = 1
        pbar = None

        while True:
            data = deepcopy(default_data)
            data["options"]["page"] = page_number
            response = self.session.post(
                f"{self.base_url}/new-search",
                json=data,
            )
            response_data = response.json()

            if pbar is None:
                pbar = tqdm(
                    total=response_data["nbResults"],
                    initial=len(response_data["places"]),
                )
            else:
                pbar.update(len(response_data["places"]))

            places_data += response_data["places"]
            page_number += 1

            if len(places_data) >= response_data["nbResults"]:
                break
            elif len(response_data["places"]) == 0:
                break

            # give some slack to the soliguide api
            time.sleep(10)

        if pbar is not None:
            pbar.close()

        return places_data


def extract_data(
    src: str, token: str, user_agent: str, **kwargs
) -> dict[str, io.BytesIO]:
    client = APIClient(base_url=src, token=token, user_agent=user_agent)

    # raw places
    data = client.search(
        location_geo_type="pays",
        location_geo_value="france",
    )

    with io.StringIO() as buf:
        json.dump(data, buf)
        return {"data.json": io.BytesIO(buf.getvalue().encode())}


def read_data(path: Path) -> tuple[pd.DataFrame, Optional[pd.Series]]:
    df = utils.read_json(path)
    return df, df.lieu_id


def transform_data(input_df: pd.DataFrame) -> pd.DataFrame:
    raw_places_df = utils.deserialize_df_data(input_df)

    utils.log_df_info(raw_places_df)
    structures_df = transform_structure_dataframe(raw_places_df)
    utils.log_df_info(structures_df)
    services_df = transform_service_dataframe(raw_places_df)
    utils.log_df_info(services_df)

    return pd.concat(
        [utils.serialize_df_data(df) for df in [structures_df, services_df]]
    )


def transform_structure_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = pd.json_normalize(input_df.to_dict(orient="records"))

    output_df = pd.DataFrame()

    if len(input_df) == 0:
        return output_df

    input_df = input_df.replace("", None)

    # id
    output_df = output_df.assign(id=input_df["lieu_id"].astype(str))

    # siret
    output_df = output_df.assign(siret=None)

    # rna
    output_df = output_df.assign(rna=None)

    # nom
    output_df = output_df.assign(nom=input_df["name"])

    # commune
    output_df = output_df.assign(commune=input_df["ville"])

    # code_postal
    output_df = output_df.assign(code_postal=input_df["position.codePostal"])

    # code_insee
    # the field is very poorly filled in the source
    output_df = output_df.assign(code_insee=None)

    # adresse
    output_df = output_df.assign(adresse=input_df["position.adresse"])

    # complement_adresse
    output_df = output_df.assign(
        complement_adresse=input_df["position.complementAdresse"]
    )

    # longitude
    output_df = output_df.assign(longitude=input_df["position.coordinates.x"])

    # latitude
    output_df = output_df.assign(latitude=input_df["position.coordinates.y"])

    # typologie
    def potential_typologie_from_name(s: str):
        # get a plausible typologie from the structure name
        # there could be several : get the first match
        potential_typologies_dict = {
            models.Typologie.MUNI: s.startswith("mairie de")
            and "ccas" not in s
            and "service" not in s,
            models.Typologie.PE: "pôle emploi" in s,
            models.Typologie.CAF: "caf" in s.split()
            or "allocation" in s
            and "familiales" in s,
            models.Typologie.ASSO: "association" in s,
            models.Typologie.CCAS: "ccas" in s,
            models.Typologie.CAARUD: "caarud" in s,
            models.Typologie.HUDA: "huda" in s.split(),
            models.Typologie.RS_FJT: "fjt" in s
            or "f.j.t" in s
            or ("sidence sociale" in s)
            or s.startswith("rs "),
        }
        return next(
            (k for k, v in potential_typologies_dict.items() if v),
            models.Typologie.Autre,
        ).value

    output_df = output_df.assign(
        typologie=input_df["name"]
        .str.lower()
        .map(lambda s: potential_typologie_from_name(s))
    )

    # telephone
    output_df = output_df.assign(
        telephone=input_df["entity.phones"].map(
            lambda o: o[0]["phoneNumber"] if len(o) > 0 else None
        )
    )

    # courriel
    output_df = output_df.assign(courriel=input_df["entity.mail"])

    # site_web
    output_df = output_df.assign(site_web=input_df["entity.website"])

    # presentation_resume
    # presentation_detail
    output_df = output_df.assign(
        presentation_resume=input_df["description"].map(
            lambda s: (s if len(s) <= 280 else s[:279] + "…") if s is not None else None
        ),
        presentation_detail=input_df["description"].map(
            lambda s: (None if len(s) <= 280 else s) if s is not None else None
        ),
    )

    # source
    output_df = output_df.assign(source=SOLIGUIDE_SOURCE_STR)

    # date_maj
    output_df = output_df.assign(
        date_maj=input_df["updatedAt"].map(lambda s: dateutil_parse(s).isoformat())
    )

    # antenne
    output_df = output_df.assign(antenne=False)

    # lien_source
    output_df = output_df.assign(
        lien_source=input_df.seo_url.map(lambda s: f"https://soliguide.fr/fiche/{s}")
    )

    # horaires_ouverture
    output_df = output_df.assign(horaires_ouverture=None)

    # accessibilite
    output_df = output_df.assign(accessibilite=None)

    # labels_nationaux
    def potential_labels_from_name(s: str):
        # get plausible labels from the structure name
        potential_labels_dict = {
            models.LabelNational.MISSION_LOCALE: "mission" in s and "locale" in s,
            models.LabelNational.CAF: "caf" in s.split()
            or "allocation" in s
            and "familiales" in s,
            models.LabelNational.SECOURS_POPULAIRE: "secours populaire" in s,
            models.LabelNational.CROIX_ROUGE: "croix rouge" in s,
            models.LabelNational.FRANCE_SERVICE: "france service" in s,
        }
        return [k.value for k, v in potential_labels_dict.items() if v]

    output_df = output_df.assign(
        labels_nationaux=input_df["name"]
        .str.lower()
        .map(lambda s: potential_labels_from_name(s))
    )

    # labels_autres
    output_df = output_df.assign(labels_autres=None)

    # thematiques
    output_df = output_df.assign(thematiques=None)

    return output_df


def transform_service_dataframe(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df = input_df.replace("", None)

    output_df = pd.DataFrame()

    if len(input_df) == 0:
        return output_df

    input_df = pd.json_normalize(
        input_df.to_dict(orient="records"),
        record_path="services_all",
        meta="lieu_id",
    )

    # id
    output_df = output_df.assign(id=input_df["serviceObjectId"])

    # structure_id
    output_df = output_df.assign(structure_id=input_df["lieu_id"])

    # source
    output_df = output_df.assign(source=SOLIGUIDE_SOURCE_STR)

    # nom
    output_df = output_df.assign(nom=input_df["name"])

    # presentation_resume
    output_df = output_df.assign(
        presentation_resume=input_df["description"].map(
            lambda s: (s if len(s) <= 280 else s[:279] + "…") if s is not None else None
        )
    )

    # TODO: map soliguide categories to data.inclusion types/thematiques

    # types
    output_df = output_df.assign(types=None)

    # thematiques
    thematique_by_categorie = {
        "Transport & Mobilité": models.Thematique.MOBILITE.value,
        "Technologie": models.Thematique.NUMERIQUE.value,
    }

    output_df = output_df.assign(
        thematiques=input_df["categorie"].map(
            lambda code: [thematique_by_categorie[categories_by_subcategories[code]]]
            if categories_by_subcategories[code] in thematique_by_categorie
            else []
        )
    )

    # prise_rdv
    output_df = output_df.assign(prise_rdv=None)

    # frais
    output_df = output_df.assign(frais=None)

    # frais_autres
    output_df = output_df.assign(frais_autres=input_df["modalities.price.precisions"])

    # profils
    output_df = output_df.assign(profils=None)

    output_df = output_df.replace([np.nan, ""], None)

    return output_df
