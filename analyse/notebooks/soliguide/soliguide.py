from copy import deepcopy
import time
from typing import Optional

import requests
from tqdm.auto import tqdm

# Documentation on the soliguide API is available here:
# https://apisolidarite.soliguide.fr/Documentation-technique-de-l-API-Solidarit-ecaf8198f0e9400d93140b8043c9f2ce


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

bigCatDictNb = {
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
    [(int(sub), k) for k, v in bigCatDictNb.items() for sub in v]
)


class APIClient:
    def __init__(self, base_url: str, token: str, user_agent: str):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": f"JWT {token}", "User-Agent": user_agent}
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
