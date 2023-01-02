import numpy as np
import pandas as pd
import pytest

from data_inclusion.scripts.tasks.sources import soliguide


@pytest.fixture
def soliguide_sample_df():
    return pd.json_normalize(
        pd.DataFrame(
            [
                {
                    "_id": "5c6fc96cbe73965c0d3085ec",
                    "close": {
                        "dateDebut": None,
                        "dateFin": None,
                        "closeType": 0,
                        "precision": "",
                    },
                    "codePostal": "44640",
                    "createdAt": "2019-02-22T10:05:32.456Z",
                    "departement": "Loire-Atlantique",
                    "description": "Le Centre Communal d&#8217;Action Sociale (CCAS) du Pellerin propose des aides pour faire face aux difficult&#233;s passag&#232;res et met en place des actions envers les familles, les personnes &#226;g&#233;es et/ou en situation de handicap.",
                    "entity": {
                        "mail": "ccas@ville-lepellerin.fr",
                        "facebook": None,
                        "instagram": None,
                        "fax": None,
                        "website": "http://www.ville-lepellerin.fr/ccas",
                        "phones": [
                            {"label": "CCAS", "phoneNumber": "02 40 05 69 81"},
                            {
                                "label": "France Services",
                                "phoneNumber": "06 09 32 24 16",
                            },
                        ],
                        "name": "Ville du Pellerin — France Services 44",
                    },
                    "lieu_id": 5447,
                    "location": {
                        "type": "Point",
                        "coordinates": [-1.7543631, 47.1990389],
                    },
                    "modalities": {
                        "animal": {"checked": False},
                        "appointment": {"checked": False, "precisions": ""},
                        "docs": [],
                        "inconditionnel": True,
                        "inscription": {"checked": False, "precisions": ""},
                        "orientation": {"checked": False, "precisions": ""},
                        "other": "",
                        "pmr": {"checked": False},
                        "price": {"checked": False, "precisions": ""},
                    },
                    "name": "Centre Communal d'Action Sociale (CCAS) / France Services du Pellerin",
                    "newhours": {
                        "description": "",
                        "friday": {
                            "open": True,
                            "timeslot": [{"end": 1230, "start": 900}],
                        },
                        "monday": {
                            "open": True,
                            "timeslot": [{"end": 1230, "start": 900}],
                        },
                        "saturday": {"open": False, "timeslot": []},
                        "sunday": {"open": False, "timeslot": []},
                        "thursday": {
                            "open": True,
                            "timeslot": [
                                {"end": 1230, "start": 900},
                                {"end": 1830, "start": 1400},
                            ],
                        },
                        "tuesday": {
                            "open": True,
                            "timeslot": [{"end": 1230, "start": 900}],
                        },
                        "wednesday": {
                            "open": True,
                            "timeslot": [{"end": 1230, "start": 900}],
                        },
                        "closedHolidays": "UNKOWN",
                    },
                    "pays": "France",
                    "photos": [],
                    "position": {
                        "codePostal": "44640",
                        "coordinates": {"x": -1.7543631, "y": 47.1990389},
                        "departement": "Loire-Atlantique",
                        "location": {
                            "type": "Point",
                            "coordinates": [-1.7543631, 47.1990389],
                        },
                        "pays": "France",
                        "region": "Pays de la Loire",
                        "adresse": "Mairie annexe CCAS, Allée George Sand, Le Pellerin",
                        "complementAdresse": "",
                    },
                    "publics": {
                        "accueil": 0,
                        "administrative": [],
                        "age": {"max": 99, "min": 0},
                        "description": "",
                        "familialle": [],
                        "gender": ["men", "women"],
                        "other": [],
                        "ukrainePrecisions": None,
                    },
                    "region": "Pays de la Loire",
                    "seo_url": "centre-communal-action-sociale-ccas-du-pellerin-le-pellerin-5447",
                    "services_all": [
                        {
                            "close": {
                                "actif": False,
                                "dateDebut": None,
                                "dateFin": None,
                            },
                            "saturated": {"precision": "", "statut": 0},
                            "description": "Le CCAS vous accompagne.",
                            "differentHours": False,
                            "differentModalities": False,
                            "differentPublics": True,
                            "hours": {
                                "friday": {
                                    "day": [],
                                    "week": [],
                                    "open": True,
                                    "timeslot": [{"end": 1230, "start": 900}],
                                },
                                "monday": {
                                    "day": [],
                                    "week": [],
                                    "open": True,
                                    "timeslot": [{"end": 1230, "start": 900}],
                                },
                                "saturday": {
                                    "day": [],
                                    "week": [],
                                    "open": False,
                                    "timeslot": [],
                                },
                                "sunday": {
                                    "day": [],
                                    "week": [],
                                    "open": False,
                                    "timeslot": [],
                                },
                                "thursday": {
                                    "day": [],
                                    "week": [],
                                    "open": True,
                                    "timeslot": [
                                        {"end": 1230, "start": 900},
                                        {"end": 1830, "start": 1400},
                                    ],
                                },
                                "tuesday": {
                                    "day": [],
                                    "week": [],
                                    "open": True,
                                    "timeslot": [{"end": 1230, "start": 900}],
                                },
                                "wednesday": {
                                    "day": [],
                                    "week": [],
                                    "open": True,
                                    "timeslot": [{"end": 1230, "start": 900}],
                                },
                                "closedHolidays": "UNKOWN",
                                "description": None,
                                "isTemp": False,
                            },
                            "jobsList": "",
                            "modalities": {
                                "animal": {"checked": False},
                                "appointment": {"checked": False, "precisions": None},
                                "inscription": {"checked": False, "precisions": None},
                                "orientation": {"checked": False, "precisions": None},
                                "pmr": {"checked": False},
                                "price": {"checked": False, "precisions": "Gratuit"},
                                "docs": [],
                                "inconditionnel": True,
                                "other": None,
                            },
                            "name": "CONSEIL",
                            "publics": {
                                "age": {"max": 99, "min": 0},
                                "accueil": 1,
                                "administrative": [
                                    "regular",
                                    "asylum",
                                    "refugee",
                                    "undocumented",
                                ],
                                "description": None,
                                "familialle": [
                                    "isolated",
                                    "family",
                                    "couple",
                                    "pregnant",
                                ],
                                "gender": ["men", "women"],
                                "other": ["ukraine"],
                                "ukrainePrecisions": "",
                            },
                            "categorie": 1201,
                            "serviceObjectId": "6181a8288ac6b179ffba2e66",
                        },
                    ],
                    "statut": "ONLINE",
                    "tempInfos": {
                        "hours": {
                            "actif": False,
                            "dateDebut": None,
                            "dateFin": None,
                            "description": None,
                            "value": None,
                        },
                        "message": {
                            "actif": False,
                            "dateDebut": None,
                            "dateFin": None,
                            "description": "Cette structure est susceptible de fermer pendant les vacances d'hiver. Appelez avant de vous y rendre ou d'orienter quelqu'un",
                            "name": "Vacances d'hiver",
                        },
                    },
                    "updatedAt": "2022-06-23T09:19:57.898Z",
                    "ville": "Le Pellerin",
                    "visibility": "ALL",
                    "address": "Mairie annexe CCAS, Allée George Sand, Le Pellerin, France",
                    "complementAddress": None,
                }
            ]
        ).to_dict(orient="records")
    ).replace([np.nan, ""], None)


def test_transform_soliguide_structure(soliguide_sample_df):
    df = soliguide.transform_structure_dataframe(soliguide_sample_df)

    assert df.to_dict(orient="records") == [
        {
            "id": "5447",
            "siret": None,
            "rna": None,
            "nom": "Centre Communal d'Action Sociale (CCAS) / France Services du Pellerin",
            "commune": "Le Pellerin",
            "code_postal": "44640",
            "code_insee": None,
            "adresse": "Mairie annexe CCAS, Allée George Sand, Le Pellerin",
            "complement_adresse": None,
            "longitude": -1.7543631,
            "latitude": 47.1990389,
            "typologie": "CCAS",
            "telephone": "02 40 05 69 81",
            "courriel": "ccas@ville-lepellerin.fr",
            "site_web": "http://www.ville-lepellerin.fr/ccas",
            "presentation_resume": "Le Centre Communal d&#8217;Action Sociale (CCAS) du Pellerin propose des aides pour faire face aux difficult&#233;s passag&#232;res et met en place des actions envers les familles, les personnes &#226;g&#233;es et/ou en situation de handicap.",
            "presentation_detail": None,
            "source": "soliguide",
            "date_maj": "2022-06-23T09:19:57.898000+00:00",
            "antenne": False,
            "lien_source": "https://soliguide.fr/fiche/centre-communal-action-sociale-ccas-du-pellerin-le-pellerin-5447",
            "horaires_ouverture": None,
            "accessibilite": None,
            "labels_nationaux": ["france-service"],
            "labels_autres": None,
            "thematiques": None,
        }
    ]


def test_transform_soliguide_service(soliguide_sample_df):
    df = soliguide.transform_service_dataframe(soliguide_sample_df)

    assert df.to_dict(orient="records") == [
        {
            "id": "6181a8288ac6b179ffba2e66",
            "structure_id": 5447,
            "source": "soliguide",
            "nom": "CONSEIL",
            "presentation_resume": "Le CCAS vous accompagne.",
            "types": None,
            "thematiques": ["mobilite"],
            "prise_rdv": None,
            "frais": None,
            "frais_autres": "Gratuit",
            "profils": None,
        }
    ]
