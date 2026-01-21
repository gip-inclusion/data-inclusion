"""This declarative module lists the sources and their streams.

It is used as a source of truth in general and to generate dedicated dags in particular.
"""

from airflow.models import Variable

from . import (
    agefiph,
    annuaire_du_service_public,
    carif_oref,
    dora,
    emplois_de_linclusion,
    france_travail,
    fredo,
    ma_boussole_aidants,
    mediation_numerique,
    mes_aides,
    mission_locale,
    soliguide,
    un_jeune_une_solution,
    utils,
)

SOURCES_CONFIGS = {
    "dora": {
        # Hourly between 5am and 10pm
        "schedule": "0 5-22 * * *",
        "extractor": dora.extract,
        "streams": {
            "structures": {
                "filename": "structures.json",
                "url": "https://api.dora.inclusion.gouv.fr/api/v2/",
                "token": Variable.get("DORA_API_TOKEN", None),
            },
            "services": {
                "filename": "services.json",
                "url": "https://api.dora.inclusion.gouv.fr/api/v2/",
                "token": Variable.get("DORA_API_TOKEN", None),
            },
        },
    },
    "ma-boussole-aidants": {
        "schedule": "@daily",
        "extractor": ma_boussole_aidants.extract,
        "streams": {
            "structures": {
                "filename": "structures.json",
                "url": "https://solutionsv2-back.maboussoleaidants.fr/api/",
                "token": Variable.get("MA_BOUSSOLE_AIDANTS_API_KEY", None),
            },
        },
    },
    "mes-aides": {
        "schedule": "@daily",
        "extractor": mes_aides.extract,
        "streams": {
            "garages": {
                "filename": "garages.json",
                "url": "https://airtable.com/appEvva5gyqqoQRnr/tblnGf4Y5EUEeVHtJ",
                "token": Variable.get("MES_AIDES_AIRTABLE_KEY", None),
            },
            "permis_velo": {
                "filename": "permis_velo.json",
                "url": "https://airtable.com/appoYjASNOp90Ryy5/tblN4m8Ayzxzgxl9W",
                "token": Variable.get("MES_AIDES_AIRTABLE_KEY", None),
            },
        },
    },
    "fredo": {
        "schedule": "@daily",
        "extractor": fredo.extract,
        "streams": {
            "structures": {
                "filename": "structures.json",
                "url": "https://api.fredo.re/v1/structures",
                "token": Variable.get("FREDO_API_TOKEN", None),
            },
        },
    },
    "cd35": {
        "schedule": "@daily",
        "streams": {
            "organisations": {
                "filename": "organisations.csv",
                "url": "https://data.ille-et-vilaine.fr/datastore/dump/3c349ef5-725a-4c5a-bb3c-95eac4a0622a?bom=True",
                "reader": lambda path: utils.read_csv(path, sep=","),
            },
        },
    },
    "emplois-de-linclusion": {
        "schedule": "@daily",
        "streams": {
            "organisations": {
                "filename": "organisations.json",
                "url": "https://emplois.inclusion.beta.gouv.fr/api/v1/data-inclusion/",
                "token": Variable.get("EMPLOIS_API_TOKEN", None),
                "extractor": emplois_de_linclusion.extract_organisations,
            },
            "siaes": {
                "filename": "siaes.json",
                "url": "https://emplois.inclusion.beta.gouv.fr/api/v1/data-inclusion/",
                "token": Variable.get("EMPLOIS_API_TOKEN", None),
                "extractor": emplois_de_linclusion.extract_siaes,
            },
        },
    },
    "un-jeune-une-solution": {
        "schedule": "@daily",
        "extractor": un_jeune_une_solution.extract,
        "streams": {
            "benefits": {
                "filename": "benefits.json",
                "url": "https://mes-aides.1jeune1solution.beta.gouv.fr/api/",
            },
            "institutions": {
                "filename": "institutions.json",
                "url": "https://mes-aides.1jeune1solution.beta.gouv.fr/api/",
            },
        },
    },
    "annuaire-du-service-public": {
        "schedule": "@daily",
        "reader": annuaire_du_service_public.read,
        "streams": {
            "etablissements": {
                "filename": "etablissements.json",
                "url": "https://www.data.gouv.fr/fr/datasets/r/73302880-e4df-4d4c-8676-1a61bb997f3d",
            },
        },
    },
    "mediation-numerique": {
        "schedule": "@daily",
        "extractor": mediation_numerique.extract,
        "streams": {
            "structures": {
                "filename": "structures.json",
                "url": "https://api.inclusion-numerique.anct.gouv.fr/carto",
                "token": Variable.get("MEDIATION_NUMERIQUE_API_TOKEN", None),
            },
        },
    },
    "soliguide": {
        "schedule": "@hourly",
        "extractor": soliguide.extract,
        "reader": soliguide.read,
        "streams": {
            "lieux": {
                "filename": "lieux.json",
                "url": "https://api.soliguide.fr/",
                "token": Variable.get("SOLIGUIDE_API_TOKEN", None),
            }
        },
    },
    "reseau-alpha": {},
    "agefiph": {
        "schedule": "@daily",
        "extractor": agefiph.extract,
        "streams": {
            "services": {
                "filename": "services.json",
                "url": "https://www.agefiph.fr/jsonapi/node/solution",
            },
            "structures": {
                "filename": "structures.json",
                "url": "https://www.agefiph.fr/jsonapi/node/contact",
            },
        },
    },
    "france-travail": {
        "schedule": "@daily",
        "streams": {
            "agences": {
                "filename": "agences.json",
                "url": "https://api.francetravail.io/",
                # the "request token" is the client_id:client_secret string.
                "token": Variable.get("FT_API_TOKEN", None),
                "extractor": france_travail.extract,
            },
            "services": {
                "filename": "services.csv",
                "url": "https://docs.google.com/spreadsheets/d/1g5p4CAxVnXk5kTTTjL_v9cDpewE57bg0bcXR9uTR5QM/export?gid=636818394&format=csv",
                "reader": lambda path: utils.read_csv(path, sep=","),
            },
        },
    },
    "action-logement": {
        "schedule": "@daily",
        "reader": lambda path: utils.read_csv(path, sep=","),
        "streams": {
            "services": {
                "filename": "services.csv",
                "url": "https://docs.google.com/spreadsheets/d/1Rh3Vrlr0ISTVmD9Ui5TYUR5oLUnfUfMcJiMstXurSCM/export?gid=636818394&format=csv",
            },
            "structures": {
                "filename": "structures.csv",
                "url": "https://docs.google.com/spreadsheets/d/1Rh3Vrlr0ISTVmD9Ui5TYUR5oLUnfUfMcJiMstXurSCM/export?gid=1318485024&format=csv",
            },
        },
    },
    "carif-oref": {
        # file updated by carif oref every sunday at 6:25am
        "schedule": "@weekly",
        "extractor": carif_oref.extract,
        "reader": carif_oref.read,
        "streams": {
            "formations": {
                "filename": "formations.xml",
                "url": Variable.get("CARIF_OREF_URL", None),
            }
        },
    },
    "monenfant": {},
    "mission-locale": {
        "schedule": "@daily",
        "extractor": mission_locale.extract,
        "streams": {
            "offres": {
                "filename": "offres.json",
                "url": "https://api-ods.dsiml.org",
                "token": Variable.get("MISSION_LOCALE_API_SECRET", None),
            },
            "structures": {
                "filename": "structures.json",
                "url": "https://api-ods.dsiml.org",
                "token": Variable.get("MISSION_LOCALE_API_SECRET", None),
            },
            "structures_offres": {
                "filename": "structures_offres.json",
                "url": "https://api-ods.dsiml.org",
                "token": Variable.get("MISSION_LOCALE_API_SECRET", None),
            },
        },
    },
}


def get_extractor(source_id: str, stream_id: str):
    source_config = SOURCES_CONFIGS[source_id]
    stream_config = source_config["streams"][stream_id]
    if extractor := stream_config.get("extractor"):
        return extractor
    if extractor := source_config.get("extractor"):
        return extractor
    return utils.extract_http_content


def get_reader(source_id: str, stream_id: str):
    source_config = SOURCES_CONFIGS[source_id]
    stream_config = source_config["streams"][stream_id]
    if reader := stream_config.get("reader"):
        return reader
    if reader := source_config.get("reader"):
        return reader
    return utils.df_from_json
