"""This declarative module lists the sources and their streams.

It is used as a source of truth in general and to generate dedicated dags in particular.
"""

from airflow.models import Variable

from . import (
    agefiph,
    annuaire_du_service_public,
    carif_oref,
    cd35,
    dora,
    emplois_de_linclusion,
    france_travail,
    fredo,
    ma_boussole_aidants,
    mediation_numerique,
    mes_aides,
    mission_locale,
    reseau_alpha,
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
        "extractor": cd35.extract,
        "streams": {
            "organisations": {
                "filename": "organisations.json",
                "url": "https://services1.arcgis.com/jGLANYlFVVx3nuxa/arcgis/rest/services/annuaire_social_CD35/FeatureServer/0/query",
            },
        },
    },
    "emplois-de-linclusion": {
        "schedule": "@daily",
        "streams": {
            "organisations": {
                "filename": "organisations.json",
                "url": "https://emplois.inclusion.beta.gouv.fr/api/v1/structures/",
                "token": Variable.get("EMPLOIS_API_TOKEN", None),
                "extractor": emplois_de_linclusion.extract_organisations,
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
                "url": "https://cartographie.societenumerique.gouv.fr/api/v0/",
            },
            "services": {
                "filename": "services.json",
                "url": "https://cartographie.societenumerique.gouv.fr/api/v0/",
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
    "reseau-alpha": {
        "schedule": "@once",
        "streams": {
            "structures": {
                "filename": "structures.tar.gz",
                "url": "https://www.reseau-alpha.org",
                "extractor": reseau_alpha.extract_structures,
                "reader": reseau_alpha.read_structures,
            },
            "formations": {
                "filename": "formations.tar.gz",
                "url": "https://www.reseau-alpha.org",
                "extractor": reseau_alpha.extract_formations,
                "reader": reseau_alpha.read_formations,
            },
        },
    },
    "agefiph": {
        "schedule": "@daily",
        "extractor": agefiph.extract,
        "streams": {
            "services": {
                "filename": "services.json",
                "url": "https://www.agefiph.fr/jsonapi/node/solution",
                "reader": agefiph.read,
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
                "reader": france_travail.read,
            },
            "services": {
                "filename": "services.json",
                "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vR0yWGydubfpeoC6uDSlWrvsNE3lq0KiC-i4Qg2IwJXYAGgxrCP6G-iTv4m7Z668LcAbqvAeV4CQztz/pub?gid=1682557473&single=true&output=csv",
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
                "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vTUGrgfisHO0mBz_6_SD3meXkU-GM_uY75jEMt6Y1C8I0fRmlNaL2Jd1bhQXAXEYZ9Wg4XJyKPFN0wa/pub?gid=636818394&single=true&output=csv",
            },
            "structures": {
                "filename": "structures.csv",
                "url": "https://docs.google.com/spreadsheets/d/e/2PACX-1vTUGrgfisHO0mBz_6_SD3meXkU-GM_uY75jEMt6Y1C8I0fRmlNaL2Jd1bhQXAXEYZ9Wg4XJyKPFN0wa/pub?gid=1318485024&single=true&output=csv",
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
    "odspep": {},
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
