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
        "schedule": "@daily",
        "snapshot": True,
        "extractor": dora.extract,
        "streams": {
            "structures": {
                "filename": "structures.json",
                "url": Variable.get("DORA_API_URL", None),
                "token": Variable.get("DORA_API_TOKEN", None),
            },
            "services": {
                "filename": "services.json",
                "url": Variable.get("DORA_API_URL", None),
                "token": Variable.get("DORA_API_TOKEN", None),
            },
        },
    },
    "mes-aides": {
        "schedule": "@daily",
        "snapshot": True,
        "extractor": mes_aides.extract,
        "streams": {
            "garages": {
                "filename": "garages.json",
                "url": Variable.get("MES_AIDES_GARAGES_URL", None),
                "token": Variable.get("MES_AIDES_AIRTABLE_KEY", None),
            },
            "permis_velo": {
                "filename": "permis_velo.json",
                "url": Variable.get("MES_AIDES_PERMIS_VELO_URL", None),
                "token": Variable.get("MES_AIDES_AIRTABLE_KEY", None),
            },
        },
    },
    "fredo": {
        "schedule": "@daily",
        "snapshot": True,
        "extractor": fredo.extract,
        "streams": {
            "structures": {
                "filename": "structures.json",
                "url": Variable.get("FREDO_API_URL", None),
                "token": Variable.get("FREDO_API_TOKEN", None),
            },
        },
    },
    "cd35": {
        "schedule": "@daily",
        "snapshot": True,
        "extractor": cd35.extract,
        "streams": {
            "organisations": {
                "filename": "organisations.json",
                "url": Variable.get("CD35_API_URL", None),
            },
        },
    },
    "emplois-de-linclusion": {
        "schedule": "@daily",
        "snapshot": True,
        "streams": {
            "organisations": {
                "filename": "organisations.json",
                "url": Variable.get("EMPLOIS_API_URL", None),
                "token": Variable.get("EMPLOIS_API_TOKEN", None),
                "extractor": emplois_de_linclusion.extract_organisations,
            },
        },
    },
    "un-jeune-une-solution": {
        "schedule": "@daily",
        "snapshot": True,
        "extractor": un_jeune_une_solution.extract,
        "streams": {
            "benefits": {
                "filename": "benefits.json",
                "url": Variable.get("UN_JEUNE_UNE_SOLUTION_API_URL", None),
            },
            "institutions": {
                "filename": "institutions.json",
                "url": Variable.get("UN_JEUNE_UNE_SOLUTION_API_URL", None),
            },
        },
    },
    "annuaire-du-service-public": {
        "schedule": "@daily",
        "snapshot": True,
        "reader": annuaire_du_service_public.read,
        "streams": {
            "etablissements": {
                "filename": "etablissements.json",
                "url": Variable.get("ETAB_PUB_FILE_URL", None),
            },
        },
    },
    "mediation-numerique": {
        "schedule": "@daily",
        "snapshot": False,
        "extractor": mediation_numerique.extract,
        "streams": {
            "structures": {
                "filename": "structures.json",
                "url": Variable.get("MEDNUM_API_URL", None),
            },
            "services": {
                "filename": "services.json",
                "url": Variable.get("MEDNUM_API_URL", None),
            },
        },
    },
    "soliguide": {
        "schedule": "@hourly",
        "snapshot": True,
        "extractor": soliguide.extract,
        "reader": soliguide.read,
        "streams": {
            "lieux": {
                "filename": "lieux.json",
                "url": Variable.get("SOLIGUIDE_API_URL", None),
                "token": Variable.get("SOLIGUIDE_API_TOKEN", None),
            }
        },
    },
    "reseau-alpha": {
        "schedule": "@once",
        "snapshot": False,
        "streams": {
            "structures": {
                "filename": "structures.tar.gz",
                "url": Variable.get("RESEAU_ALPHA_URL", None),
                "extractor": reseau_alpha.extract_structures,
                "reader": reseau_alpha.read_structures,
            },
            "formations": {
                "filename": "formations.tar.gz",
                "url": Variable.get("RESEAU_ALPHA_URL", None),
                "extractor": reseau_alpha.extract_formations,
                "reader": reseau_alpha.read_formations,
            },
        },
    },
    "agefiph": {
        "schedule": "@daily",
        "extractor": agefiph.extract,
        "snapshot": True,
        "streams": {
            "services": {
                "filename": "services.json",
                "url": Variable.get("AGEFIPH_SERVICES_API_URL", None),
                "reader": agefiph.read,
            },
            "structures": {
                "filename": "structures.json",
                "url": Variable.get("AGEFIPH_STRUCTURES_API_URL", None),
            },
        },
    },
    "france-travail": {
        "schedule": "@daily",
        "snapshot": False,
        "streams": {
            "agences": {
                "filename": "agences.json",
                "url": Variable.get("FT_API_URL", None),
                # the "request token" is the client_id:client_secret string.
                "token": Variable.get("FT_API_TOKEN", None),
                "extractor": france_travail.extract,
                "reader": france_travail.read,
            },
            "services": {
                "filename": "services.json",
                "url": Variable.get("FT_SERVICES_TEMPLATE_URL", None),
                "reader": lambda path: utils.read_csv(path, sep=","),
            },
        },
    },
    "action-logement": {
        "schedule": "@daily",
        "snapshot": True,
        "reader": lambda path: utils.read_csv(path, sep=","),
        "streams": {
            "services": {
                "filename": "services.csv",
                "url": Variable.get("ACTION_LOGEMENT_SERVICES_URL", None),
            },
            "structures": {
                "filename": "structures.csv",
                "url": Variable.get("ACTION_LOGEMENT_STRUCTURES_URL", None),
            },
        },
    },
    "carif-oref": {
        # file updated by carif oref every sunday at 6:25am
        "schedule": "@weekly",
        "snapshot": True,
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
        "snapshot": True,
        "extractor": mission_locale.extract,
        "streams": {
            "offres": {
                "filename": "offres.json",
                "url": Variable.get("MISSION_LOCALE_API_URL", None),
                "token": Variable.get("MISSION_LOCALE_API_SECRET", None),
            },
            "structures": {
                "filename": "structures.json",
                "url": Variable.get("MISSION_LOCALE_API_URL", None),
                "token": Variable.get("MISSION_LOCALE_API_SECRET", None),
            },
            "structures_offres": {
                "filename": "structures_offres.json",
                "url": Variable.get("MISSION_LOCALE_API_URL", None),
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
