"""This declarative module lists the sources and their streams.

It is used as a source of truth in general and to generate dedicated dags in particular.
"""

from airflow.models import Variable

from . import (
    agefiph,
    annuaire_du_service_public,
    cd35,
    dora,
    emplois_de_linclusion,
    france_travail,
    fredo,
    grist,
    mediation_numerique,
    mes_aides,
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
            "aides": {
                "filename": "aides.json",
                "url": Variable.get("MES_AIDES_AIDES_URL", None),
                "token": Variable.get("MES_AIDES_AIRTABLE_KEY", None),
            },
        },
    },
    "siao": {
        "schedule": "@once",
        "snapshot": False,
        "reader": utils.read_excel,
        "streams": {
            "etablissements": {
                "filename": "etablissements.xlsx",
                "url": Variable.get("SIAO_FILE_URL", None),
            },
        },
    },
    "finess": {
        "schedule": "@daily",
        "snapshot": True,
        "reader": lambda path: utils.read_csv(path, sep=","),
        "streams": {
            "etablissements": {
                "filename": "etablissements.xlsx",
                "url": Variable.get("FINESS_FILE_URL", None),
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
        "extractor": emplois_de_linclusion.extract,
        "streams": {
            "siaes": {
                "filename": "siaes.json",
                "url": Variable.get("EMPLOIS_API_URL", None),
                "token": Variable.get("EMPLOIS_API_TOKEN", None),
            },
            "organisations": {
                "filename": "organisations.json",
                "url": Variable.get("EMPLOIS_API_URL", None),
                "token": Variable.get("EMPLOIS_API_TOKEN", None),
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
        "snapshot": True,
        "reader": agefiph.read,
        "streams": {
            "services": {
                "filename": "services.json",
                "url": Variable.get("AGEFIPH_SERVICES_API_URL", None),
            }
        },
    },
    "data-inclusion": {
        "schedule": "@once",
        "snapshot": False,
        "reader": lambda path: utils.read_csv(path, sep=","),
        "streams": {
            "services": {
                "filename": "services.csv",
                "url": Variable.get("DI_EXTRA_SERVICES_FILE_URL", None),
            },
            "structures": {
                "filename": "structures.csv",
                "url": Variable.get("DI_EXTRA_STRUCTURES_FILE_URL", None),
            },
        },
    },
    "cd72": {
        "schedule": "@once",
        "snapshot": False,
        "extractor": grist.extract,
        "reader": lambda path: utils.read_csv(path, sep=","),
        "streams": {
            "structures": {
                "filename": "structures.csv",
                "url": Variable.get("CD72_STRUCTURES_FILE_URL", None),
                "token": Variable.get("GRIST_API_TOKEN", None),
            },
            "services": {
                "filename": "services.csv",
                "url": Variable.get("CD72_SERVICES_FILE_URL", None),
                "token": Variable.get("GRIST_API_TOKEN", None),
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
            },
            "services": {
                "filename": "services.json",
                "url": Variable.get("FT_SERVICES_TEMPLATE_URL", None),
                "reader": lambda path: utils.read_csv(path, sep=","),
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
    return utils.read_json
