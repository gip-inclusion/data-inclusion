"""This declarative module lists the sources and their streams.

It is used as a source of truth in general and to generate dedicated dags in particular.
"""

from airflow.models import Variable

SOURCES_CONFIGS = [
    {
        "id": "dora",
        "schedule": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "structures",
                "filename": "structures.json",
                "url": Variable.get("DORA_API_URL", None),
                "token": Variable.get("DORA_API_TOKEN", None),
            },
            {
                "id": "services",
                "filename": "services.json",
                "url": Variable.get("DORA_API_URL", None),
                "token": Variable.get("DORA_API_TOKEN", None),
            },
        ],
    },
    {
        "id": "mes-aides",
        "schedule": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "garages",
                "filename": "garages.json",
                "url": Variable.get("MES_AIDES_GARAGES_URL", None),
                "token": Variable.get("MES_AIDES_AIRTABLE_KEY", None),
            },
            {
                "id": "aides",
                "filename": "aides.json",
                "url": Variable.get("MES_AIDES_AIDES_URL", None),
                "token": Variable.get("MES_AIDES_AIRTABLE_KEY", None),
            },
        ],
    },
    {
        "id": "siao",
        "schedule": "@once",
        "snapshot": False,
        "streams": [
            {
                "id": "etablissements",
                "filename": "etablissements.xlsx",
                "url": Variable.get("SIAO_FILE_URL", None),
            },
        ],
    },
    {
        "id": "finess",
        "schedule": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "etablissements",
                "filename": "etablissements.xlsx",
                "url": Variable.get("FINESS_FILE_URL", None),
            },
        ],
    },
    {
        "id": "cd35",
        "schedule": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "organisations",
                "filename": "organisations.csv",
                "url": Variable.get("CD35_FILE_URL", None),
            },
        ],
    },
    {
        "id": "emplois-de-linclusion",
        "schedule": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "siaes",
                "filename": "siaes.json",
                "url": Variable.get("EMPLOIS_API_URL", None),
                "token": Variable.get("EMPLOIS_API_TOKEN", None),
            },
            {
                "id": "organisations",
                "filename": "organisations.json",
                "url": Variable.get("EMPLOIS_API_URL", None),
                "token": Variable.get("EMPLOIS_API_TOKEN", None),
            },
        ],
    },
    {
        "id": "un-jeune-une-solution",
        "schedule": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "benefits",
                "filename": "benefits.json",
                "url": Variable.get("UN_JEUNE_UNE_SOLUTION_API_URL", None),
            },
            {
                "id": "institutions",
                "filename": "institutions.json",
                "url": Variable.get("UN_JEUNE_UNE_SOLUTION_API_URL", None),
            },
        ],
    },
    {
        "id": "annuaire-du-service-public",
        "schedule": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "etablissements",
                "filename": "etablissements.json",
                "url": Variable.get("ETAB_PUB_FILE_URL", None),
            },
        ],
    },
    *[
        {
            "id": f"mediation-numerique-{source_id}",
            "schedule": "@daily",
            "snapshot": False,
            "streams": [
                {
                    "id": "structures",
                    "filename": "structures.json",
                    "url": Variable.get(
                        f"MEDNUM_{source_id.upper().replace('-', '_')}_DATASET_URL",
                        None,
                    ),
                },
                {
                    "id": "services",
                    "filename": "services.json",
                    "url": Variable.get(
                        f"MEDNUM_{source_id.upper().replace('-', '_')}_DATASET_URL",
                        None,
                    ),
                },
            ],
        }
        for source_id in [
            "aidants-connect",
            "angers",
            "assembleurs",
            "cd17",
            "cd23",
            "cd28-appui-territorial",
            "cd33",
            "cd40",
            "cd44",
            "cd49",
            "cd85",
            "cd87",
            "conseiller-numerique",
            "conumm",
            "cr93",
            "etapes-numerique",
            "fibre-64",
            "france-services",
            "france-tiers-lieux",
            "francilin",
            "hinaura",
            "hub-antilles",
            "hub-lo",
            "mulhouse",
            "res-in",
            "rhinocc",
            "ultra-numerique",
        ]
    ],
    {
        "id": "soliguide",
        "schedule": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "lieux",
                "filename": "lieux.json",
                "url": Variable.get("SOLIGUIDE_API_URL", None),
                "token": Variable.get("SOLIGUIDE_API_TOKEN", None),
            }
        ],
    },
    {
        "id": "monenfant",
        "schedule": "@once",
        "snapshot": True,
        "streams": [
            {
                "id": "creches",
                "filename": "creches.json",
                "url": Variable.get("MONENFANT_CRECHES_FILE_URL", None),
            },
        ],
    },
    {
        "id": "reseau-alpha",
        "schedule": "@once",
        "snapshot": False,
        "streams": [
            {
                "id": "structures",
                "filename": "structures.tar.gz",
                "url": Variable.get("RESEAU_ALPHA_URL", None),
            },
            {
                "id": "formations",
                "filename": "formations.tar.gz",
                "url": Variable.get("RESEAU_ALPHA_URL", None),
            },
        ],
    },
    {
        "id": "agefiph",
        "schedule": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "services",
                "filename": "services.json",
                "url": Variable.get("AGEFIPH_SERVICES_API_URL", None),
            }
        ],
    },
    {
        "id": "data-inclusion",
        "schedule": "@once",
        "snapshot": False,
        "streams": [
            {
                "id": "services",
                "filename": "services.csv",
                "url": Variable.get("DI_EXTRA_SERVICES_FILE_URL", None),
            },
            {
                "id": "structures",
                "filename": "structures.csv",
                "url": Variable.get("DI_EXTRA_STRUCTURES_FILE_URL", None),
            },
        ],
    },
    {
        "id": "cd72",
        "schedule": "@once",
        "snapshot": False,
        "streams": [
            {
                "id": "structures",
                "filename": "structures.csv",
                "url": Variable.get("CD72_STRUCTURES_FILE_URL", None),
                "token": Variable.get("GRIST_API_TOKEN", None),
            },
            {
                "id": "services",
                "filename": "services.csv",
                "url": Variable.get("CD72_SERVICES_FILE_URL", None),
                "token": Variable.get("GRIST_API_TOKEN", None),
            },
        ],
    },
    {
        "id": "pole-emploi",
        "schedule": "@once",
        "snapshot": False,
        "streams": [
            {
                "id": "structures",
                "filename": "structures.json",
                "url": Variable.get("DORA_PREPROD_API_URL", None),
                "token": Variable.get("DORA_PREPROD_API_TOKEN", None),
            },
            {
                "id": "services",
                "filename": "services.json",
                "url": Variable.get("DORA_PREPROD_API_URL", None),
                "token": Variable.get("DORA_PREPROD_API_TOKEN", None),
            },
        ],
    },
]
