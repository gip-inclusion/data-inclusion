"""This declarative module lists the sources and their streams.

It is used as a source of truth in general and to generate dedicated dags in particular.
"""

from airflow.models import Variable

SOURCES_CONFIGS = [
    {
        "id": "dora",
        "schedule_interval": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "structures",
                "filename": "structures.json",
                "url": Variable.get("DORA_API_URL", None),
            },
            {
                "id": "services",
                "filename": "services.json",
                "url": Variable.get("DORA_API_URL", None),
            },
        ],
    },
    {
        "id": "mes-aides",
        "schedule_interval": "@daily",
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
        "schedule_interval": "@once",
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
        "schedule_interval": "@daily",
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
        "schedule_interval": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "organisations",
                "filename": "organisations.xlsx",
                "url": Variable.get("CD35_FILE_URL", None),
            },
        ],
    },
    {
        "id": "cd72",
        "schedule_interval": "@once",
        "snapshot": False,
        "streams": [
            {
                "id": "rows",
                "filename": "rows.xlsx",
                "url": Variable.get("CD72_FILE_URL", None),
            },
        ],
    },
    {
        "id": "emplois-de-linclusion",
        "schedule_interval": "@daily",
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
        "schedule_interval": "@daily",
        "snapshot": True,
        "streams": [
            {
                "id": "benefits",
                "filename": "benefits.json",
                "url": Variable.get("UN_JEUNE_UNE_SOLUTION_API_URL", None).rstrip("/")
                + "/benefits",
            },
            {
                "id": "institutions",
                "filename": "institutions.json",
                "url": Variable.get("UN_JEUNE_UNE_SOLUTION_API_URL", None).rstrip("/")
                + "/institutions",
            },
        ],
    },
    {
        "id": "annuaire-du-service-public",
        "schedule_interval": "@daily",
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
            "schedule_interval": "@daily",
            "snapshot": False,
            "streams": [
                {
                    "id": "structures",
                    "filename": "structures.json",
                    "url": Variable.get(
                        f"MEDIATION_NUMERIQUE_{source_id.upper().replace('-', '_')}_DATASET_URL",
                        None,
                    ),
                },
                {
                    "id": "services",
                    "filename": "services.json",
                    "url": Variable.get(
                        f"MEDIATION_NUMERIQUE_{source_id.upper().replace('-', '_')}_DATASET_URL",
                        None,
                    ),
                },
            ],
        }
        for source_id in [
            "hinaura",
            "cd49",
            "assembleurs",
            "francilin",
            "france-tiers-lieux",
            "angers",
            "france-services",
            "conseiller-numerique",
        ]
    ],
]
