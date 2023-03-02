from airflow.models import Variable

SOURCES_CONFIGS = [
    {
        "id": "dora",
        "schedule_interval": "@daily",
        "read_fn": "data_inclusion.scripts.tasks.utils.read_json",
        "extract_fn": "data_inclusion.scripts.tasks.sources.dora.extract",
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
        "read_fn": "data_inclusion.scripts.tasks.utils.read_json",
        "extract_fn": "data_inclusion.scripts.tasks.sources.mes_aides.extract",
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
        "read_fn": "data_inclusion.scripts.tasks.utils.read_excel",
        "extract_fn": "data_inclusion.scripts.tasks.utils.extract_http_content",
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
        "read_fn": "data_inclusion.scripts.tasks.sources.finess.read",
        "extract_fn": "data_inclusion.scripts.tasks.utils.extract_http_content",
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
        "read_fn": "data_inclusion.scripts.tasks.sources.cd35.read",
        "extract_fn": "data_inclusion.scripts.tasks.utils.extract_http_content",
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
        "read_fn": "data_inclusion.scripts.tasks.sources.cd72.read",
        "extract_fn": "data_inclusion.scripts.tasks.utils.extract_http_content",
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
        "read_fn": "data_inclusion.scripts.tasks.utils.read_json",
        "extract_fn": "data_inclusion.scripts.tasks.sources.emplois_de_linclusion.extract",
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
        "read_fn": "data_inclusion.scripts.tasks.utils.read_json",
        "extract_fn": "data_inclusion.scripts.tasks.utils.extract_http_content",
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
        "read_fn": "data_inclusion.scripts.tasks.sources.annuaire_du_service_public.read",
        "extract_fn": "data_inclusion.scripts.tasks.utils.extract_http_content",
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
            "read_fn": "data_inclusion.scripts.tasks.utils.read_json",
            "extract_fn": "data_inclusion.scripts.tasks.sources.mediation_numerique.extract",
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
