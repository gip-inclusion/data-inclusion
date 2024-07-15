import os
from collections import defaultdict

import psycopg2

from dag_utils import sources

STREAM_NAME_MAP = {
    "mes_aides": {
        "garages": "structures",
        "aides": "services",
    },
    "siao": {
        "etablissements": "structures",
    },
    "finess": {
        "etablissements": "structures",
    },
    "cd35": {
        "organisations": "structures",
    },
    "emplois_de_linclusion": {
        "siaes": "structures",
        "organisations": "structures",
    },
    "un_jeune_une_solution": {
        "benefits": "services",
        "institutions": "structures",
    },
    "annuaire_du_service_public": {
        "etablissements": "structures",
    },
    "soliguide": {
        "lieux": "structures",
    },
    "reseau_alpha": {
        "formations": "services",
    },
    "france_travail": {
        "agences": "agences",
    },
}

CONN_STRING = os.environ.get("AIRFLOW_CONN_PG")

COUNTS_BY_SOURCE = {}
for raw_source, conf in sorted(sources.SOURCES_CONFIGS.items()):
    source = raw_source.replace("-", "_")
    COUNTS_BY_SOURCE[source] = {}
    for stream in sorted(conf["streams"]):
        COUNTS_BY_SOURCE[source][stream] = defaultdict(int)

with psycopg2.connect(CONN_STRING) as conn:
    cursor = conn.cursor()

    def safe_execute(query):
        try:
            cursor.execute(query)
            return cursor.fetchall()[0][0]
        except psycopg2.errors.UndefinedTable:
            print(f"! ERROR {query=} failed, table does not exist")
            cursor.execute("ROLLBACK")
            conn.commit()
            return 0

    for source, streams in COUNTS_BY_SOURCE.items():
        for stream in streams:
            COUNTS_BY_SOURCE[source][stream]["raw"] = safe_execute(
                f"SELECT COUNT(*) FROM {source}.{stream};",
            )
            COUNTS_BY_SOURCE[source][stream]["staging"] = safe_execute(
                f"SELECT COUNT(*) FROM public_staging.stg_{source}__{stream};",
            )
            if stream in STREAM_NAME_MAP.get(source, {}):
                int_stream_name = STREAM_NAME_MAP[source][stream]
            else:
                int_stream_name = stream

            COUNTS_BY_SOURCE[source][stream]["intermediate"] = safe_execute(
                "SELECT COUNT(*) FROM "
                f"public_intermediate.int_{source}__{int_stream_name};",
            )

            # FIXME(vperron): This 'agences' should be fixed, it's the only exception
            stream_kind = (
                "structure"
                if int_stream_name in ("structures", "agences")
                else "service"
            )
            src_canonical = source.replace("_", "-")
            COUNTS_BY_SOURCE[source][stream]["marts"] = safe_execute(
                "SELECT COUNT(*) FROM "
                f"public.{stream_kind} WHERE source = '{src_canonical}';",
            )

print(
    f"{'source':<40}{'stream':<20}{'raw':>10}{'staging':>10}{'intermediate':>15}{'marts':>10}"
)
for source, streams in COUNTS_BY_SOURCE.items():
    for stream, counts in streams.items():
        print(
            f"{source:<40}{stream:<20}{counts["raw"]:>10}{counts["staging"]:>10}"
            f"{counts["intermediate"]:>15}{counts["marts"]:>10}"
        )
