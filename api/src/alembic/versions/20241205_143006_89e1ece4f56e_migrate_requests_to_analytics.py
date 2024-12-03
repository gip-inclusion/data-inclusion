"""migrate requests to analytics

Revision ID: 89e1ece4f56e
Revises: d5351b394a5e
Create Date: 2024-12-05 14:30:06.910343

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "89e1ece4f56e"
down_revision = "d5351b394a5e"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        INSERT INTO
            public.api__consult_service_events(
                id,
                created_at,
                "user",
                source,
                service_id
            )
        SELECT
            id,
            created_at,
            "user",
            path_params ->> 'source',
            path_params ->> 'id'
        FROM
            public.api__requests
        WHERE
            status_code = 200
            AND endpoint_name = 'retrieve_service_endpoint'
    """)
    op.execute("""
        INSERT INTO
            public.api__consult_structure_events(
                id,
                created_at,
                "user",
                source,
                structure_id
            )
        SELECT
            id,
            created_at,
            "user",
            path_params ->> 'source',
            path_params ->> 'id'
        FROM
            public.api__requests
        WHERE
            status_code = 200
            AND endpoint_name = 'retrieve_structure_endpoint'
    """)
    op.execute("""
        INSERT INTO
            public.api__list_services_events(
                id,
                created_at,
                "user",
                code_region,
                code_departement,
                code_commune,
                frais,
                profils,
                thematiques,
                modes_accueil,
                types,
                inclure_suspendus,
                sources
            )
        SELECT
            id,
            created_at,
            "user",
            query_params ->> 'code_region',
            query_params ->> 'code_departement',
            query_params ->> 'code_commune',
            STRING_TO_ARRAY(query_params ->> 'frais', ','),
            STRING_TO_ARRAY(query_params ->> 'profils', ','),
            STRING_TO_ARRAY(query_params ->> 'thematiques', ','),
            STRING_TO_ARRAY(query_params ->> 'modes_accueil', ','),
            STRING_TO_ARRAY(query_params ->> 'types', ','),
            CAST(query_params ->> 'inclure_suspendus' AS BOOLEAN),
            STRING_TO_ARRAY(query_params ->> 'sources', ',')
        FROM
            public.api__requests
        WHERE
            status_code = 200
            AND endpoint_name = 'list_services_endpoint'
    """)
    op.execute("""
        INSERT INTO
            public.api__list_structures_events(
                id,
                created_at,
                "user",
                structure_id,
                typologie,
                label_national,
                thematiques,
                code_region,
                code_departement,
                code_commune,
                sources
            )
        SELECT
            id,
            created_at,
            "user",
            query_params ->> 'id',
            query_params ->> 'typologie',
            query_params ->> 'label_national',
            STRING_TO_ARRAY(query_params ->> 'thematiques', ','),
            query_params ->> 'code_region',
            query_params ->> 'code_departement',
            query_params ->> 'code_commune',
            STRING_TO_ARRAY(query_params ->> 'sources', ',')
        FROM
            public.api__requests
        WHERE
            status_code = 200
            AND endpoint_name = 'list_structures_endpoint'
    """)
    op.execute("""
        INSERT INTO
            public.api__search_services_events(
                id,
                created_at,
                "user",
                code_commune,
                lat,
                lon,
                thematiques,
                frais,
                modes_accueil,
                profils,
                types,
                inclure_suspendus,
                sources
            )
        SELECT
            id,
            created_at,
            "user",
            query_params ->> 'code_commune',
            CAST(query_params ->> 'lat' AS FLOAT),
            CAST(query_params ->> 'lon' AS FLOAT),
            STRING_TO_ARRAY(query_params ->> 'thematiques', ','),
            STRING_TO_ARRAY(query_params ->> 'frais', ','),
            STRING_TO_ARRAY(query_params ->> 'modes_accueil', ','),
            STRING_TO_ARRAY(query_params ->> 'profils', ','),
            STRING_TO_ARRAY(query_params ->> 'types', ','),
            COALESCE(CAST(query_params ->> 'inclure_suspendus' AS BOOLEAN), FALSE),
            STRING_TO_ARRAY(query_params ->> 'sources', ',')
        FROM
            public.api__requests
        WHERE
            status_code = 200
            AND (query_params ->> 'lat') NOT LIKE '%,%'
            AND (query_params ->> 'lon') NOT LIKE '%,%'
            AND endpoint_name = 'search_services_endpoint'
    """)


def downgrade() -> None:
    pass
