"""migration requests to analytics

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
        insert
          into
            public.api__consult_service_events(
            id,
            created_at,
            "user",
            score_qualite,
            service_id,
            "source")
        select
            id,
            created_at,
            "user",
            NULL as score_qualite,
            path_params ->> 'source' as "source",
            path_params ->> 'id' as id
        from
            public.api__requests ar
        where
            status_code = 200
            and "user" is not null
            and path like '%/api/v0/services/%'
        ;
    """)
    op.execute("""
        insert
          into
            public.api__consult_structure_events(
            id,
            created_at,
            "user",
            structure_id,
            "source")
        select
            id,
            created_at,
            "user",
            path_params ->> 'source' as "source",
            path_params ->> 'id' as id
        from
            public.api__requests ar
        where
            status_code = 200
            and "user" is not null
            and path like '%/api/v0/structures/%'
        ;
    """)
    op.execute("""
        insert
          into
            public.api__list_services_events(
            id,
            created_at,
            "user",
            region,
            departement,
            code_commune,
            frais,
            profils,
            thematiques,
            modes_accueil,
            "types",
            inclure_suspendus,
            sources
        )
        select
            id,
            created_at,
            "user",
            query_params ->> 'code_region' as region,
            query_params ->> 'code_departement' as departement,
            query_params ->> 'code_commune' as code_commune,
            string_to_array(query_params ->> 'frais', ',') as frais,
            string_to_array(query_params ->> 'profils', ',') as profils,
            string_to_array(query_params ->> 'thematiques', ',') as thematiques,
            string_to_array(query_params ->> 'modes_accueil', ',') as modes_accueil,
            string_to_array(query_params ->> 'types', ',') as "types",
            case
                when query_params ->> 'inclure_suspendus' = 'false' then false
                when query_params ->> 'inclure_suspendus' = 'true' then true
                else NULL
            end as inclure_suspendus,
            case
                when query_params ->> 'sources' = '' then null
                else string_to_array(query_params ->> 'sources', ',')
            end as sources
        from
            public.api__requests ar
        where
            status_code = 200
            and "user" is not null
            and path like '%/api/v0/services'
        ;
    """)
    op.execute("""
        insert
          into
            public.api__list_structures_events(
            id,
            created_at,
            "user",
            structure_id,
            typologie,
            label_national,
            thematiques,
            region,
            departement,
            code_commune,
            sources
        )
        select
            id,
            created_at,
            "user",
            query_params ->> 'id' as structure_id,
            query_params ->> 'typologie' as typologie,
            query_params ->> 'label_national' as label_national,
            string_to_array(query_params ->> 'thematiques', ',') as thematiques,
            query_params ->> 'code_region' as region, -- 0 resultat
            query_params ->> 'code_departement' as departement,
            query_params ->> 'code_commune' as code_commune,
            case
                when query_params ->> 'sources' = '' then null
                else string_to_array(query_params ->> 'sources', ',')
            end as sources
        from
            public.api__requests ar
        where
            status_code = 200
            and "user" is not null
            and path like '%/api/v0/structures'
        ;
    """)
    op.execute("""
        insert
          into
            public.api__services_searchs(
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
            first_services_ids_and_quality_score,
            total_services_count,
            sources
        )
        select
            id,
            created_at,
            "user",
            query_params ->> 'code_commune' as code_commune,
            cast(query_params ->> 'lat' as FLOAT) as lat,
            cast(query_params ->> 'lon' as FLOAT) as lon,
            string_to_array(query_params ->> 'thematiques', ',') as thematiques,
            string_to_array(query_params ->> 'frais', ',') as frais,
            string_to_array(query_params ->> 'modes_accueil', ',') as modes_accueil,
            string_to_array(query_params ->> 'profils', ',') as profils,
            string_to_array(query_params ->> 'types', ',') as types,
            case
                when query_params ->> 'inclure_suspendus' = 'true' then true
                else false
            end as inclure_suspendus,
            NULL as first_services_ids_and_quality_score,
            NULL as total_services_count,
            case
                when query_params ->> 'sources' = '' then null
                else string_to_array(query_params ->> 'sources', ',')
            end as sources
        from
            public.api__requests ar
                where
                    status_code = 200
                    and "user" is not null
                    and query_params ->> 'lat'::text not like '%,%'
                    and query_params ->> 'lon'::text not like '%,%'
                    and path like '%/api/v0/search/services';
    """)


def downgrade() -> None:
    pass
