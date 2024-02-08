{{
    config(
        pre_hook=[
            "DROP INDEX IF EXISTS service_source_idx",
            "DROP INDEX IF EXISTS structure_source_idx",
            "DROP INDEX IF EXISTS service_modes_accueil_idx",
            "DROP INDEX IF EXISTS service_thematiques_idx",
            "DROP INDEX IF EXISTS service_geography_idx",
        ],
        post_hook=[
            "ALTER TABLE {{ this }} ADD CONSTRAINT services_structure_surrogate_id_fk FOREIGN KEY (_di_structure_surrogate_id) REFERENCES {{ ref('api_structure') }} (_di_surrogate_id)",
            "CREATE INDEX IF NOT EXISTS service_source_idx ON service(source);",
            "CREATE INDEX IF NOT EXISTS structure_source_idx ON structure(source);",
            "CREATE INDEX IF NOT EXISTS service_modes_accueil_idx ON {{ this }} USING GIN (modes_accueil)",
            "CREATE INDEX IF NOT EXISTS service_thematiques_idx ON {{ this }} USING GIN (thematiques)",
            "CREATE INDEX IF NOT EXISTS service_geography_idx ON {{ this }} USING GIST ((ST_MakePoint(longitude, latitude)::geography(geometry, 4326)))",
        ]
    )
}}

WITH services AS (
    SELECT * FROM {{ ref('int__union_services__enhanced') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='services',
                from=ref('int__union_services__enhanced'),
                except=[
                    '_di_adresse_surrogate_id',
                    'adresse_id',
                ]
            )
        }}
    FROM services
)

SELECT * FROM final
