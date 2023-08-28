{{
    config(
        pre_hook=[
            "DROP INDEX IF EXISTS service_modes_accueil_idx",
            "DROP INDEX IF EXISTS service_geography_idx",
        ],
        post_hook=[
            "ALTER TABLE {{ this }} ADD CONSTRAINT services_structure_surrogate_id_fk FOREIGN KEY (_di_structure_surrogate_id) REFERENCES {{ ref('api_structure') }} (_di_surrogate_id)",
            "CREATE INDEX IF NOT EXISTS service_modes_accueil_idx ON {{ this }} USING GIN (modes_accueil)",
            "CREATE INDEX IF NOT EXISTS service_geography_idx ON {{ this }} USING GIST ((ST_MakePoint(longitude, latitude)::geography(geometry, 4326)))",
        ]
    )
}}

WITH services AS (
    SELECT * FROM {{ ref('int__enhanced_services') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='services',
                from=ref('int__enhanced_services'),
                except=[
                    '_di_adresse_surrogate_id',
                    'adresse_id',
                ]
            )
        }}
    FROM services
)

SELECT * FROM final
