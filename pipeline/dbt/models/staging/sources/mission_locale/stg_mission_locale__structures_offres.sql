WITH source AS (
    {{ stg_source_header('mission_locale', 'structures_offres') }}
),

final AS (
    SELECT
        NULLIF(TRIM(data -> 'structures_offres' ->> 'id'), '')               AS "id",
        NULLIF(TRIM(data -> 'structures_offres' ->> 'offre_id'), '')         AS "offre_id",
        NULLIF(TRIM(data -> 'structures_offres' ->> 'missionlocale_id'), '') AS "missionlocale_id"
    FROM source
)

SELECT * FROM final
