WITH source AS (
    {{ stg_source_header('mission_locale', 'offres') }}
),

final AS (
    SELECT
        NULLIF(TRIM(data -> 'offres' ->> 'id_offre'), '')                             AS "id_offre",
        LOWER(UNNEST(STRING_TO_ARRAY(data -> 'offres' ->> 'liste_des_profils', ';'))) AS "value"
    FROM
        source
)

SELECT * FROM final
