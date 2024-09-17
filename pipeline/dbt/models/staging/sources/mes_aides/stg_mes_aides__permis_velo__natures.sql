WITH source AS (
    {{ stg_source_header('mes_aides', 'permis_velo') }}
),

final AS (
    SELECT
        data #>> '{fields,ID}'                                  AS "service_id",
        JSONB_ARRAY_ELEMENTS_TEXT(data -> 'fields' -> 'Nature') AS "value"
    FROM source
    WHERE data -> 'fields' -> 'Nature' IS NOT NULL
)

SELECT * FROM final
