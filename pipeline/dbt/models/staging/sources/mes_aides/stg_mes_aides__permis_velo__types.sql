WITH source AS (
    {{ stg_source_header('mes_aides', 'permis_velo') }}
),

final AS (
    SELECT
        data #>> '{fields,ID}' AS "service_id",
        mes_aide_types         AS "value"
    FROM
        source,
        LATERAL UNNEST(STRING_TO_ARRAY(data #>> '{fields,Type}', ', ')) AS mes_aide_types
    WHERE data #>> '{fields,Type}' IS NOT NULL
)

SELECT * FROM final
