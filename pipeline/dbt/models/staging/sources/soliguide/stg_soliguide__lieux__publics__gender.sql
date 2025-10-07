WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}),

lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

final AS (
    SELECT
        data ->> 'lieu_id'                                             AS "lieu_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics' -> 'gender')) AS "value"
    FROM source
    WHERE
        data -> 'publics' -> 'gender' IS NOT NULL
)

SELECT final.*
FROM final
INNER JOIN lieux ON final.lieu_id = lieux.lieu_id
