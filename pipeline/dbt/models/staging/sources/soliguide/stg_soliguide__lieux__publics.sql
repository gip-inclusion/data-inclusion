WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}),

lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

administrative AS (
    SELECT
        data ->> 'lieu_id'                                                     AS "lieu_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics' -> 'administrative')) AS "value"
    FROM source
    WHERE
        data -> 'publics' -> 'administrative' IS NOT NULL
),

familiale AS (
    SELECT
        data ->> 'lieu_id'                                                 AS "lieu_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics' -> 'familialle')) AS "value"
    FROM source
    WHERE
        data -> 'publics' -> 'familialle' IS NOT NULL
),

gender AS (
    SELECT
        data ->> 'lieu_id'                                             AS "lieu_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics' -> 'gender')) AS "value"
    FROM source
    WHERE
        data -> 'publics' -> 'gender' IS NOT NULL
),

other AS (
    SELECT
        data ->> 'lieu_id'                                            AS "lieu_id",
        TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics' -> 'other')) AS "value"
    FROM source
    WHERE
        data -> 'publics' -> 'other' IS NOT NULL
),

final AS (
    SELECT * FROM administrative
    UNION ALL
    SELECT * FROM familiale
    UNION ALL
    SELECT * FROM gender
    UNION ALL
    SELECT * FROM other
)

SELECT final.*
FROM final
INNER JOIN lieux ON final.lieu_id = lieux.lieu_id
