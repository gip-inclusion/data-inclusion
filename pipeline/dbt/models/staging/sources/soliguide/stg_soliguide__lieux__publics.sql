WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}),

lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

-- Iterate over each dimension of publics
{% for dimension in ['administrative', 'familiale', 'gender', 'other'] %}
    {{ dimension }} AS (
        SELECT
            data ->> 'lieu_id'                                                      AS "lieu_id",
            TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics' -> '{{ dimension }}')) AS "value"
        FROM source
        WHERE
        -- Ignore publics values when access is unconditional
            CAST(data -> 'publics' -> 'accueil' AS INT) = 2
            -- Ignore dimensions that are irrelevant (i.e. all possible values are present)
            AND CARDINALITY(ARRAY(SELECT TRIM(JSONB_ARRAY_ELEMENTS_TEXT(data -> 'publics' -> '{{ dimension }}'))))
            < (
                SELECT COUNT(*) FROM {{ ref('stg_soliguide__publics') }} AS publics
                WHERE publics.dimension = '{{ dimension }}'
            )
    ),
{% endfor %}

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
