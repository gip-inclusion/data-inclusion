WITH source AS (
    {{ stg_source_header('reseau_alpha', 'structures') }}
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'id'), '')     AS "structure_id",
        NULLIF(TRIM(formations.data ->> 'id'), '') AS "formation_id",
        NULLIF(TRIM(item.value), '')               AS "value"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.formations[*]') AS formations (data),
        JSONB_ARRAY_ELEMENTS_TEXT(formations.data -> 'criteresScolarisation') AS item (value)
    WHERE NULLIF(TRIM(item.value), '') IS NOT NULL
)

SELECT * FROM final
