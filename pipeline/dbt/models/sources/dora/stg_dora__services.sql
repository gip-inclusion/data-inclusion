WITH source AS (
    SELECT * FROM {{ source('dora', 'services') }}
),

final AS (
    SELECT
        ARRAY(
            SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(
                (
                    SELECT *
                    FROM JSONB_PATH_QUERY_ARRAY(data, '$.kinds[*].value')
                )
            )
        )::TEXT[]             AS "kinds",
        ARRAY(
            SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(
                (
                    SELECT *
                    FROM JSONB_PATH_QUERY_ARRAY(data, '$.categories[*].value')
                )
            )
        )::TEXT[]             AS "categories",
        ARRAY(
            SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(
                (
                    SELECT *
                    FROM JSONB_PATH_QUERY_ARRAY(data, '$.subcategories[*].value')
                )
            )
        )::TEXT[]             AS "subcategories",
        data ->> 'id'         AS "id",
        data ->> 'structure'  AS "structure",
        data ->> 'name'       AS "name",
        data ->> 'shortDesc'  AS "short_desc",
        data ->> 'onlineForm' AS "online_form",
        data ->> 'feeDetails' AS "fee_details"
    FROM source
)

SELECT * FROM final
