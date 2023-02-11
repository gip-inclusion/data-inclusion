WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias = 'dora'
        AND file ~ 'services'
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
