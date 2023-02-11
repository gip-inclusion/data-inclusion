WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias = '1jeune1solution'
        AND file ~ 'institutions'
),

final AS (
    SELECT
        data ->> 'id'    AS "id",
        data ->> 'slug'  AS "slug",
        data ->> 'label' AS "label"
    FROM source
)

SELECT * FROM final
