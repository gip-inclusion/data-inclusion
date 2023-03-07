WITH source AS (
    SELECT * FROM {{ source('un_jeune_une_solution', 'institutions') }}
),

final AS (
    SELECT
        _di_source_id    AS "_di_source_id",
        data ->> 'id'    AS "id",
        data ->> 'slug'  AS "slug",
        data ->> 'label' AS "label"
    FROM source
)

SELECT * FROM final
