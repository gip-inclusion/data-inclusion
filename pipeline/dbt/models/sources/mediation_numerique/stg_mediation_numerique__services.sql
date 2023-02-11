WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias ~ 'mednum'
        AND file ~ 'services'
),

final AS (
    SELECT
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'types', 'null')))::TEXT[]       AS "types",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'profils', 'null')))::TEXT[]     AS "profils",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'thematiques', 'null')))::TEXT[] AS "thematiques",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'frais', 'null')))::TEXT[]       AS "frais",
        (data ->> 'longitude')::FLOAT                                                                 AS "longitude",
        (data ->> 'latitude')::FLOAT                                                                  AS "latitude",
        data ->> 'id'                                                                                 AS "id",
        data ->> 'structure_id'                                                                       AS "structure_id",
        data ->> 'nom'                                                                                AS "nom",
        data ->> 'source'                                                                             AS "source"
    FROM source
)

SELECT * FROM final
