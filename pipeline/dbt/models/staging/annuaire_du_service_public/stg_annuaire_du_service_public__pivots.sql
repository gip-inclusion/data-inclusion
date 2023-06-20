WITH source AS (
    SELECT * FROM {{ source('annuaire_du_service_public', 'etablissements') }}
),


final AS (
    SELECT
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(pivots.data -> 'code_insee_commune')) AS "code_insee_commune",
        source.data ->> 'id'                                                                AS "etablissement_id",
        pivots.data ->> 'type_service_local'                                                AS "type_service_local"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.pivot[*]')) AS pivots (data)
)

SELECT * FROM final
