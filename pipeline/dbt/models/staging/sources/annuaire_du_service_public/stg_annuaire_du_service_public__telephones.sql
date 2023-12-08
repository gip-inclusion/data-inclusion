WITH source AS (
    {{ stg_source_header('annuaire_du_service_public', 'etablissements') }}
),

final AS (
    SELECT
        source.data ->> 'id'              AS "etablissement_id",
        telephones.data ->> 'valeur'      AS "valeur",
        telephones.data ->> 'description' AS "description"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.telephone[*]')) AS telephones (data)
)

SELECT * FROM final
