WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias = 'etab_pub'
),

final AS (
    SELECT
        source.data ->> 'id'              AS "etablissement_id",
        telephones.data ->> 'valeur'      AS "valeur",
        telephones.data ->> 'description' AS "description"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.telephone[*]')) AS telephones(data)
)

SELECT * FROM final
