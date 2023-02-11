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
        site_internets.data ->> 'valeur'  AS "valeur",
        site_internets.data ->> 'libelle' AS "libelle"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.site_internet[*]')) AS site_internets(data)
)

SELECT * FROM final
