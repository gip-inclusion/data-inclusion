WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

final AS (
    SELECT DISTINCT ON (1)
        NULLIF(TRIM(actions.data ->> '@numero'), '') AS "numero_action",
        code.data ->> '$'                            AS "code_public_vise",
        code.data ->> '@ref'                         AS "version_formacode"
    FROM source,
        JSONB_PATH_QUERY(source.data, '$.action[*]') AS actions (data),
        JSONB_PATH_QUERY(actions.data, '$.code\-public\-vise[*]') AS code (data),
        JSONB_PATH_QUERY(actions.data, '$.lieu\-de\-formation[*]') AS lieux_de_formation (data)
    ORDER BY
        NULLIF(TRIM(actions.data ->> '@numero'), ''),
        (lieux_de_formation.data ->> '@tag') = 'principal' DESC
)

SELECT * FROM final
