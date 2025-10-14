WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

final AS (
    SELECT
        NULLIF(TRIM(actions.data ->> '@numero'), '') AS "numero_action",
        code.data ->> '$'                            AS "code_public_vise",
        code.data ->> '@ref'                         AS "version_formacode"
    FROM source  -- noqa: structure.unused_join
    INNER JOIN JSONB_PATH_QUERY(source.data, '$.action[*]') AS actions (data) ON TRUE
    INNER JOIN JSONB_PATH_QUERY(actions.data, '$.organisme\-formateur[*]') ON TRUE
    INNER JOIN JSONB_PATH_QUERY(actions.data, '$.code\-public\-vise[*]') AS code (data) ON TRUE
    LEFT JOIN JSONB_PATH_QUERY(actions.data, '$.lieu\-de\-formation[*]') AS lieux_de_formation (data) ON TRUE
    ORDER BY
        (lieux_de_formation.data ->> '@tag') = 'principal' DESC
)

SELECT * FROM final
