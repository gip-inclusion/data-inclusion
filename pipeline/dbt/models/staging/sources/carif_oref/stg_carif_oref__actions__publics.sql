WITH actions AS (
    SELECT * FROM {{ ref('_stg_carif_oref__actions_filtered') }}
),

final AS (
    SELECT DISTINCT
        NULLIF(TRIM(actions.data ->> '@numero'), '') AS "numero_action",
        code.data ->> '$'                            AS "code_public_vise",
        code.data ->> '@ref'                         AS "version_formacode"
    FROM actions,
        JSONB_PATH_QUERY(actions.data, '$.code\-public\-vise[*]') AS code (data)
)

SELECT * FROM final
