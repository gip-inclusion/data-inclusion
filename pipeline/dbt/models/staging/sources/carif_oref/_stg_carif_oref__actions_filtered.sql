WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

actions AS (
    SELECT
        source.data ->> '@numero' AS "numero_formation",
        actions.data              AS "data"
    FROM source,
        JSONB_PATH_QUERY(source.data, '$.action[*]') AS actions (data)
),

final AS (
    SELECT * FROM actions
    WHERE
        JSONB_PATH_EXISTS(data, '$.organisme\-formateur[*]')
        AND JSONB_PATH_EXISTS(data, '$.code\-public\-vise[*]')
        AND JSONB_PATH_EXISTS(data, '$.lieu\-de\-formation[*]')
)

SELECT * FROM final
