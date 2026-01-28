WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

final AS (
    SELECT
        NULLIF(TRIM(actions.data ->> '@numero'), '') AS "numero_action",
        code.data ->> '$'                            AS "code_public_vise",
        code.data ->> '@ref'                         AS "version_formacode"
    FROM source,  -- noqa: structure.unused_join
        JSONB_PATH_QUERY(source.data, '$.action[*]') AS actions (data),
        JSONB_PATH_QUERY(actions.data, '$.organisme\-formateur[*]'),
        JSONB_PATH_QUERY(actions.data, '$.code\-public\-vise[*]') AS code (data)
)

SELECT * FROM final
