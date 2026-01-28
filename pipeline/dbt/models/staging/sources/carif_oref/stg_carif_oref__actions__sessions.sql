WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

final AS (
    SELECT
        NULLIF(TRIM(actions.data ->> '@numero'), '')                     AS "numero_action",
        source.data ->> '@numero'                                        AS "numero_formation",
        NULLIF(TRIM(organismes_formateurs.data ->> '@numero'), '')       AS "numero_organisme_formateur",
        TO_DATE(session_data.value -> 'periode' ->> 'debut', 'YYYYMMDD') AS "session_debut",
        TO_DATE(session_data.value -> 'periode' ->> 'fin', 'YYYYMMDD')   AS "session_fin"
    FROM source
    INNER JOIN JSONB_PATH_QUERY(source.data, '$.action[*]') AS actions (data) ON TRUE
    INNER JOIN JSONB_PATH_QUERY(actions.data, '$.organisme\-formateur[*]') AS organismes_formateurs (data) ON TRUE
    LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(actions.data -> 'session') AS session_data (value) ON TRUE
    WHERE
        session_data.value -> 'periode' ->> 'debut' IS NOT NULL
        AND session_data.value -> 'periode' ->> 'fin' IS NOT NULL
)

SELECT * FROM final
