WITH source AS (
    {{ stg_source_header('reseau_alpha', 'structures') }}
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'id'), '')            AS "structure_id",
        NULLIF(TRIM(formations.data ->> 'id'), '')        AS "formation_id",
        NULLIF(TRIM(adresses.data ->> 'numero'), '')      AS "numero",
        NULLIF(TRIM(adresses.data ->> 'voie'), '')        AS "voie",
        NULLIF(TRIM(adresses.data ->> 'codePostal'), '')  AS "code_postal",
        NULLIF(TRIM(adresses.data ->> 'ville'), '')       AS "ville",
        CAST(adresses.data ->> 'longitude' AS FLOAT)      AS "longitude",
        CAST(adresses.data ->> 'latitude' AS FLOAT)       AS "latitude",
        CAST(adresses.data ->> 'gardeEnfants' AS BOOLEAN) AS "garde_enfants"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.formations[*]') AS formations (data),
        JSONB_PATH_QUERY(formations.data, '$.adresses[*]') AS adresses (data)
)

SELECT * FROM final
