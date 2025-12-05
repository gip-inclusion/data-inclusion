WITH source AS (
    {{ stg_source_header('cd35', 'organisations') }}),

known_profils AS (
    SELECT * FROM {{ ref('stg_cd35__profils') }}
),

final AS (
    SELECT
        source.data ->> 'ID' AS "structure_id",
        raw_value.trimmed    AS "raw_value",
        known_profils.value  AS "value"
    FROM source,
        REGEXP_SPLIT_TO_TABLE(source.data ->> 'PROFIL', ',') WITH ORDINALITY AS "raw" ("value", "index"),
        NULLIF(TRIM(raw.value), '') AS raw_value (trimmed)
    LEFT JOIN known_profils ON SIMILARITY(raw_value.trimmed, known_profils.raw) > 0.5
    WHERE raw_value.trimmed IS NOT NULL
)

SELECT * FROM final
