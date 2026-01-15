WITH source AS (
    {{ stg_source_header('reseau_alpha', 'structures') }}
),

known_publics AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__publics') }}
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'id'), '')                                  AS "structure_id",
        NULLIF(TRIM(formations.data ->> 'id'), '')                              AS "formation_id",
        public_specifique.raw_value                                             AS "public_specifique_raw_value",
        known_publics.value                                                     AS "public_specifique_value",
        NULLIF(TRIM(publics.data ->> 'publicSpecifiqueDescription'), '')        AS "public_specifique_description",
        LOWER(NULLIF(TRIM(publics.data ->> 'publicSpecifiquePrioritaire'), '')) AS "public_specifique_prioritaire"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.formations[*]') AS formations (data),
        -- publicsSpecifiques contains both arrays and objects
        -- the following query extracts values from both structures
        JSONB_PATH_QUERY(formations.data, '$.publicsSpecifiques[*].** ? (exists(@.publicSpecifique))') WITH ORDINALITY AS publics (data, index),
        NULLIF(TRIM(publics.data ->> 'publicSpecifique'), '') AS public_specifique (raw_value)
    LEFT JOIN known_publics ON SIMILARITY(public_specifique.raw_value, known_publics.raw) > 0.5
)

SELECT * FROM final
