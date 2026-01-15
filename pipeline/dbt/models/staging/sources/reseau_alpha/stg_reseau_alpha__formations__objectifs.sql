WITH source AS (
    {{ stg_source_header('reseau_alpha', 'structures') }}
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'id'), '')                         AS "structure_id",
        NULLIF(TRIM(formations.data ->> 'id'), '')                     AS "formation_id",
        NULLIF(TRIM(objectifs.data ->> 'objectifVise'), '')            AS "label",
        NULLIF(TRIM(objectifs.data ->> 'objectifViseDescription'), '') AS "description"

    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.formations[*]') AS formations (data),
        JSONB_PATH_QUERY(formations.data, '$.objectifsVises[*]') AS objectifs (data)
    WHERE
        (objectifs.data ->> 'objectifVise') IS NOT NULL
)

SELECT * FROM final
