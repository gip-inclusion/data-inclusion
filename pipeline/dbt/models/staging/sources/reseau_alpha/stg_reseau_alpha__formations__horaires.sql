WITH source AS (
    {{ stg_source_header('reseau_alpha', 'structures') }}
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'id'), '')      AS "structure_id",
        NULLIF(TRIM(formations.data ->> 'id'), '')  AS "formation_id",
        NULLIF(TRIM(horaires.data ->> 'jour'), '')  AS "jour",
        CAST(horaires.data ->> 'dateDebut' AS TIME) AS "debut",
        CAST(horaires.data ->> 'dateFin' AS TIME)   AS "fin"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.formations[*]') AS formations (data),
        JSONB_PATH_QUERY(formations.data, '$.joursHorairesDetails[*]') AS horaires (data)
)

SELECT * FROM final
