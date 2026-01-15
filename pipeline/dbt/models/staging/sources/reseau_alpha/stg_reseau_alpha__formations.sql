WITH source AS (
    {{ stg_source_header('reseau_alpha', 'structures') }}
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'id'), '')                                   AS "structure_id",
        NULLIF(TRIM(formations.data ->> 'id'), '')                               AS "id",
        NULLIF(TRIM(formations.data ->> 'nom'), '')                              AS "nom",
        NULLIF(TRIM(formations.data ->> 'activite'), '')                         AS "activite",
        NULLIF(TRIM(formations.data -> 'contact' ->> 'nom'), '')                 AS "contact__nom",
        NULLIF(TRIM(formations.data -> 'contact' ->> 'prenom'), '')              AS "contact__prenom",
        NULLIF(TRIM(formations.data -> 'contact' ->> 'email'), '')               AS "contact__email",
        NULLIF(TRIM(formations.data -> 'contact' ->> 'telephone1'), '')          AS "contact__telephone1",
        NULLIF(TRIM(formations.data -> 'contact' ->> 'telephone2'), '')          AS "contact__telephone2",
        CAST(formations.data ->> 'coursEte' AS BOOLEAN)                          AS "cours_ete",
        CASE LOWER(NULLIF(TRIM(formations.data ->> 'cout'), ''))
            WHEN '0' THEN 'gratuit'
            WHEN '0€' THEN 'gratuit'
            ELSE LOWER(NULLIF(TRIM(formations.data ->> 'cout'), ''))
        END                                                                      AS "cout",
        NULLIF(TRIM(formations.data ->> 'criteresSavoirBase'), '')               AS "criteres_savoir_base",
        TO_DATE(formations.data ->> 'dateDebut', 'YYYY-MM-DD')                   AS "date_debut",
        TO_DATE(formations.data ->> 'dateFin', 'YYYY-MM-DD')                     AS "date_fin",
        CAST(formations.data ->> 'gardeEnfants' AS BOOLEAN)                      AS "garde_enfants",
        CAST(formations.data ->> 'placeDisponible' AS BOOLEAN)                   AS "place_disponible",
        processings.html_to_markdown(formations.data ->> 'presentationPublique') AS "presentation_publique",
        NULLIF(TRIM(formations.data ->> 'slug'), '')                             AS "slug",
        NULLIF(TRIM(formations.data ->> 'url'), '')                              AS "url"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.formations[*]') AS formations (data)
)

SELECT * FROM final
