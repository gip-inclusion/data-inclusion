WITH structures AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__organisations') }}
),

final AS (
    SELECT
        id                              AS "id",
        id                              AS "adresse_id",
        _di_source_id                   AS "source",
        labels_nationaux                AS "labels_nationaux",
        labels_autres                   AS "labels_autres",
        thematiques                     AS "thematiques",
        -- SOURCEFIX(2024-10-30) : Remove this when there is no 'PE' in the sources anymore
        CASE
            WHEN typologie = 'PE' THEN 'FT'
            ELSE typologie
        END                             AS typologie,
        date_maj                        AS "date_maj",
        NULLIF(siret, '')               AS "siret",
        NULLIF(nom, '')                 AS "nom",
        NULLIF(lien_source, '')         AS "lien_source",
        NULLIF(horaires_ouverture, '')  AS "horaires_ouverture",
        NULLIF(accessibilite, '')       AS "accessibilite",
        NULLIF(rna, '')                 AS "rna",
        NULLIF(presentation_resume, '') AS "presentation_resume",
        NULLIF(presentation_detail, '') AS "presentation_detail",
        NULLIF(telephone, '')           AS "telephone",
        NULLIF(courriel, '')            AS "courriel",
        NULLIF(site_web, '')            AS "site_web"
    FROM structures
)

SELECT * FROM final
