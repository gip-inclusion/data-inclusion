WITH structures AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__structures') }}
),

final AS (
    SELECT
        NULL                  AS "accessibilite",
        content__courriel     AS "courriel",
        NULL                  AS "horaires_ouverture",
        id                    AS "id",
        url                   AS "lien_source",
        nom                   AS "nom",
        description           AS "presentation_detail",
        NULL                  AS "presentation_resume",
        NULL                  AS "rna",
        NULL                  AS "siret",
        content__site_web     AS "site_web",
        _di_source_id         AS "source",
        content__telephone    AS "telephone",
        NULL                  AS "typologie",
        content__date_maj     AS "date_maj",
        'structure--' || id   AS "adresse_id",
        CAST(NULL AS TEXT []) AS "labels_autres",
        CAST(NULL AS TEXT []) AS "labels_nationaux",
        CAST(NULL AS TEXT []) AS "thematiques"
    FROM structures
)

SELECT * FROM final
