WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

final AS (
    SELECT
        lieux.lieu_id                                     AS "id",
        lieux.lieu_id                                     AS "adresse_id",
        NULL                                              AS "antenne",
        NULL                                              AS "rna",
        'soliguide'                                       AS "source",
        NULL                                              AS "horaires_ouverture",
        NULL                                              AS "accessibilite",
        NULL::TEXT []                                     AS "labels_nationaux",
        NULL::TEXT []                                     AS "labels_autres",
        NULL::TEXT []                                     AS "thematiques",
        NULL                                              AS "typologie",
        lieux.updated_at                                  AS "date_maj",
        NULL                                              AS "siret",
        lieux.name                                        AS "nom",
        lieux.entity_website                              AS "site_web",
        lieux.entity_mail                                 AS "courriel",
        NULL                                              AS "telephone",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url AS "lien_source",
        CASE LENGTH(lieux.description) <= 280
            WHEN TRUE THEN lieux.description
            WHEN FALSE THEN LEFT(lieux.description, 279) || '…'
        END                                               AS "presentation_resume",
        CASE LENGTH(lieux.description) <= 280
            WHEN TRUE THEN NULL
            WHEN FALSE THEN lieux.description
        END                                               AS "presentation_detail"
    FROM lieux
    ORDER BY 1
)

SELECT * FROM final
