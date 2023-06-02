WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

final AS (
    SELECT
        lieux.lieu_id                                     AS "id",
        NULL                                              AS "antenne",
        NULL                                              AS "rna",
        lieux.position_coordinates_x                      AS "longitude",
        lieux.position_coordinates_y                      AS "latitude",
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
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url AS "lien_source",
        lieux.position_complement_adresse                 AS "complement_adresse",
        lieux.position_ville                              AS "commune",
        lieux.position_adresse                            AS "adresse",
        lieux.entity_website                              AS "site_web",
        lieux.position_code_postal                        AS "code_postal",
        NULL                                              AS "code_insee",
        lieux.entity_mail                                 AS "courriel",
        NULL                                              AS "telephone",
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
