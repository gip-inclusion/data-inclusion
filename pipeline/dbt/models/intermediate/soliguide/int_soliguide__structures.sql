WITH places AS (
    SELECT * FROM {{ ref('stg_soliguide__places') }}
),

final AS (
    SELECT
        lieu_id                     AS "id",
        NULL                        AS "antenne",
        NULL                        AS "rna",
        position_coordinates_x      AS "longitude",
        position_coordinates_y      AS "latitude",
        'soliguide'                 AS "source",
        NULL                        AS "horaires_ouverture",
        NULL                        AS "accessibilite",
        NULL::TEXT[]                AS "labels_nationaux",
        NULL::TEXT[]                AS "labels_autres",
        NULL::TEXT[]                AS "thematiques",
        NULL                        AS "typologie",
        updated_at                  AS "date_maj",
        NULL                        AS "siret",
        name                        AS "nom",
        seo_url                     AS "lien_source",
        position_complement_adresse AS "complement_adresse",
        ville                       AS "commune",
        position_adresse            AS "adresse",
        CASE LENGTH(description) <= 280
            WHEN TRUE THEN description
            WHEN FALSE THEN LEFT(description, 279) || '…'
        END                         AS "presentation_resume",
        CASE LENGTH(description) <= 280
            WHEN TRUE THEN NULL
            WHEN FALSE THEN description
        END                         AS "presentation_detail",
        NULL                        AS "telephone",
        entity_website              AS "site_web",
        position_code_postal        AS "code_postal",
        NULL                        AS "code_insee",
        entity_mail                 AS "courriel"
    FROM places
    ORDER BY 1
)

SELECT * FROM final
