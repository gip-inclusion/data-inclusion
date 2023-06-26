WITH structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

final AS (
    SELECT
        accessibilite       AS "accessibilite",
        id                  AS "adresse_id",
        antenne             AS "antenne",
        courriel            AS "courriel",
        date_maj::DATE      AS "date_maj",
        horaires_ouverture  AS "horaires_ouverture",
        id                  AS "id",
        labels_autres       AS "labels_autres",
        labels_nationaux    AS "labels_nationaux",
        lien_source         AS "lien_source",
        nom                 AS "nom",
        presentation_detail AS "presentation_detail",
        presentation_resume AS "presentation_resume",
        rna                 AS "rna",
        siret               AS "siret",
        site_web            AS "site_web",
        _di_source_id       AS "source",
        telephone           AS "telephone",
        NULL::TEXT []       AS "thematiques",
        typologie           AS "typologie"
    FROM structures
)

SELECT * FROM final
