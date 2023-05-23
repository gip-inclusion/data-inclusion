WITH structures AS (
    SELECT * FROM {{ ref('stg_monenfant__structures') }}
),

final AS (
    SELECT
        id                  AS "id",
        siret               AS "siret",
        antenne             AS "antenne",
        rna                 AS "rna",
        nom                 AS "nom",
        commune             AS "commune",
        code_postal         AS "code_postal",
        code_insee          AS "code_insee",
        adresse             AS "adresse",
        complement_adresse  AS "complement_adresse",
        longitude           AS "longitude",
        latitude            AS "latitude",
        telephone           AS "telephone",
        courriel            AS "courriel",
        site_web            AS "site_web",
        lien_source         AS "lien_source",
        horaires_ouverture  AS "horaires_ouverture",
        accessibilite       AS "accessibilite",
        labels_nationaux    AS "labels_nationaux",
        labels_autres       AS "labels_autres",
        thematiques         AS "thematiques",
        typologie           AS "typologie",
        presentation_resume AS "presentation_resume",
        presentation_detail AS "presentation_detail",
        date_maj            AS "date_maj",
        _di_source_id       AS "source"
    FROM structures
)

SELECT * FROM final
